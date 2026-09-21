"""The radio-lock primitives and the duty-cycle limiter."""
import asyncio
import collections
import time
from typing import Optional

class _PreemptedForHandshake(Exception):
    """Raised out of an interruptible wait (the duty-cycle throttle) when a
    link handshake is queued for the radio lock the waiter holds (phase 1,
    2026-09-20). The holder yields (`_PriorityAsyncLock.yield_to_preempt`)
    and retries the step afterwards."""


class _PriorityAsyncLock:
    """User-requested architectural fix (2026-09-16, real 1-hop repeater
    field data): `_direct_exchange_lock` (see `_send_direct_frame_and_
    wait_for_ack`'s own docstring) used to be a plain `asyncio.Lock`,
    strictly FIFO across every DIRECT exchange regardless of what kind of
    packet it carried. A real 1-hop test found `_direct_exchange_queue_
    depth` reaching 14 -- meaning a LINK_REQUEST/PROOF-class exchange
    (already tagged `PRIORITY_HANDSHAKE` at the *outer* two-tier queue,
    docs/reliability_engine_design.md §3) arriving while a backlog of
    ordinary DATA/RESOURCE-fragment retries was already queued for this
    same lock had no way to jump that backlog -- it just joined the back
    of one shared FIFO like everything else, even though establishing (or
    proving) a Link is usually what everything else is waiting on in the
    first place. A plain `asyncio.Lock` cannot reorder waiters once
    they're queued (there is no "insert ahead" operation), so preserving
    that outer priority tier all the way down to the actual radio
    required a real priority-aware primitive, not just a config tweak.

    Behavior: a higher-priority waiter (lower `priority` integer, matching
    `PRIORITY_HANDSHAKE < PRIORITY_NORMAL`) is served before an earlier-
    arrived lower-priority one; waiters within the same tier stay FIFO
    among themselves, same as the stdlib lock. Used via `async with
    lock(priority):` (see `__call__`/`_PriorityLockContext` below).

    Deliberately minimal, not a wrapper around `asyncio.Lock` -- the
    stdlib lock's own internal waiter queue has no reordering operation
    to hook into, so ownership here is tracked directly (`_locked`) with
    one waiter deque per priority tier. Cancellation-safe: a waiter
    cancelled before being granted just removes itself from its deque; a
    waiter cancelled in the narrow window after being granted but before
    resuming passes ownership on to the next waiter rather than leaving
    the lock stuck locked forever with nothing able to ever acquire it
    again (only reachable via `detach()`'s `task.cancel()` sweep in
    practice -- this interface's own steady-state code never cancels a
    task waiting on this lock)."""

    # Phase 1 (2026-09-20): the tier a holder re-queues at when it yields
    # to a pre-empting waiter -- between HANDSHAKE (0) and ANSWER (1), so
    # the yielded exchange resumes ahead of everything but the handshakes
    # that pre-empted it (tiers are compared numerically; a float sorts).
    YIELDED_PRIORITY = 0.5

    def __init__(self):
        self._locked = False
        self._waiters: "dict[int, collections.deque]" = {}
        # Pre-emption (phase 1, 2026-09-20): futures of waiters that asked
        # to pre-empt an idle holder, and the event an idle holder watches.
        self._preempt_waiters: set = set()
        self._preempt_event: "Optional[asyncio.Event]" = None

    def locked(self) -> bool:
        return self._locked

    def preempt_requested(self) -> bool:
        """A waiter that may pre-empt idle holds is queued (2026-09-20)."""
        return bool(self._preempt_waiters)

    def preempt_event(self) -> "asyncio.Event":
        """The event set while a pre-empting waiter is queued; created on
        the running loop the first time it is asked for."""
        if self._preempt_event is None:
            self._preempt_event = asyncio.Event()
            if self._preempt_waiters:
                self._preempt_event.set()
        return self._preempt_event

    def _preempt_add(self, fut) -> None:
        self._preempt_waiters.add(fut)
        if self._preempt_event is not None:
            self._preempt_event.set()

    def _preempt_remove(self, fut) -> None:
        self._preempt_waiters.discard(fut)
        if not self._preempt_waiters and self._preempt_event is not None:
            self._preempt_event.clear()

    async def yield_to_preempt(self) -> None:
        """Called by a holder at an idle point when `preempt_requested()`:
        hands the lock over and re-acquires it at YIELDED_PRIORITY, so
        the pre-empting handshake goes first and this exchange resumes
        before any ordinary waiter that queued meanwhile (review,
        2026-09-20: a plain release + re-acquire at NORMAL would splice a
        whole other exchange into a raw burst)."""
        self.release()
        await self.acquire(self.YIELDED_PRIORITY)

    async def acquire(self, priority: int, preempt: bool = False) -> None:
        if not self._locked:
            self._locked = True
            return
        fut = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(priority, collections.deque()).append(fut)
        if preempt:
            self._preempt_add(fut)
        try:
            await fut
        except asyncio.CancelledError:
            dq = self._waiters.get(priority)
            if dq is not None and fut in dq:
                # Never granted -- just drop out of line, lock state
                # (owned by whoever currently holds it, if anyone) is
                # entirely unaffected by our own departure.
                dq.remove(fut)
            elif fut.done() and not fut.cancelled():
                # Already granted ownership in the same instant our own
                # cancellation was delivered -- pass it on rather than
                # leaving the lock permanently locked with no owner able
                # to release it. Code-review fix: this used to call
                # _wake_next() and ignore its return value, unlike
                # release()'s own `if not self._wake_next(): self._locked
                # = False`. When no other waiter existed (_wake_next()
                # returns False), that left `_locked` stuck True forever
                # with nobody holding it and nobody able to call release()
                # for it -- a silent, permanent deadlock of every future
                # acquire() on this lock, contradicting this class's own
                # cancellation-safety docstring above.
                if not self._wake_next():
                    self._locked = False
            raise
        finally:
            if preempt:
                self._preempt_remove(fut)

    def release(self) -> None:
        if not self._wake_next():
            self._locked = False

    def _wake_next(self) -> bool:
        """Hands ownership to the next waiter, highest priority (lowest
        integer) first, FIFO within a tier. Returns whether anyone was
        actually waiting -- the lock stays `_locked=True` (ownership
        transferred) when True, and the caller (`release`) marks it
        unlocked only when False."""
        for tier in sorted(self._waiters.keys()):
            dq = self._waiters[tier]
            while dq:
                fut = dq.popleft()
                if not fut.done():
                    fut.set_result(None)
                    return True
            del self._waiters[tier]
        return False

    def __call__(self, priority: int, preempt: bool = False) -> "_PriorityLockContext":
        return _PriorityLockContext(self, priority, preempt)


class _PriorityLockContext:
    """`async with priority_lock(priority):` sugar -- `_PriorityAsyncLock`
    itself isn't a context manager (it needs a `priority` argument
    `asyncio.Lock`'s own `__aenter__` has no room for), so `__call__`
    returns one of these instead, exactly the way `asyncio.Lock` fits an
    `async with` block despite `acquire`/`release` being its own real
    methods."""

    __slots__ = ("_lock", "_priority", "_preempt")

    def __init__(self, lock: _PriorityAsyncLock, priority: int, preempt: bool = False):
        self._lock = lock
        self._priority = priority
        self._preempt = preempt

    async def __aenter__(self) -> None:
        await self._lock.acquire(self._priority, preempt=self._preempt)

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._lock.release()


class _PriorityAsyncSemaphore:
    """The counting variant of `_PriorityAsyncLock` (field fix, 2026-09-19
    night): `capacity` holders at once, and when it is full the waiters
    are served highest priority (lowest integer) first, FIFO within a
    tier -- exactly the waiter ordering the radio lock uses, instead of
    `asyncio.Semaphore`'s strict FIFO.

    Why: the per-peer in-flight cap on fragmented sends
    (`direct_fragmented_max_in_flight`, second audit 2026-09-19 evening)
    was a plain `asyncio.Semaphore`. In the night session
    (`fieldtests/raw/Alpha0.1.2/*nighttest*`) that FIFO ignored priority
    and was held across every reconcile round: the desktop's fragmented
    sends waited a median 30s for a slot, and two of the laptop's data
    packets were dropped while both of its slots were held by two
    30-minute LXMF announces reconciling at two hops. With this class a
    data packet arriving behind two queued announces is granted the next
    slot before them; and announce-class sends get a slot of their own
    (see `_fragmented_send_slot`), so they cannot occupy the data slots at
    all.

    Same cancellation contract as `_PriorityAsyncLock`: a waiter cancelled
    before it is granted just leaves the queue; one cancelled in the
    instant after being granted passes the permit on. `locked()` reports
    whether a new acquire would have to wait."""

    def __init__(self, capacity: int):
        self._capacity = max(1, int(capacity))
        self._holders = 0
        self._waiters: "dict[int, collections.deque]" = {}

    @property
    def capacity(self) -> int:
        return self._capacity

    def locked(self) -> bool:
        return self._holders >= self._capacity

    def holders(self) -> int:
        return self._holders

    def waiting(self) -> int:
        return sum(len(dq) for dq in self._waiters.values())

    async def acquire(self, priority: int) -> None:
        if self._holders < self._capacity and not self._waiters:
            self._holders += 1
            return
        fut = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(priority, collections.deque()).append(fut)
        try:
            await fut
        except asyncio.CancelledError:
            dq = self._waiters.get(priority)
            if dq is not None and fut in dq:
                dq.remove(fut)
                if not dq:
                    del self._waiters[priority]
            elif fut.done() and not fut.cancelled():
                # Granted in the same instant we were cancelled: the permit
                # counted for us already -- hand it on, or free it.
                if not self._wake_next():
                    self._holders -= 1
            raise

    def release(self) -> None:
        if self._holders <= 0:
            raise RuntimeError("_PriorityAsyncSemaphore released too many times")
        # The permit passes straight to the next waiter (holders unchanged)
        # or is freed when nobody is waiting.
        if not self._wake_next():
            self._holders -= 1

    def _wake_next(self) -> bool:
        for tier in sorted(self._waiters.keys()):
            dq = self._waiters[tier]
            while dq:
                fut = dq.popleft()
                if not fut.done():
                    fut.set_result(None)
                    if not dq:
                        del self._waiters[tier]
                    return True
            del self._waiters[tier]
        return False


class _DutyCycleLimiter:
    """User-requested fix (2026-09-16, direct user instruction following
    the field-test synthesis above): "all interfaces should spend the
    majority of their time listening" -- a global cap on how much of any
    trailing `window_s` this interface spends transmitting, `max_fraction`
    (default 0.30, i.e. at most 30% of every rolling window -- 10s as
    first requested, 60s since 2026-09-18 by the same user's decision,
    see the module docstring's page-load entry), enforced across
    *every* actual radio-keying command this interface issues (CHANNEL
    fastpath/multi-fragment sends, DIRECT sends, bind frames alike -- see
    each of their own call sites for where `wait_for_budget`/`record` are
    used) -- not a per-transport-shape or per-priority-tier budget, a
    single shared one, since there's exactly one physical radio and every
    one of these already funnels through it regardless of which higher-
    level mechanism decided to send.

    Airtime is *estimated*, not measured: `size_bytes * 8 /
    duty_cycle_estimate_bitrate` -- a *separate* config value from this
    interface's own `bitrate`, not that value reused. First shipped
    reusing `bitrate` directly, and a real zero-hop field test the same
    day immediately showed why that was wrong: `bitrate`'s own deployed
    value is deliberately chosen to model CHANNEL's worst-case *sustained*
    throughput (dominated by deliberate inter-fragment spacing) for RNS
    core's own unrelated pacing/timeout math, not a real single-frame
    over-the-air rate -- reusing it estimated a small ~100-char frame at
    ~10 real seconds of airtime, instantly exhausting the budget on every
    single exchange. `duty_cycle_estimate_bitrate` defaults to 1200 (see
    that config value's own comment for the two-step tuning history --
    300 was tried first and still fired on ordinary Link+Resource
    traffic in the same real hardware test) -- a more realistic raw LoRa
    PHY figure, the right basis for *this* estimate specifically. Since
    2026-09-18 (evening) the estimate is the real LoRa time-on-air from
    `_estimate_tx_airtime_s` whenever SELF_INFO has provided the radio's
    SF/BW/CR (see that method: the bitrate figure quantized a 151-char
    fragment to 1.007s where the air really carries 0.877s at SF7/BW62.5/
    CR8, costing a third of the policy's own allowance); the bitrate
    figure remains the fallback. Either way it is deliberately NOT the wall-clock
    duration of the `send_msg`/`send_chan_msg` command call itself --
    that duration is dominated by local serial/BLE/TCP round-trip
    overhead to the companion radio, not real over-the-air time, and
    would make this limiter's accuracy hostage to transport-specific
    latency that has nothing to do with the actual duty cycle question.
    Good enough for a self-imposed courtesy limit, not a regulatory
    compliance guarantee.

    Implementation: a deque of `(start_time, duration)` samples (monotonic
    clock), pruned to the trailing window on every check. `wait_for_budget`
    sleeps in a loop -- never a single fixed sleep -- until transmitting
    for `estimated_duration_s` more would not push cumulative busy time
    over `window_s * max_fraction`, each iteration waking exactly when the
    oldest sample is due to age out of the window (not a fixed poll
    interval), so it wakes only as often as actually necessary."""

    def __init__(self, window_s: float, max_fraction: float):
        self._window_s = window_s
        self._max_busy_s = window_s * max_fraction
        self._samples: "collections.deque" = collections.deque()

    def _prune(self, now: float) -> None:
        cutoff = now - self._window_s
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def _busy_s(self, now: float) -> float:
        self._prune(now)
        return sum(duration for _start, duration in self._samples)

    async def wait_for_budget(self, estimated_duration_s: float, interrupt: "Optional[asyncio.Event]" = None) -> float:
        """Sleeps until sending for `estimated_duration_s` would not push
        the trailing window's cumulative busy time over the cap. Returns
        the total delay actually applied (0.0 if none was needed) --
        capture/debug-only, never affects whether the send itself
        proceeds. A single transmission longer than the entire cap on its
        own (shouldn't happen in practice -- every real frame this
        interface sends is small) is let through once the window is
        otherwise empty, rather than waiting forever for room that will
        never exist."""
        total_wait = 0.0
        while True:
            now = time.monotonic()
            self._prune(now)
            if self._busy_s(now) + estimated_duration_s <= self._max_busy_s or not self._samples:
                return total_wait
            oldest_start, _oldest_duration = self._samples[0]
            wait_s = max(0.01, (oldest_start + self._window_s) - now)
            if interrupt is not None:
                # Phase 1 (2026-09-20): a raw burst's throttle wait is the
                # longest idle hold of the radio lock in the field (26 s
                # in the zero-hop session, a KEEPALIVE queued 20 s behind
                # it); a queued link handshake -- itself duty-cycle exempt
                # -- ends it.
                if interrupt.is_set():
                    raise _PreemptedForHandshake()
                try:
                    await asyncio.wait_for(interrupt.wait(), timeout=wait_s)
                except asyncio.TimeoutError:
                    total_wait += wait_s
                    continue
                total_wait += time.monotonic() - now
                raise _PreemptedForHandshake()
            await asyncio.sleep(wait_s)
            total_wait += wait_s

    def record(self, duration_s: float) -> None:
        self._samples.append((time.monotonic(), duration_s))
