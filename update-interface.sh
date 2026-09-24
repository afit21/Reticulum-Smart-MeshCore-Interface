#!/usr/bin/env bash
#
# update-interface.sh -- install the RELEASED SmartMeshCoreInterface.py (the
# main branch) from GitHub into a Reticulum install's interfaces/ directory.
# update-interface-dev.sh is the same script for the development branch.
#
# Defaults to the main branch and ~/.reticulum. The downloaded file is
# checked before it replaces anything: a truncated download, an HTML error page
# or a Python syntax error leaves the installed interface untouched, because a
# broken interface file stops rnsd/MeshChat from starting at all.
# The interface imports the `meshcore` python library at startup and panics
# rnsd without it, so the script also makes sure that library is importable by
# the python that runs rnsd, installing it with pip if it is not.
#
#   ./update-interface.sh                      # main -> ~/.reticulum
#   ./update-interface.sh --branch development # (or use update-interface-dev.sh)
#   ./update-interface.sh --config-dir ~/.reticulum_test
#   ./update-interface.sh --check              # report only, install nothing
#   ./update-interface.sh --force              # reinstall even if unchanged
#   ./update-interface.sh --skip-deps          # don't check/install the meshcore library
#
# Remote hosts: run it over ssh, e.g.
#   ssh -i ~/claudtolaptop afi@192.168.20.44 'bash -s' < update-interface.sh
#
set -euo pipefail

REPO="afit21/Reticulum-Smart-MeshCore-Interface"
BRANCH="main"
SRC_PATH="Interface/SmartMeshCoreInterface.py"
CONFIG_DIR="${RNS_CONFIG_DIR:-$HOME/.reticulum}"
KEEP_BACKUPS=5
CHECK_ONLY=0
FORCE=0
SKIP_DEPS=0

die() { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }
info() { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m  ok:\033[0m %s\n' "$*"; }
warn() { printf '\033[33mwarn:\033[0m %s\n' "$*" >&2; }

while [ $# -gt 0 ]; do
  case "$1" in
    -b|--branch)     BRANCH="${2:?--branch needs a value}"; shift 2 ;;
    -d|--config-dir) CONFIG_DIR="${2:?--config-dir needs a value}"; shift 2 ;;
    -c|--check)      CHECK_ONLY=1; shift ;;
    -f|--force)      FORCE=1; shift ;;
    -s|--skip-deps)  SKIP_DEPS=1; shift ;;
    -h|--help)       sed -n '3,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)               die "unknown option: $1 (try --help)" ;;
  esac
done

command -v curl >/dev/null || die "curl is not installed"
PYTHON="$(command -v python3 || true)"
[ -n "$PYTHON" ] || die "python3 is not installed (needed to syntax-check the download)"

# --- the meshcore python library --------------------------------------------
# The interface does `import meshcore` when rnsd loads it and panics rnsd if
# that fails, so check the python that actually runs rnsd (its shebang), not
# just whichever python3 is first on PATH -- a pipx or venv rnsd has its own.
RNSD_PY="$PYTHON"
if command -v rnsd >/dev/null 2>&1; then
  shebang="$(head -n1 "$(command -v rnsd)" 2>/dev/null || true)"
  case "$shebang" in
    '#!'*)
      cand="$(printf '%s\n' "${shebang#\#!}" | awk '{ if ($1 ~ /\/env$/) print $2; else print $1 }')"
      case "$cand" in
        /*) [ -x "$cand" ] && RNSD_PY="$cand" ;;
        ?*) cand="$(command -v "$cand" 2>/dev/null || true)"; [ -n "$cand" ] && RNSD_PY="$cand" ;;
      esac ;;
  esac
fi

meshcore_version() {
  "$RNSD_PY" -c 'import importlib.metadata as m; print(m.version("meshcore"))' 2>/dev/null || echo "version unknown"
}

ensure_meshcore() {
  if "$RNSD_PY" -c 'import meshcore' >/dev/null 2>&1; then
    ok "meshcore library $(meshcore_version) importable by $RNSD_PY"
    return 0
  fi
  if [ "$CHECK_ONLY" -eq 1 ]; then
    warn "the meshcore python library is NOT importable by $RNSD_PY -- the interface panics rnsd without it"
    return 0
  fi
  info "meshcore library not importable by $RNSD_PY -- installing it"
  case "$RNSD_PY" in
    */pipx/venvs/*)
      command -v pipx >/dev/null 2>&1 && pipx inject rns meshcore >/dev/null 2>&1 || true ;;
  esac
  "$RNSD_PY" -c 'import meshcore' >/dev/null 2>&1 \
    || "$RNSD_PY" -m pip install --quiet meshcore >/dev/null 2>&1 \
    || "$RNSD_PY" -m pip install --quiet --user meshcore >/dev/null 2>&1 \
    || true
  "$RNSD_PY" -c 'import meshcore' >/dev/null 2>&1 \
    || die "could not install the meshcore library for $RNSD_PY -- install it by hand ('$RNSD_PY -m pip install meshcore', or 'pipx inject rns meshcore' for a pipx rnsd) and re-run"
  ok "meshcore library $(meshcore_version) installed for $RNSD_PY"
}

if [ "$SKIP_DEPS" -eq 1 ]; then
  info "--skip-deps given: not checking the meshcore library"
else
  ensure_meshcore
fi

DEST_DIR="$CONFIG_DIR/interfaces"
DEST="$DEST_DIR/SmartMeshCoreInterface.py"
URL="https://raw.githubusercontent.com/$REPO/$BRANCH/$SRC_PATH"

[ -d "$CONFIG_DIR" ] || die "config dir does not exist: $CONFIG_DIR (use --config-dir)"
mkdir -p "$DEST_DIR"

TMP="$(mktemp -t SmartMeshCoreInterface.XXXXXX.py)"
trap 'rm -f "$TMP"' EXIT

info "fetching $BRANCH from github.com/$REPO"
# --fail so a 404 (bad branch/path) is an error, not a saved error page.
curl -fsSL --retry 3 --retry-delay 2 --connect-timeout 15 -o "$TMP" "$URL" \
  || die "download failed: $URL (does branch '$BRANCH' exist?)"

# --- sanity checks on what we actually got -------------------------------
bytes=$(wc -c < "$TMP" | tr -d ' ')
[ "$bytes" -ge 100000 ] || die "downloaded file is only ${bytes} bytes -- truncated or not the interface"
grep -q "class SmartMeshCoreInterface" "$TMP" \
  || die "downloaded file does not define SmartMeshCoreInterface -- wrong path or an error page"
"$PYTHON" -m py_compile "$TMP" 2>/dev/null \
  || die "downloaded file is not valid Python -- refusing to install it"
rm -rf "$(dirname "$TMP")/__pycache__" 2>/dev/null || true
ok "${bytes} bytes, defines SmartMeshCoreInterface, compiles"

new_sum=$("$PYTHON" -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$TMP")
if [ -f "$DEST" ]; then
  old_sum=$("$PYTHON" -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$DEST")
  old_lines=$(wc -l < "$DEST" | tr -d ' ')
  new_lines=$(wc -l < "$TMP" | tr -d ' ')
  info "installed: ${old_lines} lines (${old_sum:0:12})"
  info "on $BRANCH: ${new_lines} lines (${new_sum:0:12})"
  if [ "$old_sum" = "$new_sum" ]; then
    if [ "$FORCE" -eq 1 ]; then
      warn "identical to what is installed, but --force given"
    else
      ok "already up to date -- nothing to do"
      exit 0
    fi
  fi
else
  info "no interface installed yet at $DEST"
fi

if [ "$CHECK_ONLY" -eq 1 ]; then
  info "--check given: not installing"
  if [ -f "$DEST" ] && command -v diff >/dev/null; then
    diff -u "$DEST" "$TMP" | head -60 || true
    echo "(diff truncated to 60 lines)"
  fi
  exit 0
fi

# --- install, keeping a timestamped backup -------------------------------
if [ -f "$DEST" ]; then
  backup="$DEST.bak.$(date +%Y%m%dT%H%M%S)"
  cp -p "$DEST" "$backup"
  ok "backed up to $(basename "$backup")"
  # Keep only the newest $KEEP_BACKUPS backups.
  ls -1t "$DEST".bak.* 2>/dev/null | tail -n +$((KEEP_BACKUPS + 1)) | while read -r stale; do
    rm -f "$stale"
  done
fi

install -m 0644 "$TMP" "$DEST"
ok "installed $DEST"

# --- who needs restarting for this to take effect? ----------------------
running=""
for pat in rnsd nomadnet meshchat MeshChat lxmd; do
  if pgrep -f "$pat" >/dev/null 2>&1; then
    running="$running $pat"
  fi
done
if [ -n "$running" ]; then
  warn "Reticulum loads this file at startup -- restart to pick it up:$running"
else
  info "no rnsd/MeshChat/NomadNet process seen; it will load on next start"
fi

printf '\033[32mdone:\033[0m %s @ %s -> %s\n' "$BRANCH" "${new_sum:0:12}" "$DEST"
