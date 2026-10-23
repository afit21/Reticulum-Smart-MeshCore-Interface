"""
Micron -> HTML converter.

Turns a NomadNet page (Micron markup, `.mu`) into an HTML fragment that
looks like NomadNet's terminal browser: a monospace character grid, the
page's own colours, full-width heading bars and section indentation.

Behaviour follows NomadNet's own parser (nomadnet/ui/textui/MicronParser.py)
so pages render the way their authors saw them, with two deliberate
differences:

  * Text fields are real <textarea> elements the user can drag to resize,
    and they grow as the user types. They start at the width x rows the
    author asked for (`<40x5|name`>), so the page looks as intended until
    the user changes it. Masked fields are <input type=password> and keep
    a fixed size (browsers cannot resize inputs).
  * Tables, partials and images are rendered as simple stand-ins (an HTML
    table and placeholders carrying the data the host app needs to fetch
    them) rather than via NomadNet's urwid widgets.

Everything taken from the page is HTML-escaped, colours are validated
before they reach CSS, and links only ever get an href for nomadnet://
URLs or in-page anchors. The output carries no inline event handlers, so
it works under a strict Content-Security-Policy.
"""
import html
import re
from dataclasses import dataclass, field

HASH_HEX_LENGTH = 32           # RNS truncated hash, hex encoded
DEFAULT_PATH = "/page/index.mu"
URL_SCHEME = "nomadnet"

DEFAULT_FIELD_WIDTH = 24
MAX_FIELD_WIDTH = 256
MAX_FIELD_ROWS = 256
SECTION_INDENT = 2
DIVIDER_REPEAT = 400           # clipped to the line width by CSS

THEMES = {
    "dark": {
        "fg": "ddd",
        "headings": {1: ("222", "bbb"), 2: ("111", "999"), 3: ("000", "777")},
    },
    "light": {
        "fg": "222",
        "headings": {1: ("000", "777"), 2: ("111", "aaa"), 3: ("222", "ccc")},
    },
}

_STRIP_CONTROL_RE = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
_HEX_RE = re.compile(r"[0-9a-fA-F]+\Z")
_HASH_RE = re.compile(r"[0-9a-fA-F]{%d}\Z" % HASH_HEX_LENGTH)
_ANCHOR_CHARS_RE = re.compile(r"[A-Za-z0-9_\-]*")
_SLUG_STRIP_RE = re.compile(
    r"`[FB]T[0-9a-fA-F]{6}"
    r"|`[FB][0-9a-fA-F]{3}"
    r"|`:[A-Za-z0-9_\-]*"
    r"|`[!*_=fbacrl`<>{]"
)
_TABLE_RULE_RE = re.compile(r"[\s|:\-+=]+\Z")


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------

@dataclass
class NomadUrl:
    destination_hash: str      # 32 lowercase hex chars
    path: str                  # e.g. /page/index.mu
    variables: dict = field(default_factory=dict)

    @property
    def href(self):
        return f"{URL_SCHEME}://{self.destination_hash}{self.path}"


@dataclass
class RenderedPage:
    html: str                  # <form class="mu-page">...</form> fragment
    foreground: str            # CSS colour of the page's default text
    background: str | None     # CSS colour from #!bg=, None = theme default
    anchors: list              # anchor names, in page order
    cache_time: int | None     # seconds from a #!c= header, if present


def resolve_url(url, current_hash=None):
    """
    Parse a NomadNet link target the way NomadNet's browser does.

    Accepts `hash`, `hash:/path`, `:/path` (same node as `current_hash`) and
    an optional trailing "`name=value|..." variables part. Returns a
    NomadUrl, or None for anything that isn't a page/file URL on a node
    (in-page anchors, lxmf@ addresses, junk).
    """
    if not url:
        return None
    variables = {}
    if "`" in url:
        url, _, var_str = url.partition("`")
        for entry in var_str.split("|"):
            parts = entry.split("=")
            if len(parts) == 2:
                variables[parts[0]] = parts[1]

    parts = url.split(":")
    if len(parts) == 1:
        dest, path = parts[0], DEFAULT_PATH
    elif len(parts) == 2:
        dest, path = parts
        if dest == "":
            dest = current_hash or ""
        path = path or DEFAULT_PATH
    else:
        return None

    if not _HASH_RE.match(dest):
        return None
    if not path.startswith("/"):
        path = "/" + path
    return NomadUrl(dest.lower(), path, variables)


def slugify(text):
    """Heading text -> anchor name, as NomadNet does it."""
    if text is None:
        return ""
    stripped = _SLUG_STRIP_RE.sub("", text)
    return re.sub(r"[^A-Za-z0-9]+", "-", stripped).strip("-").lower()


def css_color(spec):
    """
    Micron colour spec -> "#rrggbb", or None for "default"/invalid.

    Accepts 3 hex digits (`F0af`), 6 hex digits (`FT00aaff`) and NomadNet's
    grayscale form gNN (0-99 percent).
    """
    if not spec or spec == "default":
        return None
    if len(spec) == 3 and spec[0] == "g":
        if spec[1:].isdigit():
            level = round(int(spec[1:]) * 255 / 100)
            return "#{0:02x}{0:02x}{0:02x}".format(level)
        return None
    if len(spec) == 3 and _HEX_RE.match(spec):
        return "#" + "".join(c * 2 for c in spec.lower())
    if len(spec) == 6 and _HEX_RE.match(spec):
        return "#" + spec.lower()
    return None


def page_headers(markup):
    """Read the #!c=, #!fg= and #!bg= page headers like NomadNet does."""
    cache_time = None
    if markup.startswith("#!c="):
        end = markup.find("\n")
        try:
            cache_time = int(markup[4:end if end != -1 else len(markup)])
        except ValueError:
            pass

    def color_header(tag):
        pos = markup.find(tag)
        if pos < 0:
            return None
        end = markup.find("\n", pos)
        if end == -1:
            end = len(markup)
        value = markup[pos + len(tag):end].strip()
        return value if len(value) in (3, 6) and css_color(value) else None

    return cache_time, color_header("#!fg="), color_header("#!bg=")


def render(markup, destination_hash=None, theme="dark"):
    """
    Convert Micron markup to an HTML fragment.

    `destination_hash` is the node the page came from; it resolves
    relative links (`:/page/x.mu`). `theme` is "dark" or "light" and picks
    the default text and heading colours, as NomadNet's text UI does.
    """
    if isinstance(markup, bytes):
        markup = markup.decode("utf-8", errors="replace")
    markup = markup.replace("\r\n", "\n")
    markup = _STRIP_CONTROL_RE.sub("", markup)

    cache_time, page_fg, page_bg = page_headers(markup)
    theme_spec = THEMES.get(theme, THEMES["dark"])
    renderer = _Renderer(
        theme_spec,
        default_fg=page_fg or theme_spec["fg"],
        default_bg=page_bg or "default",
        current_hash=destination_hash.lower() if destination_hash else None,
    )
    body = renderer.render(markup)

    foreground = css_color(page_fg or theme_spec["fg"])
    background = css_color(page_bg) if page_bg else None
    style = f"--mu-page-fg:{foreground};"
    if background:
        style += f"--mu-page-bg:{background};"
    page_html = (
        f'<form class="mu-page mu-theme-{_attr(theme)}" style="{style}" '
        f'autocomplete="off" novalidate>{body}</form>'
    )
    return RenderedPage(page_html, foreground, background, renderer.anchor_names, cache_time)


# --------------------------------------------------------------------------
# Internals
# --------------------------------------------------------------------------

def _text(s):
    return html.escape(s, quote=False)


def _attr(s):
    return html.escape(str(s), quote=True)


@dataclass
class _Style:
    fg: str
    bg: str
    bold: bool = False
    underline: bool = False
    italic: bool = False

    def copy(self):
        return _Style(self.fg, self.bg, self.bold, self.underline, self.italic)


@dataclass
class _Text:
    style: _Style
    text: str


@dataclass
class _Link:
    style: _Style
    label: str
    url: str
    fields: str | None


@dataclass
class _Field:
    style: _Style
    kind: str            # "text", "checkbox" or "radio"
    name: str
    width: int = DEFAULT_FIELD_WIDTH
    rows: int = 1
    masked: bool = False
    data: str = ""       # initial text, or the label for checkbox/radio
    value: str = ""
    checked: bool = False


@dataclass
class _Anchor:
    name: str


class _Renderer:
    def __init__(self, theme, default_fg, default_bg, current_hash):
        self.theme = theme
        self.default_fg = default_fg
        self.default_bg = default_bg
        self.current_hash = current_hash
        self.style = _Style(default_fg, default_bg)
        self.align = "left"
        self.depth = 0
        self.literal = False
        self.table_mode = False
        self.table_buffer = []
        self.table_align = None
        self.pending_anchors = []
        self.anchor_names = []
        self.rows = []

    # ---- document --------------------------------------------------------

    def render(self, markup):
        for line in markup.split("\n"):
            if line:
                self._parse_line(line)
            else:
                self._emit_line([], kind="blank")
        if self.table_mode and self.table_buffer:
            self._emit_table()
        return "".join(self.rows)

    # ---- block level -----------------------------------------------------

    def _parse_line(self, line):
        if line == "`=":
            self.literal = not self.literal
            return

        pre_escape = False
        if not self.literal:
            collapsible = None
            if line[:2] in ("`+", "`-") and line[2:3] == ">":
                collapsible = "collapsed" if line[1] == "-" else "expanded"
                line = line[2:]

            # A heading can't hold fields; NomadNet renders it as plain text.
            if line.startswith(">") and "`<" in line:
                line = line.lstrip(">")
                collapsible = None
                if not line:
                    return
            first = line[0]

            if first == "\\":
                line = line[1:]
                pre_escape = True
            elif first == "#":
                return

            if line.startswith("`t"):
                self._toggle_table(line[2:])
                return
            if self.table_mode:
                self.table_buffer.append(line)
                return
            if line.startswith("`{"):
                self._emit_partial(line[2:])
                return
            if line.startswith("`("):
                self._emit_image(line[2:])
                return
            if first == "<":
                self.depth = 0
                if len(line) > 1:
                    self._parse_line(line[1:])
                return
            if first == ">":
                self._heading(line, collapsible)
                return
            if first == "-":
                char = line[1] if len(line) == 2 and ord(line[1]) >= 32 else "─"
                self._emit_line([], kind="divider", divider=char)
                return

        # A line that is only formatting commands (e.g. "`Faaa") produces
        # no row at all, exactly as in NomadNet.
        parts = self._inline(line, pre_escape)
        if parts:
            self._emit_line(parts)

    def _heading(self, line, collapsible):
        level = len(line) - len(line.lstrip(">"))
        self.depth = level
        text = line[level:]
        if not text:
            return
        fg, bg = self.theme["headings"][min(level, 3)]
        latched = self.style
        self.style = _Style(fg, bg)
        parts = self._inline(text)
        self.style = latched
        if not parts:
            return
        slug = slugify(text)
        if slug:
            self.pending_anchors.append(slug)
        self._emit_line(parts, kind="heading", level=level, collapsible=collapsible)

    # ---- inline level ----------------------------------------------------

    def _inline(self, line, pre_escape=False):
        if self.literal:
            if line == "\\`=":
                line = "`="
            return [_Text(self.style.copy(), line)]

        out = []
        part = ""
        mode = "text"
        escape = pre_escape
        skip = 0

        def flush():
            nonlocal part
            if part:
                out.append(_Text(self.style.copy(), part))
                part = ""

        for i, c in enumerate(line):
            if skip > 0:
                skip -= 1
                continue

            if mode == "text":
                if c == "\\":
                    if escape:
                        part += c
                    escape = not escape
                elif c == "`":
                    if escape:
                        part += c
                        escape = False
                    else:
                        mode = "formatting"
                        flush()
                else:
                    part += c
                    escape = False
                continue

            # mode == "formatting": one command character after a backtick
            mode = "text"
            s = self.style
            if c == "_":
                s.underline = not s.underline
            elif c == "!":
                s.bold = not s.bold
            elif c == "*":
                s.italic = not s.italic
            elif c in "FB":
                if len(line) >= i + 4:
                    if line[i + 1] == "T" and len(line) >= i + 8:
                        color, skip = line[i + 2:i + 8], 7
                    else:
                        color, skip = line[i + 1:i + 4], 3
                    if c == "F":
                        s.fg = color
                    else:
                        s.bg = color
            elif c == "f":
                s.fg = self.default_fg
            elif c == "b":
                s.bg = self.default_bg
            elif c == "`":
                self.style = _Style(self.default_fg, self.default_bg)
                self.align = "left"
            elif c == "c":
                self.align = "center"
            elif c == "l":
                self.align = "left"
            elif c == "r":
                self.align = "right"
            elif c == "a":
                self.align = "left"
            elif c == ":":
                name = _ANCHOR_CHARS_RE.match(line, i + 1).group(0)
                if name:
                    self.pending_anchors.append(name)
                skip = len(name)
            elif c == "<":
                skip = self._field(line, i, out)
            elif c == "[":
                skip = self._link(line, i, out)

        flush()
        return out

    def _field(self, line, i, out):
        backtick = line.find("`", i + 1)
        if backtick == -1:
            return 0
        end = line.find(">", backtick)
        if end == -1:
            return 0

        content = line[i + 1:backtick]
        f = _Field(self.style.copy(), "text", content)
        if "|" in content:
            components = content.split("|")
            flags, f.name = components[0], components[1]
            if "^" in flags:
                f.kind, flags = "radio", flags.replace("^", "")
            elif "?" in flags:
                f.kind, flags = "checkbox", flags.replace("?", "")
            elif "!" in flags:
                f.masked, flags = True, flags.replace("!", "")
            if flags:
                width, _, rows = flags.partition("x")
                try:
                    f.width = max(1, min(int(width), MAX_FIELD_WIDTH))
                except ValueError:
                    pass
                if rows:
                    try:
                        f.rows = max(1, min(int(rows), MAX_FIELD_ROWS))
                    except ValueError:
                        pass
            f.value = components[2] if len(components) > 2 else ""
            f.checked = len(components) > 3 and components[3] == "*"

        f.data = line[backtick + 1:end]
        if f.kind != "text" and not f.value:
            f.value = f.data
        out.append(f)
        return end - i

    def _link(self, line, i, out):
        end = line.find("]", i)
        if end == -1:
            return 0
        components = line[i + 1:end].split("`")
        if len(components) == 1:
            label, url, fields = "", components[0], None
        elif len(components) == 2:
            label, url, fields = components[0], components[1], None
        elif len(components) == 3:
            label, url, fields = components
        else:
            label, url, fields = "", "", None
        if url:
            out.append(_Link(self.style.copy(), label or url, url, fields))
        return end - i

    # ---- output ----------------------------------------------------------

    def _take_anchors(self):
        html_out = ""
        for name in self.pending_anchors:
            if name and name not in self.anchor_names:
                self.anchor_names.append(name)
                html_out += f'<span class="mu-anchor" id="mu-anchor-{_attr(name)}"></span>'
        self.pending_anchors = []
        return html_out

    def _indent(self):
        return (self.depth - 1) * SECTION_INDENT if self.depth > 0 else 0

    def _emit_line(self, parts, kind="text", level=0, collapsible=None, divider=None):
        classes = ["mu-line", f"mu-{kind}"]
        attrs = [f'data-depth="{self.depth}"']
        styles = []

        if kind == "heading":
            # The heading bar takes the colours of its first segment and
            # spans the full width; only its text is indented.
            line_style = parts[0].style
            classes.append(f"mu-h{min(level, 3)}")
            attrs.append(f'data-level="{level}"')
            if collapsible:
                classes.append("mu-collapsible")
                attrs.append(f'data-collapsed="{"true" if collapsible == "collapsed" else "false"}"')
            if self._indent():
                styles.append(f"text-indent:{self._indent()}ch")
        else:
            line_style = self.style
            if self._indent():
                styles.append(f"padding-inline:{self._indent()}ch")

        fg = css_color(line_style.fg) if line_style.fg != self.default_fg else None
        bg = css_color(line_style.bg) if line_style.bg != self.default_bg else None
        if fg:
            styles.append(f"color:{fg}")
        if bg:
            styles.append(f"background-color:{bg}")
        if self.align != "left":
            styles.append(f"text-align:{self.align}")

        inner = self._take_anchors()
        if kind == "heading" and collapsible:
            inner += '<button type="button" class="mu-fold" aria-label="Toggle section"></button>'
        if kind == "divider":
            inner += f'<span class="mu-rule" aria-hidden="true">{_text(divider * DIVIDER_REPEAT)}</span>'
        inner += self._render_parts(parts, line_style)

        style_attr = f' style="{";".join(styles)}"' if styles else ""
        self.rows.append(
            f'<div class="{" ".join(classes)}" {" ".join(attrs)}{style_attr}>{inner}</div>'
        )

    def _span_style(self, style, line_style):
        classes = []
        if style.bold:
            classes.append("mu-b")
        if style.italic:
            classes.append("mu-i")
        if style.underline:
            classes.append("mu-u")
        css = []
        # Page-default colours go through the CSS variables so the host app
        # can theme them.
        if style.fg != line_style.fg:
            fg = css_color(style.fg) if style.fg != self.default_fg else None
            css.append(f"color:{fg or 'var(--mu-page-fg)'}")
        if style.bg != line_style.bg:
            bg = css_color(style.bg) if style.bg != self.default_bg else None
            css.append(f"background-color:{bg or 'var(--mu-page-bg)'}")
        return classes, css

    def _open_tag(self, tag, style, line_style, extra_classes=(), extra_attrs=""):
        classes, css = self._span_style(style, line_style)
        classes = list(extra_classes) + classes
        out = f"<{tag}"
        if classes:
            out += f' class="{" ".join(classes)}"'
        if css:
            out += f' style="{";".join(css)}"'
        return out + extra_attrs + ">"

    def _render_parts(self, parts, line_style):
        out = []
        # Merge neighbouring text runs that share a style to keep the DOM small.
        merged = []
        for p in parts:
            if isinstance(p, _Text) and merged and isinstance(merged[-1], _Text) \
                    and merged[-1].style == p.style:
                merged[-1] = _Text(p.style, merged[-1].text + p.text)
            else:
                merged.append(p)

        for p in merged:
            if isinstance(p, _Text):
                classes, css = self._span_style(p.style, line_style)
                if not classes and not css:
                    out.append(_text(p.text))
                else:
                    out.append(self._open_tag("span", p.style, line_style) + _text(p.text) + "</span>")
            elif isinstance(p, _Link):
                out.append(self._render_link(p, line_style))
            elif isinstance(p, _Field):
                out.append(self._render_field(p, line_style))
        return "".join(out)

    def _render_link(self, link, line_style):
        attrs = f' data-mu-url="{_attr(link.url)}"'
        if link.fields is not None:
            attrs += f' data-mu-fields="{_attr(link.fields)}"'
        if link.url.startswith("#"):
            name = link.url[1:]
            attrs += f' href="#mu-anchor-{_attr(name)}"' if name else ' href="#"'
            attrs += ' data-mu-anchor="' + _attr(name) + '"'
        else:
            target = resolve_url(link.url, self.current_hash)
            if target:
                attrs += f' href="{_attr(target.href)}"'
        return self._open_tag("a", link.style, line_style, ["mu-link"], attrs) + _text(link.label) + "</a>"

    def _render_field(self, f, line_style):
        name = _attr(f.name)
        if f.kind in ("checkbox", "radio"):
            checked = " checked" if f.checked else ""
            control = (
                f'<input type="{f.kind}" class="mu-{f.kind}" name="{name}" '
                f'value="{_attr(f.value)}" data-mu-field="{name}"{checked}>'
            )
            label = f"<span>{_text(f.data)}</span>" if f.data else ""
            return self._open_tag("label", f.style, line_style, ["mu-choice"]) + control + label + "</label>"

        size = f"--mu-w:{f.width};--mu-r:{f.rows}"
        classes, css = self._span_style(f.style, line_style)
        # Field colours always come from the field's own style, so a field
        # on a coloured line still shows its `B444 background.
        css = [c for c in css if not c.startswith(("color:", "background-color:"))]
        fg, bg = css_color(f.style.fg), css_color(f.style.bg)
        if fg and f.style.fg != self.default_fg:
            css.append(f"color:{fg}")
        if bg:
            css.append(f"background-color:{bg}")
        style = ";".join([size] + css)
        cls = " ".join(["mu-field"] + (["mu-masked"] if f.masked else []) + classes)

        if f.masked:
            return (
                f'<input type="password" class="{cls}" style="{style}" name="{name}" '
                f'data-mu-field="{name}" value="{_attr(f.data)}" size="{f.width}" spellcheck="false">'
            )
        return (
            f'<textarea class="{cls}" style="{style}" name="{name}" data-mu-field="{name}" '
            f'rows="{f.rows}" cols="{f.width}" spellcheck="false">{_text(f.data)}</textarea>'
        )

    # ---- tables, partials, images ---------------------------------------

    def _toggle_table(self, spec):
        if self.table_mode:
            self._emit_table()
            return
        self.table_mode = True
        self.table_buffer = []
        self.table_align = spec[0] if spec[:1] in ("l", "c", "r") else None

    def _emit_table(self):
        lines, align = self.table_buffer, self.table_align
        self.table_mode, self.table_buffer, self.table_align = False, [], None
        if not lines:
            return

        rows = []
        header = len(lines) > 1 and _TABLE_RULE_RE.match(lines[1]) is not None
        for line in lines:
            if _TABLE_RULE_RE.match(line):
                continue
            cells = line.strip()
            if cells.startswith("|"):
                cells = cells[1:]
            if cells.endswith("|"):
                cells = cells[:-1]
            rows.append([c.strip() for c in cells.split("|")])

        line_style = self.style
        out = []
        for r, cells in enumerate(rows):
            tag = "th" if header and r == 0 else "td"
            out.append("<tr>" + "".join(
                f"<{tag}>{self._render_parts(self._inline(c), line_style)}</{tag}>" for c in cells
            ) + "</tr>")
        style = ""
        if self._indent():
            style = f' style="padding-inline:{self._indent()}ch"'
        table_align = {"c": "center", "r": "right"}.get(align, "left")
        self.rows.append(
            f'<div class="mu-line mu-table-wrap mu-table-{table_align}" data-depth="{self.depth}"{style}>'
            f'{self._take_anchors()}<table class="mu-table">{"".join(out)}</table></div>'
        )

    def _emit_partial(self, spec):
        end = spec.find("}")
        if end == -1:
            return
        components = spec[:end].split("`")
        url = components[0] if len(components) <= 3 else ""
        if not url:
            return
        refresh = None
        if len(components) >= 2:
            try:
                refresh = float(components[1])
            except ValueError:
                refresh = None
            if refresh is not None and refresh < 1:
                refresh = None
        fields = components[2] if len(components) == 3 else ""
        attrs = f' data-mu-partial-url="{_attr(url)}"'
        target = resolve_url(url, self.current_hash)
        if target:
            attrs += f' data-mu-partial-href="{_attr(target.href)}"'
        if refresh:
            attrs += f' data-mu-refresh="{refresh:g}"'
        if fields:
            attrs += f' data-mu-fields="{_attr(fields)}"'
        self.rows.append(
            f'<div class="mu-line mu-partial" data-depth="{self.depth}"{attrs}>'
            f'{self._take_anchors()}<span class="mu-pending">⧖</span></div>'
        )

    def _emit_image(self, spec):
        end = spec.rfind(")")
        if end <= 0:
            return
        components = spec[:end].split("`")
        if len(components) < 2:
            return
        alt, url = components[0].strip(), components[-1].strip()
        attrs = f' data-mu-image-url="{_attr(url)}"'
        for prop in components[1:-1]:
            key, eq, value = prop.partition("=")
            if eq and key.strip() in ("w", "h", "a"):
                attrs += f' data-mu-image-{key.strip()}="{_attr(value.strip())}"'
        self.rows.append(
            f'<div class="mu-line mu-image" data-depth="{self.depth}"{attrs}>'
            f'{self._take_anchors()}<span class="mu-image-alt">[image: {_text(alt or url)}]</span></div>'
        )
