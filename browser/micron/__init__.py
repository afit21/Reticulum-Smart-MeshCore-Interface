"""
Render NomadNet (Micron) pages as terminal-styled HTML.

    from micron import render, render_document

    page = render(markup, destination_hash="72914442a3689add83a09a767963f57c")
    page.html          # <form class="mu-page">...</form>, embed in your UI
    render_document(markup)   # complete standalone HTML page

A host app embeds `page.html`, includes STYLESHEET and SCRIPT once, and
listens for the "micron:navigate" event to fetch linked pages over
Reticulum. See browser/README.md.
"""
import html
import secrets
from pathlib import Path

from .parser import (  # noqa: F401
    NomadUrl,
    RenderedPage,
    css_color,
    page_headers,
    render,
    resolve_url,
    slugify,
)

_STATIC = Path(__file__).with_name("static")
STYLESHEET = (_STATIC / "micron.css").read_text(encoding="utf-8")
SCRIPT = (_STATIC / "micron.js").read_text(encoding="utf-8")


def render_document(markup, destination_hash=None, theme="dark", title="NomadNet page"):
    """
    A complete, self-contained HTML document for one page.

    The Content-Security-Policy blocks every network request (images,
    fonts, scripts, form posts) so a page can't reach the clearnet and
    leak the viewer's IP; only the page's own inline style and our one
    nonce-tagged script are allowed.
    """
    page = render(markup, destination_hash=destination_hash, theme=theme)
    nonce = secrets.token_urlsafe(16)
    csp = (
        "default-src 'none'; style-src 'unsafe-inline'; "
        f"script-src 'nonce-{nonce}'; img-src data:; "
        "form-action 'none'; base-uri 'none'"
    )
    body_bg = page.background or "var(--mu-default-bg, #121417)"
    return (
        "<!doctype html>\n"
        '<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        f'<meta http-equiv="Content-Security-Policy" content="{html.escape(csp)}">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{html.escape(title)}</title>\n"
        f"<style>\n{STYLESHEET}\n"
        f"html, body {{ margin: 0; min-height: 100%; background: {body_bg}; }}\n"
        ".mu-page { min-height: 100vh; box-sizing: border-box; padding-inline: 1ch; }\n"
        "</style>\n</head>\n<body>\n"
        f"{page.html}\n"
        f'<script nonce="{nonce}">\n{SCRIPT}\n</script>\n'
        "</body>\n</html>\n"
    )
