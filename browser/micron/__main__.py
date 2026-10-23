"""
Render a .mu file to a standalone HTML page.

    python3 -m micron page.mu                  # writes page.html next to it
    python3 -m micron page.mu -o out.html --theme light
    python3 -m micron page.mu --node 72914442a3689add83a09a767963f57c

Run from the browser/ directory (or put it on PYTHONPATH).
"""
import argparse
import sys
from pathlib import Path

from . import render_document


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python3 -m micron", description=__doc__.strip().splitlines()[0])
    ap.add_argument("source", type=Path, help=".mu file to render")
    ap.add_argument("-o", "--output", type=Path, help="output .html (default: next to the source)")
    ap.add_argument("--theme", choices=("dark", "light"), default="dark")
    ap.add_argument("--node", help="destination hash the page came from (resolves :/relative links)")
    args = ap.parse_args(argv)

    markup = args.source.read_text(encoding="utf-8", errors="replace")
    out = args.output or args.source.with_suffix(".html")
    out.write_text(
        render_document(markup, destination_hash=args.node, theme=args.theme, title=args.source.name),
        encoding="utf-8",
    )
    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
