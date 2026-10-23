"""
Tests for the Micron -> HTML converter.

    python3 -m unittest discover -s browser/tests -v
"""
import html
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from micron import render, render_document, resolve_url, css_color, slugify  # noqa: E402

NODE = "72914442a3689add83a09a767963f57c"


def lines(markup, **kw):
    """Rendered page body split into its line <div>s."""
    body = render(markup, **kw).html
    inner = body[body.index(">") + 1:body.rindex("</form>")]
    return [l for l in inner.split('<div class="mu-line') if l]


class ColorTests(unittest.TestCase):
    def test_short_hex_doubles(self):
        self.assertEqual(css_color("f80"), "#ff8800")

    def test_truecolor(self):
        self.assertEqual(css_color("00AAff"), "#00aaff")

    def test_grayscale(self):
        self.assertEqual(css_color("g50"), "#808080")
        self.assertEqual(css_color("g00"), "#000000")

    def test_invalid_and_default(self):
        for bad in ("default", "", None, "zzz", "12", "f;x", "gxx"):
            self.assertIsNone(css_color(bad), bad)


class InlineFormattingTests(unittest.TestCase):
    def test_bold_italic_underline(self):
        out = render("`!b`! `*i`* `_u`_").html
        self.assertIn('<span class="mu-b">b</span>', out)
        self.assertIn('<span class="mu-i">i</span>', out)
        self.assertIn('<span class="mu-u">u</span>', out)

    def test_colours(self):
        out = render("`Ff00red`f `B00fblue`b `FT112233tc").html
        self.assertIn('style="color:#ff0000">red</span>', out)
        self.assertIn('background-color:#0000ff">blue</span>', out)
        self.assertIn('style="color:var(--mu-page-fg)"> </span>', out)
        self.assertIn("color:#112233", out)

    def test_reset_clears_style_and_alignment(self):
        # Alignment is whatever is in effect at the end of the line.
        rows = lines("`c`!x``y")
        self.assertNotIn("text-align", rows[0])
        self.assertIn('<span class="mu-b">x</span>y</div>', rows[0])

    def test_escapes(self):
        out = render("a \\` b \\\\ c").html
        self.assertIn("a ` b \\ c", out)

    def test_formatting_only_line_emits_no_row(self):
        self.assertEqual(len(lines("`Faaa\ntext")), 1)

    def test_alignment(self):
        self.assertIn("text-align:center", lines("`cmiddle")[0])
        self.assertIn("text-align:right", lines("`rright")[0])


class BlockTests(unittest.TestCase):
    def test_comments_are_dropped(self):
        self.assertEqual(lines("# hidden\nshown"), lines("shown"))

    def test_heading_levels_and_indent(self):
        rows = lines(">One\n>>Two\nbody")
        self.assertIn("mu-h1", rows[0])
        self.assertIn("mu-h2", rows[1])
        self.assertIn("text-indent:2ch", rows[1])
        self.assertIn("padding-inline:2ch", rows[2])

    def test_depth_reset(self):
        rows = lines(">>Deep\n<flat")
        self.assertNotIn("padding-inline", rows[1])

    def test_heading_anchor(self):
        page = render(">Introduction & Setup\n`:custom here")
        self.assertEqual(page.anchors, ["introduction-setup", "custom"])
        self.assertIn('id="mu-anchor-introduction-setup"', page.html)

    def test_divider(self):
        self.assertIn("───", lines("-")[0])
        self.assertIn("===", lines("-=")[0])

    def test_literal(self):
        out = render("`=\n`!not bold`!\n`=").html
        self.assertIn("`!not bold`!", out)
        self.assertNotIn("mu-b", out)

    def test_collapsible(self):
        rows = lines("`->Folded\nhidden")
        self.assertIn('data-collapsed="true"', rows[0])
        self.assertIn("mu-fold", rows[0])

    def test_heading_with_field_becomes_text(self):
        rows = lines(">Name: `<n`>")
        self.assertNotIn("mu-heading", rows[0])
        self.assertIn("<textarea", rows[0])

    def test_table(self):
        out = render("`t\n| a | b |\n|---|---|\n| 1 | `!2`! |\n`t").html
        self.assertIn("<th>a</th>", out)
        self.assertIn('<td><span class="mu-b">2</span></td>', out)
        self.assertNotIn("---", out)

    def test_page_headers(self):
        page = render("#!c=60\n#!bg=222\n#!fg=0f0\nhi")
        self.assertEqual(page.cache_time, 60)
        self.assertEqual(page.background, "#222222")
        self.assertEqual(page.foreground, "#00ff00")

    def test_control_characters_stripped(self):
        self.assertIn(">ab</div>", render("a\x1b[31mb".replace("[31m", "")).html)


class FieldTests(unittest.TestCase):
    def test_default_field(self):
        out = render("`<name`Bob>").html
        self.assertIn('<textarea class="mu-field"', out)
        self.assertIn("--mu-w:24;--mu-r:1", out)
        self.assertIn(">Bob</textarea>", out)

    def test_sized_multirow_field(self):
        out = render("`<40x5|notes`>").html
        self.assertIn("--mu-w:40;--mu-r:5", out)
        self.assertIn('rows="5" cols="40"', out)

    def test_size_limits(self):
        out = render("`<9999x9999|big`>").html
        self.assertIn("--mu-w:256;--mu-r:256", out)

    def test_masked(self):
        out = render("`<!16|pw`secret>").html
        self.assertIn('<input type="password"', out)
        self.assertIn('value="secret"', out)

    def test_field_background(self):
        self.assertIn("background-color:#444444", render("`B444`<f`>`b").html)

    def test_checkbox_and_radio(self):
        out = render("`<?|opt|yes|*`>Label\n`<^|pick|a`>A").html
        self.assertIn('type="checkbox" class="mu-checkbox" name="opt" value="yes" data-mu-field="opt" checked', out)
        self.assertIn('type="radio" class="mu-radio" name="pick" value="a"', out)

    def test_checkbox_label_used_as_value_when_missing(self):
        self.assertIn('value="Label"', render("`<?|opt`Label>").html)


class LinkTests(unittest.TestCase):
    def test_absolute(self):
        out = render(f"`[home`{NODE}:/page/index.mu]").html
        self.assertIn(f'href="nomadnet://{NODE}/page/index.mu"', out)
        self.assertIn(">home</a>", out)

    def test_relative_uses_current_node(self):
        out = render("`[x`:/page/a.mu]", destination_hash=NODE).html
        self.assertIn(f'href="nomadnet://{NODE}/page/a.mu"', out)

    def test_relative_without_node_has_no_href(self):
        out = render("`[x`:/page/a.mu]").html
        self.assertNotIn("href=", out)
        self.assertIn('data-mu-url=":/page/a.mu"', out)

    def test_bare_hash_link(self):
        out = render(f"`[{NODE}]").html
        self.assertIn(f'href="nomadnet://{NODE}/page/index.mu"', out)
        self.assertIn(f">{NODE}</a>", out)

    def test_fields_spec_kept_for_script(self):
        out = render(f"`[go`{NODE}:/p.mu`*|a=1]").html
        self.assertIn('data-mu-fields="*|a=1"', out)

    def test_anchor_links(self):
        out = render("`[top`#intro] `[next`#]").html
        self.assertIn('href="#mu-anchor-intro"', out)
        self.assertIn('href="#" data-mu-anchor=""', out)

    def test_resolve_url(self):
        u = resolve_url(f"{NODE.upper()}:/page/x.mu`a=1|b=2")
        self.assertEqual((u.destination_hash, u.path, u.variables), (NODE, "/page/x.mu", {"a": "1", "b": "2"}))
        self.assertIsNone(resolve_url("not-a-hash:/x"))
        self.assertIsNone(resolve_url("lxmf@" + NODE))


class SafetyTests(unittest.TestCase):
    """Pages come from strangers on the mesh; nothing may escape into HTML."""

    def test_text_is_escaped(self):
        out = render("x <script>alert(1)</script> & <b>").html
        self.assertNotIn("<script", out)
        self.assertIn("&lt;script&gt;", out)

    def test_javascript_url_gets_no_href(self):
        out = render("`[click`javascript:alert(1)]").html
        self.assertNotIn("href=", out)

    def test_attribute_injection_in_field_name(self):
        out = render('`<16|x" onfocus="alert(1)`v>').html
        self.assertNotIn('" onfocus="', out)
        self.assertIn("&quot;", out)

    def test_attribute_injection_in_link(self):
        out = render('`[a`#x" onclick="alert(1)]').html
        self.assertNotIn('" onclick="', out)

    def test_textarea_breakout(self):
        out = render("`<f`</textarea><img src=x>>").html
        self.assertNotIn("<img", out)

    def test_bad_colour_never_reaches_css(self):
        out = render("`Fx;}`!text").html
        self.assertNotIn("x;}", out)

    def test_document_has_strict_csp(self):
        doc = html.unescape(render_document("hi"))
        self.assertIn("default-src 'none'", doc)
        self.assertIn("script-src 'nonce-", doc)


class SlugTests(unittest.TestCase):
    def test_slugify(self):
        self.assertEqual(slugify("Hello World"), "hello-world")
        self.assertEqual(slugify("`!Bold`! title!"), "bold-title")


if __name__ == "__main__":
    unittest.main()
