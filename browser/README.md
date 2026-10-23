# Micron → HTML (NomadNet page renderer)

The first piece of a NomadNet browser. It converts NomadNet pages (Micron
markup, `.mu`) into HTML that looks like NomadNet's terminal browser: a
monospace grid, the page's own colours, full-width heading bars and section
indents. The webview handles the rest.

Text input fields are real, resizable `<textarea>`s:

- They **start at the size the page author set**. `` `<16|name`> `` is 16
  columns wide. `` `<40x5|notes`> `` is 40 × 5. Fields without a size are 24 wide.
- The user can **drag the corner handle** to make a field bigger or smaller.
- They **grow downwards while you type** and never shrink on their own, so a
  size the user dragged out is kept.
- Masked fields (`` `<!16|pw`> ``) are `<input type=password>`, so they're the
  one kind of field that can't be resized.

## Try it

```sh
cd browser
python3 -m micron samples/forms.mu       # writes samples/forms.html
python3 -m micron samples/showcase.mu --node 72914442a3689add83a09a767963f57c
python3 -m unittest discover -s tests -v
```

Open the `.html` in a browser. `forms.mu` shows every field type, and
`showcase.mu` covers the rest of Micron.

## Using it from the browser app

```python
from micron import render, STYLESHEET, SCRIPT

page = render(markup, destination_hash=node_hash)   # node the page came from
page.html         # <form class="mu-page">…</form>, put it in a tab
page.background   # #!bg= colour or None, for the tab chrome
page.cache_time   # #!c= header, seconds
```

Include `STYLESHEET` and `SCRIPT` once in the host page. Pages added later
need `Micron.attach(element)`. `render_document()` builds a standalone page
with both inlined.

Clicking a link fires a cancelable `micron:navigate` event. It's sent before
anything else happens, so the app can fetch the target over Reticulum:

```js
document.addEventListener("micron:navigate", (e) => {
  e.preventDefault();
  const { url, href, requestData } = e.detail;
  // url:         raw Micron target, e.g. ":/page/guestbook.mu"
  // href:        "nomadnet://<hash>/page/guestbook.mu" (null if unresolvable)
  // requestData: {field_name: "...", var_action: "sign"} or null,
  //              built exactly like NomadNet: field_ for fields, var_ for
  //              link variables, checkboxes joined with ","
  backend.open(href, requestData);   // → RNS.Link … link.request(path, data=requestData)
});
```

If nothing cancels the event, `#anchor` links scroll within the page, and
`nomadnet://` links navigate with the request data as a query string. That
suits a Qt `QWebEngineUrlSchemeHandler` registered for `nomadnet`.

## What's supported

All of the Micron that NomadNet documents:

- Bold, italic and underline.
- Colours: 3-digit hex, `T` truecolour and `gNN` grey.
- `` ` `` reset, alignment, and `\` escapes.
- `>` headings and section indents, `<` depth reset, and `-` dividers.
- `` `= `` literal blocks and `#` comments.
- `#!fg=`, `#!bg=` and `#!c=` page headers.
- Links, including relative `:/path` links, link fields and variables.
- Anchors: `` `:name ``, plus automatic anchors from headings, `#name` and `#`.
- Text, multi-row and masked fields, checkboxes and radio groups.
- Collapsible headings (`` `+> `` and `` `-> ``).

Tables are rendered as an HTML table. Partials (`` `{ ``) and images (`` `( ``)
become placeholders. The data needed to fetch them is in `data-mu-*`
attributes, so the app can fill them in.

## Safety

Pages come from strangers on the mesh, so:

- All page text and attributes are HTML-escaped.
- Colours are validated before they reach CSS.
- Only `nomadnet://` and in-page links get an `href`. A `javascript:` link
  renders as inert text.
- The output has no inline event handlers.
- `render_document()` sets a CSP that blocks all network loads, so a page
  can't pull clearnet resources and leak your IP.

If the app embeds pages itself, it should apply the same CSP.

## Licence note

The rendering rules follow NomadNet's `MicronParser.py`, which is GPLv3, the
same licence as this repository.
