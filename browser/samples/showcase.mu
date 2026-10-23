#!c=0
# Sample page exercising most of Micron. Lines starting with # are comments.
`c`!Reticulum Field Station`!
`cA test node on the `F5afMeshCore`f LoRa mesh
``
-

>Formatting

Text can be `!bold`!, `*italic`*, `_underlined`_ or `!`*`_all three`_`*`!.
Colours: `Ff55red `F5f5green `F55fblue`f, truecolour `FTff8800orange`f, grey `Fg50fifty percent`f.
Backgrounds: `B335 `Fddddark slate `b`f and `B550`F000 warning `b`f.
`rRight aligned line
`cCentred line
``
Escaped tick: \` and backslash: \\ stay as text.

>Links

`F7af`_`[Home page`72914442a3689add83a09a767963f57c:/page/index.mu]`_`f on another node.
Same node, relative path: `F7af`_`[forms demo`:/page/forms.mu]`_`f
Bare link: `[72914442a3689add83a09a767963f57c]
Jump to an anchor: `F7af`_`[the table`#a-table]`_`f, or `F7af`_`[continue`#]`_`f to the next section.

>Sections
>>Nested section
Content under a level 2 heading is indented by two columns.
>>>Third level
And level 3 indents again.
-~
<
Back at depth 0 after a `!<`! line.

`->Collapsed by default (click me)
This section starts folded, like `!`->`! does in NomadNet.
>>Nested inside the fold
Still hidden until the parent is opened.
`+>Expanded collapsible
This one starts open and can be folded.

>Table
`:a-table
`tc
| Node      | Hops | RSSI |
|-----------|------|------|
| `!alpha`! | 1    | -92  |
| bravo     | 3    | -110 |
`t

>Literal block
`=
`!This is not bold`! because literal mode is on.
`=
