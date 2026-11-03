# Surgewave branding pipeline

**The generator is shared.** `generate-logos.mjs`, `outline-wordmark.py` and `Inter-LICENSE.txt` are byte-identical
copies of the files in Terracle's `scripts/branding/` (Terracle, Surgewave, Modelzero and Sealbolt carry the same
copy); only `brand.json`, `wordmark.json` and the source drawing are Surgewave's own. Do not change the copies here alone:
change the generator in every product at once and keep the copies identical, until it moves into one shared tool.

`generate-logos.mjs` names no product: everything product-specific is in `brand.json` beside it (the product name, the
output prefix, the source drawing, the wordmark text and its outline, the colour roles, the light background and where
the web icons are copied to); `--config <file>` names a configuration elsewhere.

| Key | Surgewave | What it sets |
|---|---|---|
| `root` | `../..` | The repository root, relative to `brand.json`; every other path is relative to it. |
| `product` | `Surgewave` | The `aria-label` of every SVG and the lockups' labels. |
| `prefix` | `surgewave` | File names (`<prefix>_mark_*`, `<prefix>_lockup_*`) and the adaptive CSS classes (`<prefix>-<role>`). |
| `source` | `scripts/branding/surgewave_mark_source.svg` | The owner's drawing of the mark. |
| `wordmark` | `{ text: Surgewave, outline: scripts/branding/wordmark.json }` | The wordmark; the run stops when the outline holds another text. |
| `images` | `images` | The output folder; the run deletes what it did not write there. |
| `roles` | wave, crest, ink | Per role the light and dark colour and the drawing's fills that mean it. |
| `wordmarkRole` | `ink` | The role that paints the wordmark. |
| `background` | Terracle's light glass | The light background of `apple-touch-icon.png` and `og-image`. |
| `distribute` | `site/assets/images`, `docs`, `docs/templates/surgewave-theme/public`, `src/Kuestenlogik.Surgewave.Control/wwwroot` | Folders that get copies of the web icons and the Open Graph image. |

Every logo file in `images/` comes from one drawing by the owner. Everything in `images/` is derived: delete it and run
the generator again.

## Sources

| File | What it is | Feeds |
|---|---|---|
| `scripts/branding/surgewave_mark_source.svg` | The mark: the wave intersecting an upward arc | every file in `images/` |
| `scripts/branding/wordmark.json` | "Surgewave" in Inter Bold as an outline, made by `outline-wordmark.py` | the lockups' wordmark |

The source lives here and not in `images/` so that `images/` holds only outputs, and the generator never writes to
it. The outputs are cleaned: no Inkscape/sodipodi metadata, no unused defs, no empty groups, `role="img"` and
`aria-label="Surgewave"`. To change the mark, edit `surgewave_mark_source.svg` in Inkscape and run the generator.

## Colours

The generator maps every fill of the drawing to a role; a fill it does not know stops the run.

| Role | Light (as drawn) | Dark (`adaptive`) | Fills of the drawing | Why |
|---|---|---|---|---|
| wave | `#33bcff` | `#33bcff` | `#33bcff` | the wave; Surgewave Blue everywhere (BRAND.md), 8.6:1 on #121212 |
| crest | `#66cfff` | `#66cfff` | `#66cfff` | the upward arc, the lighter blue; 10.6:1 on #121212, stays |
| ink | `#003e60` | `#ffffff` | `#003e60` | the body and the wordmark; navy on #121212 would be 1.7:1 (BRAND.md: white on dark) |

`black` paints every role `#000000`, `white` every role `#ffffff`. The `adaptive` files carry CSS: the system's
`prefers-color-scheme`, and where the SVG is inlined the page's own toggle (`data-theme` or `data-theme-resolved`).
The light background of `apple-touch-icon.png` and `og-image` is a radial gradient over `#F6F8FA`, `#E6EBEF`, `#D3DCE3`.

## Run

```sh
BRANDING_PLAYWRIGHT_FROM=<absolute path to a package.json> node scripts/branding/generate-logos.mjs [--config <brand.json>] [--contact-sheet <file.png>]
```

Playwright comes from the root `package.json` (`npm install` there), so `BRANDING_PLAYWRIGHT_FROM` names it; the path must be absolute (e.g. `$PWD/package.json`). Edge is the browser
(channel `msedge`).

The generator:

1. reads the source (never writes it) and maps its fills to the roles;
2. writes `surgewave_mark_{colored,black,white,adaptive}.svg` (the drawing's own frame) and the lockups
   `surgewave_lockup_{h,v}_{colored,black,white,adaptive}.svg` (horizontal 1100×200, vertical 480×380; the mark in a square
   around its ink beside or above the wordmark);
3. renders the PNGs in Edge with a transparent background: marks 512 and 1024 px, lockups 1100×200 / 2200×400 and
   480×380 / 960×760; the `adaptive` SVGs get no PNG, a PNG cannot follow the theme (use `colored` or `white`);
4. writes `favicon.svg` (the mark, cropped square, adaptive), `favicon.ico` (16/32/48, PNG entries) and
   `apple-touch-icon.png` (180, the mark on the light background);
5. writes `og-image.svg` and `og-image.png` (1200×630, the Open Graph image for link previews): the horizontal colored
   lockup centred on the light background, opaque;
6. deletes every file in `images/` it did not write (`README.md` stays), so the folder holds exactly its outputs;
7. copies the files `distribute` names: `favicon.svg, favicon.ico, apple-touch-icon.png, surgewave_mark_colored.png, og-image.png` to `site/assets/images/`; `favicon.ico` to `docs/`; `favicon.svg, surgewave_mark_adaptive.svg` to `docs/templates/surgewave-theme/public/`; `favicon.svg, favicon.ico, apple-touch-icon.png` to `src/Kuestenlogik.Surgewave.Control/wwwroot/`;
8. with `--contact-sheet`, renders every SVG, `favicon.ico`, `apple-touch-icon.png` and `og-image.png` at 16, 32, 64,
   180 and 512 px on light and on dark into one PNG for review.

## The wordmark

`wordmark.json` is committed, so the generator needs no font. Make it again only when the text or the font changes:

```sh
python -m venv .venv && .venv/Scripts/python -m pip install fonttools uharfbuzz
.venv/Scripts/python scripts/branding/outline-wordmark.py <path/to/Inter-Bold.ttf> --text Surgewave
```

(The script's default `--text` is Terracle's, because the script is the shared copy.) The font is Inter 4.1 from its
official release (<https://github.com/rsms/inter/releases>, `Inter-4.1.zip`, `extras/ttf/Inter-Bold.ttf`), SIL Open
Font License 1.1, licence text in `Inter-LICENSE.txt`. HarfBuzz shapes the text, so the font's kerning applies.
