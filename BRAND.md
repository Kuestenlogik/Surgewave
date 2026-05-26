# Surgewave brand guide

Quick reference for using the Surgewave name and mark in product UIs,
documentation, and partner integrations. Source files live in
[`images/`](images/), [`site/assets/images/`](site/assets/images/), and
[`scripts/`](scripts/).

---

## Wordmark

Use the literal word **Surgewave** as a wordmark — capitalised as shown,
one word, no hyphen, no inner caps (not "SurgeWave" or "surge wave").

| Context | Wording |
|---|---|
| Product name in prose | Surgewave |
| Package id / namespace | `Kuestenlogik.Surgewave.*` |
| Container repository / image name | `surgewave-broker`, `surgewave-control` |
| Tool / CLI command | `surgewave` |

---

## Primary mark (adaptive)

The canonical Surgewave mark — a stylised wave intersecting an upward
arc — is **theme-aware**: the wave fill stays Surgewave-blue across
light and dark backgrounds, while the ink colour switches between navy
(on light) and white (on dark). Use this variant **wherever the host
surface controls its own colour scheme** (web site, docs, IDE plugins,
in-app branding).

**Source:** [`images/mark_adaptive.svg`](images/mark_adaptive.svg)
**Inlineable (Liquid):** [`site/_includes/surgewave-mark.svg`](site/_includes/surgewave-mark.svg)
**ViewBox:** `0 0 77.026421 77.673187` (~square)

Behaviour:
- Light background → wave `#33bcff`, ink `#003e60` (navy)
- Dark background  → wave `#33bcff`, ink `#ffffff` (white)

Driven by either `prefers-color-scheme: dark` (auto) or the manual
theme toggle on the Surgewave site (via `data-theme-resolved="dark"`
on `<html>`).

---

## Alternative mark — Mark-on-Circle (NuGet / app-icon use)

A second packaging of the same mark on top of a filled navy circle,
designed for surfaces that can't rely on the host theme to control the
ink colour. **This is the variant in `icon.png`** used as the
`<PackageIcon>` for every Surgewave NuGet package (broker, client,
connectors, etc.).

**Source:** [`scripts/icon.svg`](scripts/icon.svg)
**Rendered output:** [`icon.png`](icon.png) (128×128)
**Render command:** `node scripts/generate-icon.js` (requires `sharp`,
see `package.json`)

Composition:
- Filled circle, fill `#003e60` (Surgewave navy), edge-to-edge in a
  100×100 viewBox
- Dark-variant of the adaptive mark inside: wave `#33bcff`, ink
  `#ffffff`
- **Optical centering**: the mark is offset roughly `(+3.4, -3.8)`
  from the geometric centre to put the *visual centre of mass*
  slightly above the geometric centre of the circle. The mark is
  asymmetric (small wave-tail top-right, larger C-body bottom-left),
  so naive bounding-box centering looks bottom-heavy. The 2-unit
  upward optical bias compensates for the well-known perceptual effect
  that round containers make mathematically centred content feel low.

When to use Mark-on-Circle:
- NuGet package icons (cannot inherit page theme)
- Container registry avatars (GHCR, Docker Hub)
- macOS / Windows app-icon-style placements (Homebrew tap art, MSI
  shortcut icons)
- Any surface where the surrounding background colour is unknown or
  outside our control

When **not** to use Mark-on-Circle:
- Anywhere the adaptive variant works — the navy disc is a stand-in
  for "we don't know your background"; if the host theme is known, use
  the transparent adaptive mark and let the host's colour scheme drive
  ink contrast.

---

## Colour palette

| Token | Hex | Usage |
|---|---|---|
| Surgewave Blue (Wave) | `#33bcff` | Wave element of the mark; primary accent across UI |
| Surgewave Navy (Ink) | `#003e60` | Ink element of the mark on light backgrounds; deep-background fill (Mark-on-Circle) |
| White (Ink, dark) | `#ffffff` | Ink element when the surrounding surface is dark (Mark-on-Circle, dark theme) |

---

## Things to avoid

- Don't recolour the wave element. The wave is `#33bcff` everywhere;
  it's the part that anchors brand recognition across both variants.
- Don't add a stroke or outline to the mark. The two-tone fills are
  the entire visual system.
- Don't drop the wave-tail to "simplify" the mark. The diagonal of the
  wave-tail and the C-body is the silhouette signature.
- Don't put the **adaptive** mark on a coloured background that isn't
  near-white or near-black; the ink colour switches on theme, not on
  contrast.
- Don't put the **Mark-on-Circle** on an already-dark background; the
  navy disc + dark surface kills the silhouette. That variant is for
  unknown / light surfaces where the disc itself supplies the dark
  field for the white ink.

---

## Regenerating the NuGet icon

The PNG is committed (`icon.png` in this repo and in
`Kuestenlogik/Surgewave.Connectors`) so consumers never need to render
it. To regenerate after a tweak to `scripts/icon.svg`:

```bash
npm install                          # picks up sharp from devDependencies
node scripts/generate-icon.js        # writes icon.png
node scripts/measure-icon.js         # sanity-check visual centre of mass
```

The `measure-icon.js` helper rasterises the SVG, finds every non-navy
pixel, and reports both the geometric bounding box and the
luminance-weighted centre of mass — so if the mark, scale, or disc
size change, you can verify (or recompute) the optical offset without
guessing.
