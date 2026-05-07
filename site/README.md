# Surgewave marketing site

Jekyll-powered landing + marketing pages for the Surgewave project. Technical
docs live in `../docs/` under DocFX; this site sits at the root of
`https://kuestenlogik.github.io/Surgewave/` and links out to DocFX under `/docs/`.

## Local development

Prerequisites: Ruby (tested with 4.0). On Windows the toolchain lives at
`C:\Program Files\Ruby40-x64`.

```powershell
cd site
bundle install
bundle exec jekyll serve --baseurl ""
```

Then open <http://localhost:4000>. Pages auto-reload on change.

## Production build

The GitHub Actions workflow `.github/workflows/docs.yml` builds both this
site and the DocFX technical docs, combines them (site at root, DocFX at
`/docs/`), and publishes to GitHub Pages.

To build locally the same way CI does:

```powershell
cd site
bundle install
bundle exec jekyll build --baseurl "/Surgewave"
# Output lands in _site/.
```

## Layout

```
site/
├── _config.yml          # Jekyll config
├── Gemfile              # Ruby gems (Jekyll + SEO tag)
├── _layouts/
│   ├── default.html     # Full-width landing layout
│   └── page.html        # Standard content-body layout
├── _includes/           # Reusable section components
│   ├── header.html
│   ├── footer.html
│   ├── hero.html
│   ├── features.html
│   └── cta.html
├── index.html           # Landing page
├── features.html        # Deeper feature list
└── assets/
    └── css/style.css    # All styles, theme-aware
```

Add new pages as standalone `.html`/`.md` files at the site root, using
either the `page` layout (content pages) or `default` (hero layouts).
