/**
 * Render Surgewave favicon PNGs in all standard sizes from mark_adaptive.svg.
 *
 * Uses Playwright (already a dev-dep for capture-pipeline) to rasterise the SVG
 * at each target size. The adaptive SVG carries `prefers-color-scheme: dark`
 * media queries — browsers (Chrome, Edge, Safari) pick the right palette
 * automatically for the SVG <link rel="icon">. PNG fallbacks are rendered in
 * light-mode colours (Wave #33bcff on transparent) because PWA-manifest icons
 * and apple-touch-icons cannot be adaptive.
 *
 * Sizes follow the union of the de facto standards:
 *   16, 32, 48        — legacy Windows + Browser-Tab
 *   64                — Windows desktop shortcut
 *   96                — Android home (mdpi)
 *   128               — Chrome Web Store, generic
 *   152, 167          — iPad
 *   180               — apple-touch (iPhone retina)
 *   192, 256, 512     — Android home + PWA-manifest + high-res
 *
 * Output: site/assets/images/favicon-<N>.png + apple-touch-icon.png (180).
 */

const { chromium } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const SVG_PATH = path.resolve(__dirname, '..', 'site', 'assets', 'images', 'mark_adaptive.svg');
const OUT_DIR = path.resolve(__dirname, '..', 'site', 'assets', 'images');
const SIZES = [16, 32, 48, 64, 96, 128, 152, 167, 180, 192, 256, 512];

(async () => {
    const svg = fs.readFileSync(SVG_PATH, 'utf8');

    const browser = await chromium.launch({ headless: true });
    const ctx = await browser.newContext({ deviceScaleFactor: 1 });

    for (const size of SIZES) {
        const page = await ctx.newPage();
        // Inline the SVG into a minimal HTML shell. The viewport matches the
        // target size so screenshot crops to exactly NxN. transparent
        // background keeps the icon usable on any chrome.
        const html = `<!doctype html><html><head><meta charset="utf-8"><style>
            html, body { margin: 0; padding: 0; background: transparent; }
            svg { display: block; width: ${size}px; height: ${size}px; }
        </style></head><body>${svg}</body></html>`;
        await page.setViewportSize({ width: size, height: size });
        await page.setContent(html, { waitUntil: 'load' });

        const buffer = await page.screenshot({ omitBackground: true });
        const file = path.join(OUT_DIR, `favicon-${size}.png`);
        fs.writeFileSync(file, buffer);
        console.log(`  ${size}x${size} -> ${path.relative(process.cwd(), file)}`);
        await page.close();
    }

    // apple-touch-icon.png is conventionally the 180x180 variant.
    fs.copyFileSync(path.join(OUT_DIR, 'favicon-180.png'), path.join(OUT_DIR, 'apple-touch-icon.png'));
    console.log(`  apple-touch-icon.png = favicon-180.png (alias)`);

    await ctx.close();
    await browser.close();
})();
