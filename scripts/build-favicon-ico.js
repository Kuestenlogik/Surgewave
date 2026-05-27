/**
 * Build a multi-size favicon.ico from the rendered PNGs.
 *
 * Bundle: 16/32/48 (the de-facto Windows ICO standard since XP). Larger
 * sizes are served via the explicit `<link rel="icon" sizes="...">` tags
 * in default.html, so the ICO only needs to cover legacy browsers and
 * Windows pinning fallback.
 *
 * Source PNGs are produced by `node scripts/render-favicons.js` and must
 * exist before this script runs.
 */

const path = require('path');
const fs = require('fs');

const IMG_DIR = path.resolve(__dirname, '..', 'site', 'assets', 'images');
const SIZES = [16, 32, 48];
const OUT = path.join(IMG_DIR, 'favicon.ico');

(async () => {
    // png-to-ico v3+ is ESM-only; load via dynamic import from CommonJS shell.
    const { default: pngToIco } = await import('png-to-ico');

    const inputs = SIZES.map(s => path.join(IMG_DIR, `favicon-${s}.png`));
    for (const f of inputs) {
        if (!fs.existsSync(f)) {
            console.error(`missing input ${f} — run render-favicons.js first`);
            process.exit(1);
        }
    }
    const buf = await pngToIco(inputs);
    fs.writeFileSync(OUT, buf);
    console.log(`  multi-size .ico (${SIZES.join('/')}) -> ${path.relative(process.cwd(), OUT)}`);
})();
