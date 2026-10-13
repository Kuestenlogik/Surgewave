/**
 * Renders scripts/icon.svg to icon.png (128x128) in the repo root.
 * This file is embedded into every NuGet package via Directory.Build.props
 * (<PackageIcon>icon.png</PackageIcon>).
 *
 * Usage:   node scripts/generate-icon.js
 * Output:  ./icon.png
 *
 * Deliberately built on 'sharp' (native libvips, ~5 MB) and not on Playwright —
 * for an SVG → PNG without HTML/CSS layout a headless browser
 * (~350 MB Chromium) is clearly overkill. Playwright stays only for
 * scripts/generate-og-image.js (HTML template with CSS layout).
 */
const sharp = require('sharp');
const path = require('path');
const fs = require('fs');

const SVG = path.resolve(__dirname, 'icon.svg');
const OUT = path.resolve(__dirname, '..', 'icon.png');

(async () => {
    if (!fs.existsSync(SVG)) {
        console.error(`SVG source not found: ${SVG}`);
        process.exit(1);
    }

    await sharp(SVG)
        .resize(128, 128)
        .png()
        .toFile(OUT);

    console.log(`wrote ${OUT}`);
})();
