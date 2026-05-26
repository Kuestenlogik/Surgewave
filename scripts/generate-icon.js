/**
 * Rendert scripts/icon.svg zu icon.png (128x128) im Repo-Root.
 * Diese Datei wird via Directory.Build.props in jedes NuGet-Package
 * eingebettet (<PackageIcon>icon.png</PackageIcon>).
 *
 * Usage:   node scripts/generate-icon.js
 * Output:  ./icon.png
 *
 * Bewusst auf 'sharp' (native libvips, ~5 MB) und nicht auf Playwright —
 * fuer ein SVG → PNG ohne HTML-/CSS-Layout ist ein headless Browser
 * (~350 MB Chromium) deutlich Overkill. Playwright bleibt nur fuer
 * scripts/generate-og-image.js (HTML-Template mit CSS-Layout).
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
