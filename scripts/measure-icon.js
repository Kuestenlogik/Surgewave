// Visueller Schwerpunkt der nicht-Kreis-Pixel (also nur das Mark) in icon.svg.
// Detection: alle Pixel die NICHT Navy (#003e60) sind.
const sharp = require('sharp');
const path = require('path');
const fs = require('fs');

const SVG = fs.readFileSync(path.resolve(__dirname, 'icon.svg'), 'utf8');

(async () => {
    const SIZE = 1024;
    const { data, info } = await sharp(Buffer.from(SVG))
        .resize(SIZE, SIZE)
        .raw()
        .ensureAlpha()
        .toBuffer({ resolveWithObject: true });

    // Navy = #003e60 = (0, 62, 96). Anything else = Mark pixel.
    const NAVY_R = 0, NAVY_G = 62, NAVY_B = 96;
    const isMarkPixel = (r, g, b, a) => {
        if (a < 128) return false;
        // Far enough from navy to count as mark colour
        const dr = r - NAVY_R, dg = g - NAVY_G, db = b - NAVY_B;
        return (dr * dr + dg * dg + db * db) > 1000;
    };

    let minX = SIZE, minY = SIZE, maxX = -1, maxY = -1;
    let sumX = 0, sumY = 0, count = 0;
    for (let y = 0; y < info.height; y++) {
        for (let x = 0; x < info.width; x++) {
            const i = (y * info.width + x) * 4;
            const r = data[i], g = data[i + 1], b = data[i + 2], a = data[i + 3];
            if (isMarkPixel(r, g, b, a)) {
                if (x < minX) minX = x;
                if (y < minY) minY = y;
                if (x > maxX) maxX = x;
                if (y > maxY) maxY = y;
                sumX += x;
                sumY += y;
                count++;
            }
        }
    }
    const u = (px) => (px / SIZE) * 100;
    const bx0 = u(minX), by0 = u(minY), bx1 = u(maxX + 1), by1 = u(maxY + 1);
    const bcx = bx0 + (bx1 - bx0) / 2, bcy = by0 + (by1 - by0) / 2;
    const cmx = u(sumX / count), cmy = u(sumY / count);
    console.log(`Mark pixels: ${count}`);
    console.log(`Mark BBox        : x ${bx0.toFixed(2)}..${bx1.toFixed(2)}  y ${by0.toFixed(2)}..${by1.toFixed(2)}`);
    console.log(`Mark BBox center : (${bcx.toFixed(2)}, ${bcy.toFixed(2)})`);
    console.log(`Mark visual CoM  : (${cmx.toFixed(2)}, ${cmy.toFixed(2)})`);
    console.log(`Circle center    : (50, 50)`);
    console.log('');
    console.log(`Shift for visual CoM == circle center:`);
    console.log(`  dx = ${(50 - cmx).toFixed(2)}, dy = ${(50 - cmy).toFixed(2)}`);
    console.log(`Shift for BBox center == circle center:`);
    console.log(`  dx = ${(50 - bcx).toFixed(2)}, dy = ${(50 - bcy).toFixed(2)}`);
})();
