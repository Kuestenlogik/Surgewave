// A product's logo set, generated from the owner's one drawing of its mark. See README.md beside this file.
//
//   node scripts/branding/generate-logos.mjs [--config <brand.json>] [--contact-sheet <file.png>]
//
// Everything product-specific comes from the configuration (default: brand.json beside this script): the product
// name, the output prefix, the source drawing, the wordmark (its text and its outline), the colour roles, the light
// background and where the web icons are copied to. The script itself names no product, so another product runs it
// with its own brand.json.
//
// Sources (the only ones; everything in the images folder is derived and may be deleted and regenerated):
//   source             the mark, the owner's Inkscape drawing; read only, never rewritten
//   wordmark.outline   the wordmark text as an outline (outline-wordmark.py), so no font is needed
//
// Steps:
//   1. read the source; every shape's fill is read in Edge and mapped to a role; an unknown fill stops the run;
//   2. write the mark <prefix>_mark_{colored,black,white,adaptive}.svg, cleaned: no Inkscape/sodipodi metadata, no
//      unused defs or empty groups, role="img" and aria-label = the product; the lockups
//      <prefix>_lockup_{h,v}_{colored,black,white,adaptive}.svg; favicon.svg (the mark, cropped square, adaptive);
//      og-image.svg (the horizontal colored lockup centred on the light background, 1200×630);
//   3. render PNGs (1x, @2x) in Edge with a transparent background, favicon.ico (16/32/48), apple-touch-icon.png
//      (180, the mark on the light background) and og-image.png (1200×630);
//   4. delete every file in the images folder the run did not write (README.md stays);
//   5. copy the web icons to the folders the configuration names.
//
// Playwright comes from scripts/browser-checks (npm install there); BRANDING_PLAYWRIGHT_FROM may name another
// package.json to resolve it from. Edge is the browser (channel msedge).
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const arg = (name) => (args.includes(name) ? path.resolve(args[args.indexOf(name) + 1]) : null);
const configFile = arg('--config') ?? path.join(here, 'brand.json');
const contactSheet = arg('--contact-sheet');

const config = JSON.parse(fs.readFileSync(configFile, 'utf8'));
for (const key of ['product', 'prefix', 'source', 'wordmark', 'images', 'roles', 'wordmarkRole', 'background']) {
  if (config[key] === undefined) throw new Error(`${configFile}: "${key}" is missing`);
}
const root = path.resolve(path.dirname(configFile), config.root ?? '.');
const images = path.join(root, config.images);
const source = path.join(root, config.source);
const label = config.product;
const prefix = config.prefix;
const require = createRequire(process.env.BRANDING_PLAYWRIGHT_FROM ?? path.join(root, 'scripts', 'browser-checks', 'package.json'));
const { chromium } = require('playwright');

// Palette. A role per colour of the drawing (source: the fills that mean it); the adaptive files swap each role's
// light colour for its dark one.
const roles = Object.fromEntries(Object.entries(config.roles).map(([role, c]) =>
  [role, { light: c.light, dark: c.dark, source: c.source.map((v) => v.toLowerCase()) }]));
if (!roles[config.wordmarkRole]) throw new Error(`${configFile}: wordmarkRole "${config.wordmarkRole}" is no role`);
const flat = { black: '#000000', white: '#ffffff' };
// The light background behind the touch icon and the Open Graph image: a radial gradient over these stops.
const glass = config.background.stops;

const wordmark = JSON.parse(fs.readFileSync(path.join(root, config.wordmark.outline), 'utf8'));
if (wordmark.text !== config.wordmark.text) {
  throw new Error(`${config.wordmark.outline} outlines "${wordmark.text}", the configuration names "${config.wordmark.text}": run outline-wordmark.py --text`);
}

// Lockup geometry as Surgewave's set: horizontal 1100×200 (mark 180 at 10,10; text from x 220, 100 px),
// vertical 480×380 (mark 240 at 120,20; text centred at 240, 70 px, baseline near 340).
const lockups = {
  h: { w: 1100, h: 200, mark: { x: 10, y: 10, size: 180 }, text: { x: 220, centreY: 100, size: 100, anchor: 'start' }, label: `${label} logo horizontal` },
  v: { w: 480, h: 380, mark: { x: 120, y: 20, size: 240 }, text: { x: 240, baseline: 340, size: 70, anchor: 'middle' }, label: `${label} logo vertical` },
};
const markPng = 512;
// The Open Graph image: 1200×630, the horizontal lockup's ink 840 px wide (at most 240 tall) in its middle, so it
// reads in a link preview's thumbnail.
const og = { w: 1200, h: 630, inkWidth: 840, inkHeight: 240 };

const write = (file, text) => { fs.writeFileSync(file, text.replace(/\r\n/g, '\n')); console.log('  wrote', path.relative(root, file)); };

// 1. Inkscape's editing state (the namedview, inkscape:/sodipodi: attributes and their namespaces) stays in the
//    source and is left out of every output. Paths, transforms and styles are taken as drawn.
const editorless = (text) => text
  .replace(/\s*<sodipodi:namedview[\s\S]*?<\/sodipodi:namedview>/g, '')
  .replace(/\s*<sodipodi:namedview[^>]*\/>/g, '')
  .replace(/\s+(?:inkscape|sodipodi):[\w-]+="[^"]*"/g, '')
  .replace(/\s+xmlns:(?:inkscape|sodipodi|svg)="[^"]*"/g, '');

// The drawing's body with a role class on every filled shape, read in the browser so the effective fill
// (attribute, style, inherited from a group) is what counts.
async function readBody(page, svgText) {
  await page.setContent(`<!doctype html><body style="margin:0">${editorless(svgText).replace(/<\?xml[^>]*>/, '')}</body>`);
  return page.evaluate((roleSources) => {
    const svg = document.querySelector('svg');
    const hex = (c) => {
      const m = c.match(/\d+(\.\d+)?/g);
      if (!m || c === 'none') return c;
      return '#' + m.slice(0, 3).map((v) => (+v).toString(16).padStart(2, '0')).join('');
    };
    const unknown = [];
    for (const el of svg.querySelectorAll('path, circle, ellipse, rect, polygon, polyline, line')) {
      if (el.closest('defs, clipPath, mask, symbol')) continue;
      const fill = hex(getComputedStyle(el).fill);
      const role = Object.entries(roleSources).find(([, s]) => s.includes(fill))?.[0];
      if (!role) { unknown.push(`${el.id || el.tagName}: ${fill}`); continue; }
      el.setAttribute('data-role', role);
    }
    if (unknown.length) return { error: 'fills without a role: ' + unknown.join(', ') };
    // Strip fills (they come back per variant), ids (nothing refers to them; inlined twice they would clash),
    // and style properties that only matter to the stroke-less fill.
    // Inkscape leaves empty groups behind; they draw nothing.
    for (const g of [...svg.querySelectorAll('g')].reverse()) if (!g.closest('defs') && !g.children.length) g.remove();
    for (const el of svg.querySelectorAll('*')) {
      if (el.closest('defs')) continue;
      el.removeAttribute('fill');
      el.removeAttribute('id');
      const style = el.getAttribute('style');
      if (style !== null) {
        const kept = style.split(';').map((s) => s.trim()).filter((s) => s && !/^fill(-opacity)?\s*:/.test(s));
        if (kept.length) el.setAttribute('style', kept.join(';')); else el.removeAttribute('style');
      }
    }
    const parts = [...svg.children].filter((c) => c.tagName !== 'defs').map((c) => new XMLSerializer().serializeToString(c));
    const bb = svg.getBBox();
    return { body: parts.join('\n').replace(/ xmlns="http:\/\/www\.w3\.org\/2000\/svg"/g, ''), viewBox: svg.getAttribute('viewBox'), bbox: [bb.x, bb.y, bb.width, bb.height] };
  }, Object.fromEntries(Object.entries(roles).map(([k, v]) => [k, v.source])));
}

// 2. Variants.
const paint = (body, variant) => body.replace(/data-role="(\w+)"/g, (_, role) =>
  variant === 'adaptive' ? `class="${prefix}-${role}"` : `fill="${flat[variant] ?? roles[role].light}"`);

// Theme-aware CSS: the system's scheme, and the page's own toggle where the SVG is inlined
// (data-theme-resolved as some products' pages set it, data-theme as others do).
function adaptiveStyle() {
  const rule = (scheme, scope = '') => Object.entries(roles)
    .map(([role, c]) => `${scope}.${prefix}-${role} { fill: ${c[scheme]}; }`).join('\n    ');
  const toggled = (scheme) => Object.entries(roles).map(([role, c]) =>
    `:root[data-theme-resolved="${scheme}"] .${prefix}-${role}, [data-theme-resolved="${scheme}"] .${prefix}-${role},\n    :root[data-theme="${scheme}"] .${prefix}-${role}, [data-theme="${scheme}"] .${prefix}-${role} { fill: ${c[scheme]}; }`).join('\n    ');
  return `<style>
    ${rule('light')}
    @media (prefers-color-scheme: dark) {
      ${rule('dark').replace(/\n {4}/g, '\n      ')}
    }
    /* The page's own theme toggle; only effective when the SVG is inlined. */
    ${toggled('dark')}
    ${toggled('light')}
  </style>`;
}

const svgDoc = (viewBox, label, inner, style = '') =>
  `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="${viewBox}" role="img" aria-label="${label}">
${style ? `  <defs>${style}</defs>\n` : ''}${inner}
</svg>
`;

function lockup(kind, variant, mark) {
  const L = lockups[kind];
  const s = L.text.size / wordmark.unitsPerEm;
  const [x0, , x1] = wordmark.bbox;
  const inkWidth = (x1 - x0) * s;
  // Horizontal: the cap height centred on the mark's centre line. Vertical: on Surgewave's baseline.
  const baseline = L.text.baseline ?? L.text.centreY + (wordmark.capHeight * s) / 2;
  const tx = L.text.anchor === 'middle' ? L.text.x - inkWidth / 2 - x0 * s : L.text.x - x0 * s;
  const textPaint = variant === 'adaptive' ? `class="${prefix}-${config.wordmarkRole}"` : `fill="${flat[variant] ?? roles[config.wordmarkRole].light}"`;
  const inner = `  <svg x="${L.mark.x}" y="${L.mark.y}" width="${L.mark.size}" height="${L.mark.size}" viewBox="${square(mark, 1).map((v) => +v.toFixed(3)).join(' ')}">
${paint(mark.body, variant)}
  </svg>
  <path ${textPaint} transform="translate(${tx.toFixed(2)} ${baseline.toFixed(2)}) scale(${s.toFixed(6)})" d="${wordmark.d}" />`;
  return svgDoc(`0 0 ${L.w} ${L.h}`, L.label, inner, variant === 'adaptive' ? adaptiveStyle() : '');
}

// A square around the drawing's ink, `air` times its larger side.
function square(mark, air) {
  const [x, y, w, h] = mark.bbox;
  const side = Math.max(w, h) * air;
  return [x + w / 2 - side / 2, y + h / 2 - side / 2, side, side];
}

// The favicon: the mark in a square around its drawing, a little air on every side.
function favicon(mark) {
  const vb = square(mark, 1.06).map((v) => +v.toFixed(3)).join(' ');
  return svgDoc(vb, label, paint(mark.body, 'adaptive'), adaptiveStyle());
}

// The apple-touch-icon: the mark on the light glass, square, no transparency (iOS would fill it black).
function touchIcon(mark) {
  const [ox, oy, side] = square(mark, 1.4);
  return svgDoc(`${ox.toFixed(3)} ${oy.toFixed(3)} ${side.toFixed(3)} ${side.toFixed(3)}`, label, `  <rect x="${ox.toFixed(3)}" y="${oy.toFixed(3)}" width="${side.toFixed(3)}" height="${side.toFixed(3)}" fill="url(#glass)" />
${paint(mark.body, 'colored')}`, glassGradient());
}

function glassGradient() {
  const offsets = glass.map((_, i) => (i === 0 ? 0 : i === glass.length - 1 ? 1 : +(0.7 * i / (glass.length - 2)).toFixed(3)));
  return `<radialGradient id="glass" cx="0.3" cy="0.25" r="0.95">${glass.map((c, i) => `<stop offset="${offsets[i]}" stop-color="${c}" />`).join('')}</radialGradient>`;
}

// The Open Graph image: the horizontal colored lockup centred on the light background, opaque.
function ogImage(lockupH, ink) {
  const [ix, iy, iw, ih] = ink;
  const k = Math.min(og.inkWidth / iw, og.inkHeight / ih);
  const w = iw * k, h = ih * k;
  const f = (v) => +v.toFixed(3);
  const inner = lockupH.replace(/<\?xml[^>]*>\s*/, '')
    .replace(/<svg [^>]*>/, `<svg x="${f((og.w - w) / 2)}" y="${f((og.h - h) / 2)}" width="${f(w)}" height="${f(h)}" viewBox="${[ix, iy, iw, ih].map(f).join(' ')}">`)
    .trimEnd();
  return svgDoc(`0 0 ${og.w} ${og.h}`, label, `  <rect width="${og.w}" height="${og.h}" fill="url(#glass)" />
${inner}`, glassGradient());
}

// The ink of a drawing (its bounding box in its own units), measured in the browser.
async function inkOf(page, svgText) {
  await page.setContent(`<!doctype html><body style="margin:0">${svgText.replace(/<\?xml[^>]*>/, '')}</body>`);
  return page.evaluate(() => { const b = document.querySelector('svg').getBBox(); return [b.x, b.y, b.width, b.height]; });
}

// 3. Rendering. The SVG fills a viewport of the target's CSS size; @2x doubles the device pixels.
async function render(browser, svgText, w, h, scale, out, colorScheme = 'light') {
  const ctx = await browser.newContext({ viewport: { width: w, height: h }, deviceScaleFactor: scale, colorScheme });
  const page = await ctx.newPage();
  const sized = svgText.replace(/<\?xml[^>]*>/, '').replace('<svg ', `<svg width="${w}" height="${h}" style="display:block" `);
  await page.setContent(`<!doctype html><html><body style="margin:0;background:transparent">${sized}</body></html>`);
  const png = await page.screenshot({ omitBackground: true });
  await ctx.close();
  if (out) { fs.writeFileSync(out, png); console.log('  wrote', path.relative(root, out)); }
  return png;
}

// An ICO with PNG entries (supported since Windows Vista and by every browser).
function ico(pngs) {
  const header = Buffer.alloc(6 + 16 * pngs.length);
  header.writeUInt16LE(0, 0); header.writeUInt16LE(1, 2); header.writeUInt16LE(pngs.length, 4);
  let offset = header.length;
  pngs.forEach(({ size, data }, i) => {
    const e = 6 + 16 * i;
    header.writeUInt8(size >= 256 ? 0 : size, e); header.writeUInt8(size >= 256 ? 0 : size, e + 1);
    header.writeUInt8(0, e + 2); header.writeUInt8(0, e + 3);
    header.writeUInt16LE(1, e + 4); header.writeUInt16LE(32, e + 6);
    header.writeUInt32LE(data.length, e + 8); header.writeUInt32LE(offset, e + 12);
    offset += data.length;
  });
  return Buffer.concat([header, ...pngs.map((p) => p.data)]);
}

// ---------------------------------------------------------------------------------------------------------------
const browser = await chromium.launch({ channel: 'msedge' });
const written = new Set(['README.md']);
const out = (name) => { written.add(name); return path.join(images, name); };
try {
  const page = await browser.newPage();
  console.log('==> source', path.relative(root, source));
  const mark = await readBody(page, fs.readFileSync(source, 'utf8'));
  if (mark.error) throw new Error(`${path.basename(source)}: ${mark.error}`);

  const pngJobs = [];
  console.log('==> mark');
  for (const variant of ['colored', 'black', 'white', 'adaptive']) {
    const text = svgDoc(mark.viewBox, label, paint(mark.body, variant), variant === 'adaptive' ? adaptiveStyle() : '');
    write(out(`${prefix}_mark_${variant}.svg`), text);
    if (variant !== 'adaptive') pngJobs.push({ svg: text, name: `${prefix}_mark_${variant}`, w: markPng, h: markPng });
  }

  console.log('==> lockups');
  let lockupH = null;
  for (const kind of ['h', 'v']) {
    for (const variant of ['colored', 'black', 'white', 'adaptive']) {
      const text = lockup(kind, variant, mark);
      write(out(`${prefix}_lockup_${kind}_${variant}.svg`), text);
      if (kind === 'h' && variant === 'colored') lockupH = text;
      if (variant !== 'adaptive') pngJobs.push({ svg: text, name: `${prefix}_lockup_${kind}_${variant}`, w: lockups[kind].w, h: lockups[kind].h });
    }
  }

  console.log('==> PNGs');
  for (const j of pngJobs) {
    await render(browser, j.svg, j.w, j.h, 1, out(`${j.name}.png`));
    await render(browser, j.svg, j.w, j.h, 2, out(`${j.name}@2x.png`));
  }

  console.log('==> favicon');
  const fav = favicon(mark);
  write(out('favicon.svg'), fav);
  const icoPngs = [];
  for (const size of [16, 32, 48]) icoPngs.push({ size, data: await render(browser, fav, size, size, 1, null) });
  fs.writeFileSync(out('favicon.ico'), ico(icoPngs)); console.log('  wrote', path.join(config.images, 'favicon.ico'));
  const touch = touchIcon(mark);
  await render(browser, touch, 180, 180, 1, out('apple-touch-icon.png'));

  console.log('==> Open Graph image');
  const ogSvg = ogImage(lockupH, await inkOf(page, lockupH));
  write(out('og-image.svg'), ogSvg);
  await render(browser, ogSvg, og.w, og.h, 1, out('og-image.png'));

  console.log('==> clean', config.images);
  for (const f of fs.readdirSync(images)) {
    if (written.has(f)) continue;
    fs.rmSync(path.join(images, f), { recursive: true });
    console.log('  deleted', path.join(config.images, f));
  }

  console.log('==> distribute');
  for (const { dir, files } of config.distribute ?? []) {
    for (const f of files) {
      if (!written.has(f)) throw new Error(`distribute: ${f} is no output of this run`);
      fs.copyFileSync(path.join(images, f), path.join(root, dir, f));
      console.log('  copied', `${dir}/${f}`);
    }
  }

  if (contactSheet) {
    console.log('==> contact sheet');
    // Every output at 16, 32, 64, 180 and 512 px tall (the horizontal lockup up to 180: at 512 it is 2816 px wide),
    // on white and on #121212: the SVGs, favicon.ico and the touch icon (each other PNG is a render of the SVG of
    // its name and shows the same picture).
    const files = fs.readdirSync(images).filter((f) => /\.svg$/.test(f) || ['favicon.ico', 'apple-touch-icon.png', 'og-image.png'].includes(f)).sort();
    const sizes = [16, 32, 64, 180, 512];
    const row = (f, dark) => {
      const wide = f.includes('lockup_h') || f.startsWith('og-image');
      const imgs = sizes.filter((s) => !(wide && s > 180)).map((s) => `<img src="${f}" style="height:${s}px">`).join('');
      return `<div class="row" style="background:${dark ? '#121212' : '#ffffff'}"><code style="color:${dark ? '#ddd' : '#333'}">${f}</code>${imgs}</div>`;
    };
    const head = `<!doctype html><html><head><meta charset="utf-8"><style>
      body { margin: 24px; font: 13px Segoe UI, sans-serif; background: #8a96a3; }
      h2 { margin: 24px 0 8px; color: #fff; }
      .row { display: flex; align-items: flex-end; gap: 16px; padding: 10px 12px; margin-bottom: 6px; border-radius: 6px; width: max-content; }
      .row code { width: 270px; flex: none; align-self: center; font-family: Consolas, monospace; font-size: 12px; }
    </style></head><body>`;
    // An SVG in <img> follows the embedding page's colour scheme: the light half is rendered in light mode, the
    // dark half in dark mode, so the adaptive files show both faces; the halves stand side by side in one image.
    const halves = [];
    for (const dark of [false, true]) {
      const ctx = await browser.newContext({ viewport: { width: 1600, height: 400 }, colorScheme: dark ? 'dark' : 'light' });
      const p = await ctx.newPage();
      const title = dark ? 'Dark background (#121212), the page in dark mode' : 'Light background (#ffffff)';
      const part = head + `<h2>${title}: 16, 32, 64, 180, 512 px</h2>${files.map((f) => row(f, dark)).join('')}</body>`;
      const tmp = path.join(images, `__contact-${dark ? 'dark' : 'light'}.html`);
      fs.writeFileSync(tmp, part);
      await p.goto('file:///' + tmp.replace(/\\/g, '/'));
      await p.waitForLoadState('networkidle');
      halves.push({ file: tmp, png: await p.screenshot({ fullPage: true }) });
      await ctx.close();
    }
    // The halves side by side in one image.
    const ctx = await browser.newContext({ viewport: { width: halves.reduce((w, h) => w + h.png.readUInt32BE(16), 0), height: 400 } });
    const p = await ctx.newPage();
    await p.setContent(`<body style="margin:0;background:#8a96a3;white-space:nowrap;font-size:0">${halves.map((h) => `<img style="display:inline-block;vertical-align:top" src="data:image/png;base64,${h.png.toString('base64')}">`).join('')}</body>`);
    await p.waitForLoadState('load');
    fs.mkdirSync(path.dirname(contactSheet), { recursive: true });
    fs.writeFileSync(contactSheet, await p.screenshot({ fullPage: true }));
    await ctx.close();
    for (const h of halves) fs.rmSync(h.file);
    console.log('  wrote', contactSheet);
  }
} finally {
  await browser.close();
}
