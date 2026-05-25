/**
 * Builds surgewave-docs.pdf from the standalone DocFX HTML output.
 *
 * Why custom instead of `docfx pdf`:
 *   The DocFX 2.x pdf command bundles Microsoft.Playwright internally
 *   and auto-installs Chromium on first use. The auto-install path
 *   races on CI (network hiccups + concurrent runs), produces no
 *   useful diagnostic when it fails, and writes the output to a
 *   path that depends on globalMetadata fields not all builds set.
 *   This script does the same job with the @playwright/test browser
 *   we already use for screenshot capture, gives us deterministic
 *   ordering, and fails loudly when something is missing.
 *
 * Usage:
 *   node scripts/build-docs-pdf.js
 *
 * Inputs:
 *   artifacts/docs-standalone/  — HTML tree from `docfx docs/docfx.standalone.json`
 *   docs/_pdfcover/cover.html   — branded cover page (rendered separately and
 *                                 spliced in front of the merged content)
 *
 * Outputs:
 *   artifacts/pub/surgewave-docs.pdf
 */
const { chromium } = require('@playwright/test');
const fs = require('fs');
const path = require('path');
const { PDFDocument } = require('pdf-lib');

const ROOT = path.resolve(__dirname, '..');
const SRC = path.join(ROOT, 'artifacts', 'docs-standalone');
const COVER_HTML = path.join(ROOT, 'docs', '_pdfcover', 'cover.html');
const OUT_DIR = path.join(ROOT, 'artifacts', 'pub');
const OUT_PATH = path.join(OUT_DIR, 'surgewave-docs.pdf');

// Surgewave version + build-date the cover renders. Pulled from
// Directory.Build.props — same source the release pipeline reads,
// so the badge always reflects the floor of the build that
// rendered the PDF. Both fall through to placeholders when the
// build runs from a stale checkout.
function readBuildVersion() {
    try {
        const propsPath = path.join(ROOT, 'Directory.Build.props');
        const xml = fs.readFileSync(propsPath, 'utf-8');
        const match = /<Version>([^<]+)<\/Version>/.exec(xml);
        if (match && match[1]) {
            // Strip a "-dev" floor suffix — the published PDF carries
            // the released version, not the in-progress dev floor.
            return match[1].replace(/-dev$/, '');
        }
    } catch (err) {
        log(`could not read Directory.Build.props (${err.message}) — falling back to placeholder version`);
    }
    return 'unreleased';
}
const BUILD_VERSION = readBuildVersion();
const BUILD_DATE = new Date().toISOString().slice(0, 10);

// Top-level table of contents, hand-curated to mirror the section
// ordering below. Rendered as a standalone page after the cover so
// the PDF opens like a real book — cover, TOC, content. Sub-sections
// stay implicit; for a paginated TOC with page numbers we'd need a
// two-pass render (collect → renumber → emit) — the curated list is
// the cheaper 80%-good option.
const TOC_ENTRIES = [
    { title: 'Getting Started', file: 'quickstart/index.html' },
    { title: 'Architecture', file: 'architecture/index.html' },
    { title: 'Storage', file: 'storage/index.html' },
    { title: 'Transport', file: 'transport/index.html' },
    { title: 'Clustering', file: 'clustering/index.html' },
    { title: 'Clients', file: 'clients/index.html' },
    { title: 'Features', file: 'features/index.html' },
    { title: 'Connectors', file: 'connectors/index.html' },
    { title: 'AI & LLM', file: 'ai/index.html' },
    { title: 'Deployment', file: 'deployment/index.html' },
    { title: 'Operations', file: 'operations/index.html' },
    { title: 'Cookbook', file: 'cookbook/index.html' },
    { title: 'Reference', file: 'reference/index.html' },
    { title: 'API', file: 'api/index.html' },
];

function buildTocHtml() {
    const rows = TOC_ENTRIES.map(e =>
        `<li><a href="${e.file}">${escapeHtml(e.title)}</a></li>`
    ).join('\n        ');
    return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Surgewave Documentation — Table of Contents</title>
<style>
    body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        color: #0a0a0a;
        padding: 24mm 18mm;
        margin: 0;
    }
    h1 {
        font-size: 28pt;
        font-weight: 700;
        letter-spacing: -0.02em;
        margin: 0 0 0.5em;
    }
    .subtitle {
        color: #6b6b6b;
        font-size: 12pt;
        margin: 0 0 2em;
    }
    ol {
        list-style: none;
        counter-reset: toc;
        padding: 0;
        margin: 0;
    }
    ol li {
        counter-increment: toc;
        font-size: 14pt;
        padding: 10px 0;
        border-bottom: 1px solid #e3e3e3;
        display: flex;
        align-items: baseline;
        gap: 12px;
    }
    ol li::before {
        content: counter(toc, decimal-leading-zero);
        font-family: ui-monospace, "SF Mono", Menlo, monospace;
        font-size: 11pt;
        color: #999;
        min-width: 28px;
    }
    ol li a {
        color: #0a0a0a;
        text-decoration: none;
    }
</style>
</head>
<body>
    <h1>Table of Contents</h1>
    <p class="subtitle">Surgewave ${escapeHtml(BUILD_VERSION)} · ${BUILD_DATE}</p>
    <ol>
        ${rows}
    </ol>
</body>
</html>`;
}

function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

// Logical reading order. Pages outside this prefix list are appended at
// the end in alphabetical order so nothing gets dropped silently when a
// new section lands.
const SECTION_ORDER = [
    'index.html',
    'quickstart',
    'setup',
    'tutorials',
    'architecture',
    'storage',
    'transport',
    'clustering',
    'clients',
    'features',
    'connectors',
    'ai',
    'deployment',
    'operations',
    'cookbook',
    'reference',
    'conformance',
    'adr',
    'api',
];

function log(msg) { console.log(`[pdf] ${msg}`); }

function collectPages(rootDir) {
    const pages = [];
    walk(rootDir, '', pages);

    // Sort: section-order prefix first (in defined order), unknown
    // sections appended alphabetically. Within a section, alphabetical
    // by relative path.
    const orderIndex = (relPath) => {
        for (let i = 0; i < SECTION_ORDER.length; i++) {
            const seg = SECTION_ORDER[i];
            if (relPath === seg || relPath.startsWith(seg + path.sep)) return i;
        }
        return SECTION_ORDER.length; // unknown → goes last
    };

    pages.sort((a, b) => {
        const ai = orderIndex(a);
        const bi = orderIndex(b);
        if (ai !== bi) return ai - bi;
        return a.localeCompare(b);
    });

    return pages;
}

function walk(rootDir, relPath, list) {
    const absDir = path.join(rootDir, relPath);
    const entries = fs.readdirSync(absDir, { withFileTypes: true });
    for (const ent of entries) {
        const childRel = relPath ? path.join(relPath, ent.name) : ent.name;
        if (ent.isDirectory()) {
            // Skip DocFX internals and any pre-existing _pdf folder.
            if (ent.name.startsWith('_')) continue;
            walk(rootDir, childRel, list);
        } else if (ent.isFile()
                && ent.name.endsWith('.html')
                && ent.name !== 'toc.html'
                && ent.name !== '404.html') {
            list.push(childRel);
        }
    }
}

// Renders the standalone cover page (docs/_pdfcover/cover.html) with
// ${_version} / ${_date} placeholder substitution. Inlined into the
// PDF as the very first page so the document opens like a book.
async function renderCoverPdf(ctx) {
    if (!fs.existsSync(COVER_HTML)) {
        log(`cover template not found at ${COVER_HTML}; skipping cover page`);
        return null;
    }
    const tpl = fs.readFileSync(COVER_HTML, 'utf-8')
        .replace(/\{\{_version\}\}/g, BUILD_VERSION)
        .replace(/\{\{_date\}\}/g, BUILD_DATE);
    // Resolve relative <link rel="stylesheet" href="cover.css"/> by
    // setting a file:// base URL pointed at the cover folder.
    const baseUrl = 'file://' + COVER_HTML.split(path.sep).join('/');
    const page = await ctx.newPage();
    try {
        await page.goto(baseUrl, { waitUntil: 'load' });
        await page.setContent(tpl, { waitUntil: 'load', baseURL: baseUrl });
        const buf = await page.pdf({
            format: 'A4',
            printBackground: true,
            margin: { top: 0, bottom: 0, left: 0, right: 0 },
        });
        return buf;
    } finally {
        await page.close();
    }
}

async function main() {
    if (!fs.existsSync(SRC)) {
        throw new Error(`expected DocFX standalone output at ${SRC} — run \`docfx docs/docfx.standalone.json\` first`);
    }
    fs.mkdirSync(OUT_DIR, { recursive: true });

    const pages = collectPages(SRC);
    if (pages.length === 0) {
        throw new Error(`no HTML pages found under ${SRC}`);
    }
    log(`rendering ${pages.length} pages from ${SRC}`);

    const browser = await chromium.launch({ headless: true });
    try {
        const merged = await PDFDocument.create();
        const ctx = await browser.newContext({ viewport: { width: 1200, height: 900 } });

        // 1. Cover page from docs/_pdfcover/cover.html (branded, gradient,
        //    version + date). Goes in first so the PDF opens like a book.
        const coverBuf = await renderCoverPdf(ctx);
        if (coverBuf) {
            const coverDoc = await PDFDocument.load(coverBuf);
            const coverCopied = await merged.copyPages(coverDoc, coverDoc.getPageIndices());
            coverCopied.forEach(p => merged.addPage(p));
            log('  cover page rendered + added');
        }

        // 2. TOC page — curated section list, rendered through Playwright
        //    with setContent so it shares the same A4 / margin / font setup.
        const tocPage = await ctx.newPage();
        try {
            await tocPage.setContent(buildTocHtml(), { waitUntil: 'load' });
            const tocBuf = await tocPage.pdf({
                format: 'A4',
                printBackground: true,
                margin: { top: '18mm', bottom: '18mm', left: '14mm', right: '14mm' },
            });
            const tocDoc = await PDFDocument.load(tocBuf);
            const tocCopied = await merged.copyPages(tocDoc, tocDoc.getPageIndices());
            tocCopied.forEach(p => merged.addPage(p));
            log('  TOC page rendered + added');
        } finally {
            await tocPage.close();
        }

        // 3. DocFX content pages in section-order.
        let count = 0;
        for (const rel of pages) {
            const url = 'file://' + path.join(SRC, rel).split(path.sep).join('/');
            const page = await ctx.newPage();
            try {
                await page.goto(url, { waitUntil: 'networkidle', timeout: 30000 });
                // Force light theme for print — dark backgrounds waste
                // ink and most readers expect doc PDFs to render light.
                await page.evaluate(() => {
                    try { localStorage.setItem('theme', 'light'); } catch (_) {}
                    document.documentElement.setAttribute('data-theme', 'light');
                    document.documentElement.setAttribute('data-theme-resolved', 'light');
                });
                await page.reload({ waitUntil: 'networkidle', timeout: 30000 });
                const buf = await page.pdf({
                    format: 'A4',
                    printBackground: true,
                    margin: { top: '18mm', bottom: '18mm', left: '14mm', right: '14mm' },
                });
                const sub = await PDFDocument.load(buf);
                const copied = await merged.copyPages(sub, sub.getPageIndices());
                copied.forEach(p => merged.addPage(p));
                count++;
                if (count % 10 === 0) log(`  ${count}/${pages.length} merged`);
            } finally {
                await page.close();
            }
        }
        await ctx.close();

        const bytes = await merged.save();
        fs.writeFileSync(OUT_PATH, bytes);
        log(`wrote ${OUT_PATH} (${(bytes.length / 1024).toFixed(0)} KB, ${merged.getPageCount()} pages from ${count} content sources + cover + TOC)`);
    } finally {
        await browser.close();
    }
}

main().catch(err => {
    console.error(`[pdf] FAILED: ${err.message}`);
    if (err.stack) console.error(err.stack);
    process.exit(1);
});
