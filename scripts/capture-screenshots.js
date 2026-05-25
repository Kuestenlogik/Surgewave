/**
 * Captures marketing screenshots from Surgewave.Control by driving the UI
 * with Playwright (analog Bowire's capture-screenshots.js).
 *
 * Prerequisites:
 *   1. Surgewave-Broker running (default: HTTPS :9093 + Kafka-wire :9092)
 *   2. Surgewave.Control running (default port 5050)
 *   3. (Optional) Demo data populated via scripts/seed-demo-data.ps1
 *
 * Better: use scripts/capture-pipeline.ps1 which orchestrates everything.
 *
 * Output: site/assets/images/screenshots/control-*-{dark,light}.png
 */
const { chromium } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const OUT = path.resolve(__dirname, '..', 'site', 'assets', 'images', 'screenshots');
const URL = process.env.CONTROL_URL || 'http://localhost:5050';
const THEME = (process.env.THEME || 'dark').toLowerCase();
const WIDTH = 1280;
const HEIGHT = 720;

fs.mkdirSync(OUT, { recursive: true });

function log(m) { console.log(new Date().toISOString().slice(11, 19), m); }

async function shot(page, name) {
    const file = path.join(OUT, `control-${name}-${THEME}.png`);
    await page.screenshot({ path: file, fullPage: false });
    if (THEME === 'dark') {
        fs.copyFileSync(file, path.join(OUT, `control-${name}.png`));
    }
    log(`  -> control-${name}-${THEME}.png`);
}

/** Navigate + wait for Blazor interactivity */
async function goTo(page, route) {
    log(`Navigate: ${route}`);
    await page.goto(`${URL}${route}`, { waitUntil: 'networkidle', timeout: 15000 });
    // Blazor server-side rendering hydration: short pause for layout-shift to settle
    await page.waitForTimeout(800);
}

(async () => {
    log(`Starting capture (theme=${THEME}, target=${URL})`);
    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
        viewport: { width: WIDTH, height: HEIGHT },
        colorScheme: THEME,
        ignoreHTTPSErrors: true,
    });
    const page = await context.newPage();
    await page.emulateMedia({ colorScheme: THEME });

    try {
        // Initial load — sets theme in localStorage so subsequent pages keep it
        await page.goto(URL, { waitUntil: 'domcontentloaded' });
        await page.evaluate(t => {
            try { localStorage.setItem('surgewave_theme_pref', t); } catch {}
        }, THEME);

        // === Dashboard (Home) — broker health + topic list + throughput sparklines
        await goTo(page, '/');
        await shot(page, 'dashboard');

        // === Topic List — partitions, retention, consumer-lag summary
        await goTo(page, '/topics');
        await shot(page, 'topic-detail');

        // === Consumer Groups — KIP-848 rebalance protocol viewer
        await goTo(page, '/consumer-groups');
        await shot(page, 'consumer-groups');

        // === Pipeline Editor — visual node graph for stream processing
        await goTo(page, '/pipelines');
        await shot(page, 'pipeline-editor');

        // === Schema Registry — Avro / JSON-Schema / Protobuf browser
        await goTo(page, '/schemas');
        await shot(page, 'schema-registry');

        // === Plugin Marketplace — signed .swpkg browse + install
        await goTo(page, '/plugins');
        await shot(page, 'plugin-marketplace');

        // === Connector Configuration — Available Plugins tab (zeigt installierte .swpkg's)
        await goTo(page, '/connectors');
        // Click "AVAILABLE PLUGINS" tab — Mat-Tab links sind anchor/button mit dem Tab-Label.
        // Fallback ist die existing "Running Connectors" Ansicht falls Tab nicht klickbar.
        const availableTab = page.locator('text=AVAILABLE PLUGINS').first();
        if (await availableTab.isVisible({ timeout: 1500 }).catch(() => false)) {
            await availableTab.click();
            await page.waitForTimeout(800);
        }
        await shot(page, 'connectors');

        // === Live Throughput Graph — per-topic bytes/sec, P50/P90/P99
        await goTo(page, '/metrics');
        await shot(page, 'throughput-graph');

        // === Cluster Topology — broker list, partition leaders, ISR
        await goTo(page, '/clusters');
        await shot(page, 'cluster-view');

        // === Surgewave.Fleet — multi-cluster geo-replication view
        await goTo(page, '/replication');
        await shot(page, 'fleet');

        // === Surgewave.Streams — pipeline lineage / DAG topology
        await goTo(page, '/pipelines/lineage');
        await shot(page, 'streams-topology');

        // === AI agents & RAG traces — agent list + step traces
        await goTo(page, '/agents');
        await shot(page, 'ai-agents');

        // === Identity, ACLs & RBAC — access-control editor
        await goTo(page, '/acls');
        await shot(page, 'acls');

        // === Audit log & compliance trail
        await goTo(page, '/audit');
        await shot(page, 'audit-log');

        log('Done.');
    } catch (e) {
        log(`ERROR: ${e.message}`);
        await page.screenshot({ path: path.join(OUT, `_debug-${THEME}.png`), fullPage: true });
        process.exitCode = 1;
    } finally {
        await context.close();
        await browser.close();
    }
})();
