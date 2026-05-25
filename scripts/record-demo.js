/**
 * Records a Surgewave.Control demo video by driving the UI with Playwright.
 *
 * Demo flow — 4 beats designed to show the workbench in ~30s:
 *   1) Dashboard arrival — broker-health, topic-list, throughput sparklines visible
 *   2) Topic drill-down — list -> detail with partitions + consumer-group lag
 *   3) Pipeline editor — drag-drop node graph (visual stream processing)
 *   4) Plugin marketplace — browse + verify a signed .swpkg
 *
 * Output: site/assets/videos/surgewave-demo-{dark,light}.webm
 */
const { chromium } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const URL = process.env.CONTROL_URL || 'http://localhost:5050';
const THEME = (process.env.THEME || 'dark').toLowerCase();
const OUTPUT_DIR = path.resolve(__dirname, '..', 'site', 'assets', 'videos');
const WIDTH = 1280;
const HEIGHT = 720;

function log(m) { console.log(new Date().toISOString().slice(11, 19), m); }

(async () => {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
        viewport: { width: WIDTH, height: HEIGHT },
        recordVideo: { dir: OUTPUT_DIR, size: { width: WIDTH, height: HEIGHT } },
        ignoreHTTPSErrors: true,
        colorScheme: THEME,
    });
    const page = await context.newPage();
    await page.emulateMedia({ colorScheme: THEME });

    page.on('console', msg => {
        if (msg.type() === 'error') log(`[browser ERR] ${msg.text()}`);
    });

    try {
        // ── Beat 1: Dashboard arrival ──────────────────────────────────────
        log(`Beat 1: Dashboard (theme=${THEME})`);
        await page.goto(URL, { waitUntil: 'networkidle', timeout: 30000 });
        await page.evaluate(t => {
            try { localStorage.setItem('surgewave_theme_pref', t); } catch {}
        }, THEME);
        await page.waitForTimeout(3000);  // dwell on dashboard — sparklines tick once

        // ── Beat 2: Topic drill-down ───────────────────────────────────────
        log('Beat 2: Topic list -> detail');
        await page.goto(`${URL}/topics`, { waitUntil: 'networkidle' });
        await page.waitForTimeout(2500);
        // If there's at least one topic-row, click it (drill into detail)
        const topicRow = page.locator('a[href^="/topics/"]:not([href*="/create"]):not([href*="/intent"])').first();
        if (await topicRow.isVisible({ timeout: 1500 }).catch(() => false)) {
            await topicRow.click();
            await page.waitForTimeout(2500);
        }

        // ── Beat 3: Pipeline editor ────────────────────────────────────────
        log('Beat 3: Pipeline editor');
        await page.goto(`${URL}/pipelines`, { waitUntil: 'networkidle' });
        await page.waitForTimeout(2000);
        // Try to click into "new pipeline" to show the editor canvas
        const newPipelineBtn = page.locator('a[href="/pipelines/new"]').first();
        if (await newPipelineBtn.isVisible({ timeout: 1500 }).catch(() => false)) {
            await newPipelineBtn.click();
            await page.waitForTimeout(2500);
        }

        // ── Beat 4: Plugin marketplace ────────────────────────────────────
        log('Beat 4: Plugin marketplace');
        await page.goto(`${URL}/plugins`, { waitUntil: 'networkidle' });
        await page.waitForTimeout(3000);  // dwell on plugin grid

        log('Demo flow complete.');
    } catch (e) {
        log(`ERROR: ${e.message}`);
        await page.screenshot({ path: path.join(OUTPUT_DIR, `_debug-${THEME}.png`) });
        process.exitCode = 1;
    } finally {
        await context.close();
        await browser.close();

        // Playwright writes video with a random hash; rename to predictable name.
        const files = fs.readdirSync(OUTPUT_DIR).filter(f => f.endsWith('.webm') && !f.startsWith('surgewave-demo-'));
        if (files.length > 0) {
            const newest = files
                .map(f => ({ f, mtime: fs.statSync(path.join(OUTPUT_DIR, f)).mtime }))
                .sort((a, b) => b.mtime - a.mtime)[0];
            const target = path.join(OUTPUT_DIR, `surgewave-demo-${THEME}.webm`);
            fs.renameSync(path.join(OUTPUT_DIR, newest.f), target);
            log(`Saved: ${target}`);
        }
    }
})();
