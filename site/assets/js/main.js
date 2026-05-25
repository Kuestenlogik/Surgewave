// ====================================================================
// Surgewave marketing site — central UX layer
//
// Three independent modules wrapped in IIFEs so a failure in one
// doesn't kill the others. Loaded with `defer` from default.html so
// every section of the page is parsed before we wire anything up.
//
//   1. Theme toggle (light / dark / auto) with View Transitions
//   2. Code-copy buttons on every <pre> in main content
//   3. Mobile-menu hamburger
//
// Surgewave-docs uses the same patterns embedded inline in
// docs/templates/surgewave-theme/layout/_master.tmpl, so site ↔ docs
// behaviour stays identical.
// ====================================================================

// ====================================================================
// Theme toggle (Light / Dark / Auto) with View Transitions
//
// The button cycles light → dark → auto. The icon swaps in real-time
// (sun / moon / half-circle). When the View Transitions API is
// available the swap rides a circular reveal anchored at the toggle
// button — the matching ::view-transition-new(root) keyframe lives
// in style.css. Browsers without the API (Firefox as of mid-2026)
// fall through to the instant swap.
//
// The localStorage key 'theme' is shared with the DocFX template so
// the choice carries across site ↔ docs without a re-toggle.
// ====================================================================
(function () {
    const THEME_KEY = 'theme';
    const CYCLE = ['light', 'dark', 'auto'];

    const ICONS = {
        light: '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>',
        dark:  '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>',
        auto:  '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M12 3 a 9 9 0 0 1 0 18 z" fill="currentColor" stroke="none"/></svg>'
    };

    function getTheme() {
        return localStorage.getItem(THEME_KEY) || 'auto';
    }

    function applyTheme(mode) {
        const resolved = (mode === 'auto')
            ? (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light')
            : mode;
        document.documentElement.setAttribute('data-theme', mode);
        document.documentElement.setAttribute('data-theme-resolved', resolved);
    }

    function updateButton(btn, mode) {
        btn.innerHTML = ICONS[mode] || ICONS.auto;
        const label = 'Theme: ' + mode + ' (click to cycle)';
        btn.setAttribute('title', label);
        btn.setAttribute('aria-label', label);
    }

    // Apply immediately so the page paints in the chosen theme — the
    // <html data-theme-…> attributes drive the CSS variable swap.
    applyTheme(getTheme());

    const btn = document.getElementById('theme-toggle');
    if (!btn) return;

    updateButton(btn, getTheme());

    btn.addEventListener('click', function (ev) {
        const next = CYCLE[(CYCLE.indexOf(getTheme()) + 1) % CYCLE.length];
        const commit = function () {
            localStorage.setItem(THEME_KEY, next);
            applyTheme(next);
            updateButton(btn, next);
        };
        // Anchor the circular reveal at the button centre — keyboard
        // (Enter/Space) activation reports clientX/Y = 0 which would
        // reveal from the top-left corner, looking weird.
        const rect = btn.getBoundingClientRect();
        const x = rect.left + rect.width / 2;
        const y = rect.top + rect.height / 2;
        document.documentElement.style.setProperty('--theme-transition-x', x + 'px');
        document.documentElement.style.setProperty('--theme-transition-y', y + 'px');

        if (!document.startViewTransition || ev.isTrusted === false) {
            commit();
            return;
        }
        document.startViewTransition(commit);
    });
})();

// ====================================================================
// Code-copy buttons — every <pre> in <main> gets a floating copy
// button at the top-right. Click copies the trimmed code text to the
// clipboard and flashes the button into a "Copied!" state for ~1.5 s.
//
// The button is appended to the <pre>'s parent rather than the <pre>
// itself so the absolute positioning anchors against the surrounding
// chrome (e.g. .install-snippet, .launch-step's pre wrapper). Falls
// back gracefully when navigator.clipboard isn't available.
// ====================================================================
(function () {
    const COPY_ICON =
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
          '<rect x="9" y="9" width="13" height="13" rx="2"/>' +
          '<path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>' +
        '</svg>';
    const CHECK_ICON =
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
          '<path d="M20 6 9 17l-5-5"/>' +
        '</svg>';

    function copyText(text, btn) {
        if (!navigator.clipboard) return;
        navigator.clipboard.writeText(text).then(function () {
            btn.classList.add('is-copied');
            btn.innerHTML = CHECK_ICON;
            setTimeout(function () {
                btn.classList.remove('is-copied');
                btn.innerHTML = COPY_ICON;
            }, 1500);
        });
    }

    const pres = document.querySelectorAll('main pre');
    pres.forEach(function (pre) {
        // Don't double-wrap if the markup already came with chrome.
        if (pre.parentElement && pre.parentElement.classList.contains('code-block')) return;
        // Skip <pre>s with no actual code content (e.g. ASCII layouts).
        const text = (pre.textContent || '').replace(/^\s+|\s+$/g, '');
        if (!text) return;

        // Wrap the <pre> in a relative-positioned container so the
        // copy button can absolute-anchor itself to the corner.
        const wrap = document.createElement('div');
        wrap.className = 'pre-copy-wrap';
        pre.parentNode.insertBefore(wrap, pre);
        wrap.appendChild(pre);

        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'pre-copy-btn';
        btn.setAttribute('aria-label', 'Copy snippet to clipboard');
        btn.setAttribute('title', 'Copy');
        btn.innerHTML = COPY_ICON;
        btn.addEventListener('click', function (e) {
            e.preventDefault();
            copyText(text, btn);
        });
        wrap.appendChild(btn);
    });
})();

// ====================================================================
// Mobile-menu hamburger — collapses the header nav into a full-screen
// drop panel on viewports < 720 px. The toggle button lives in
// .header-actions and is hidden on desktop via CSS. On click it
// toggles `body.mobile-menu-open` which the CSS uses to slide the
// nav panel in from the top and trap focus inside.
// ====================================================================
(function () {
    const toggle = document.getElementById('mobile-menu-toggle');
    if (!toggle) return;
    const nav = document.querySelector('.header-nav');
    if (!nav) return;

    const ICON_OPEN =
        '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
          '<line x1="3" y1="6" x2="21" y2="6"/>' +
          '<line x1="3" y1="12" x2="21" y2="12"/>' +
          '<line x1="3" y1="18" x2="21" y2="18"/>' +
        '</svg>';
    const ICON_CLOSE =
        '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
          '<line x1="6" y1="6" x2="18" y2="18"/>' +
          '<line x1="18" y1="6" x2="6" y2="18"/>' +
        '</svg>';
    toggle.innerHTML = ICON_OPEN;

    function setOpen(open) {
        document.body.classList.toggle('mobile-menu-open', open);
        toggle.setAttribute('aria-expanded', open ? 'true' : 'false');
        toggle.innerHTML = open ? ICON_CLOSE : ICON_OPEN;
    }

    toggle.addEventListener('click', function () {
        setOpen(!document.body.classList.contains('mobile-menu-open'));
    });

    // Tapping a nav link closes the menu so the section scroll lands
    // without the panel still covering the viewport.
    nav.addEventListener('click', function (e) {
        if (e.target.tagName === 'A') setOpen(false);
    });

    // Esc closes the menu — same idiom as the search overlay.
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape' && document.body.classList.contains('mobile-menu-open')) {
            setOpen(false);
        }
    });

    // Resizing past the breakpoint closes the menu — otherwise the
    // body stays in mobile-menu-open state on a desktop viewport.
    window.addEventListener('resize', function () {
        if (window.innerWidth >= 720 && document.body.classList.contains('mobile-menu-open')) {
            setOpen(false);
        }
    });
})();

// ====================================================================
// "See it in action" — Surgewave.Control screenshot carousel
//
// 1:1 port of the Bowire screenshots carousel: auto-drift, prev/next
// buttons, dot indicators, click-to-jump. Auto-drift is JS-driven (not
// a CSS keyframe) so pause / resume is seamless — the current scroll
// position stays put, the drift just picks up from wherever the reader
// left off.
//
// Infinite-loop is built from triple-cloning the card set
// ([prefix clones] + [originals] + [suffix clones]). The "safe zone"
// for scrollLeft is [setLength, 2*setLength]; if drift or manual scroll
// wanders into a clone set, scrollLeft is invisibly +/- setLength so
// the reader stays on visually identical content.
//
// Pause triggers:   hover, focus-within, pointer down (touch), dot click,
//                   prev/next button, arrow keys, manual scroll
// Resume behaviour: 5 s after the last interaction the drift starts again
// ====================================================================
(function () {
    const carousel = document.querySelector('.screenshot-carousel');
    const track    = document.querySelector('.screenshot-carousel-track');
    const dotsRow  = document.querySelector('.screenshot-dots');
    const prevBtn  = document.querySelector('.screenshot-nav-prev');
    const nextBtn  = document.querySelector('.screenshot-nav-next');
    if (!carousel || !track || !dotsRow) return;

    const cards = Array.from(track.querySelectorAll('.screenshot-card'));
    if (cards.length === 0) return;

    // ---- Infinite-loop: triple the card set so the reader always has
    //      cards to the left *and* the right of the focused one. Layout is
    //      [prefix clones] + [originals] + [suffix clones]. Auto-drift and
    //      manual scrolling stay inside the middle "safe zone"; once they
    //      wander into a clone set we invisibly subtract/add one set length
    //      so the reader stays on visually identical content. ----
    const originalCount = cards.length;

    function makeClone(card) {
        const clone = card.cloneNode(true);
        clone.setAttribute('aria-hidden', 'true');
        clone.setAttribute('tabindex', '-1');
        clone.classList.add('is-clone');
        return clone;
    }
    // Suffix clones — appended after originals.
    cards.forEach(card => track.appendChild(makeClone(card)));
    // Prefix clones — prepended in reverse so DOM order matches originals.
    for (let i = originalCount - 1; i >= 0; i--) {
        track.insertBefore(makeClone(cards[i]), track.firstChild);
    }

    // ---- Build the dot row (one dot per original card) ----
    cards.forEach((card, idx) => {
        const dot = document.createElement('button');
        dot.type = 'button';
        dot.className = 'screenshot-dot';
        dot.role = 'tab';
        const label = card.querySelector('h4')?.textContent?.trim() || `Screenshot ${idx + 1}`;
        dot.setAttribute('aria-label', label);
        dot.dataset.idx = String(idx);
        // Inner span carries the card title as a hover tooltip — mirrors
        // the section-nav dots on the right edge of the page.
        const labelSpan = document.createElement('span');
        labelSpan.className = 'screenshot-dot-label';
        labelSpan.textContent = label;
        dot.appendChild(labelSpan);
        dot.addEventListener('click', () => { scrollToCard(idx); nudgePauseTimer(); });
        dotsRow.appendChild(dot);
    });
    const dots = Array.from(dotsRow.querySelectorAll('.screenshot-dot'));

    // ---- Auto-drift controller ----
    const DRIFT_PX_PER_SEC    = 28;    // leisurely film-strip cadence
    const DRIFT_INTERVAL_MS   = 30;
    const RESUME_AFTER_MS     = 5000;  // inactivity before drift restarts

    const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    let pausedUntil = 0;
    // Sub-pixel accumulator — scrollLeft is snapped to an integer, so fractional
    // per-tick deltas would silently stall the drift.
    let driftAccum = 0;
    let lastTickAt = performance.now();

    function nudgePauseTimer() {
        pausedUntil = Date.now() + RESUME_AFTER_MS;
    }

    // Only hover pauses auto-scroll "live" — keyboard focus used to pause too,
    // but it made the drift feel broken in browsers that gave the focusable
    // carousel region implicit focus on load. Explicit interactions (clicks,
    // arrow keys, manual scroll) drive the 5s resume timer via nudgePauseTimer.
    function isHovered() {
        return carousel.matches(':hover');
    }

    // Length of one complete card set. Re-measured on every resize. The "safe
    // zone" for scrollLeft is [setLength, 2*setLength] — that's where the
    // originals live. If drift or manual scroll moves beyond either edge, we
    // subtract/add one setLength to snap back invisibly.
    let setLength = 0;
    function measureSetLength() {
        const firstOriginal = track.children[originalCount];            // idx N (after N prefix clones)
        const firstSuffix   = track.children[originalCount * 2];        // idx 2N
        if (firstOriginal && firstSuffix) {
            setLength = firstSuffix.offsetLeft - firstOriginal.offsetLeft;
        }
    }
    function initialScroll() {
        // Centre the SECOND original card — that way the first original card
        // sits on the left of the viewport (fully visible) and readers
        // immediately see two cards to the right, signalling "there's more".
        measureSetLength();
        const secondOriginal = track.children[originalCount + 1];
        if (!secondOriginal || setLength <= 0) return;
        const centreOffset = secondOriginal.offsetLeft + secondOriginal.offsetWidth / 2
                           - carousel.clientWidth / 2;
        markProgrammatic();
        carousel.scrollLeft = Math.max(0, centreOffset);
    }
    // Wait for the browser to finish layout before measuring / seeking —
    // offsetLeft can be stale or zero until after the first paint.
    requestAnimationFrame(() => {
        measureSetLength();
        initialScroll();
    });
    if (typeof ResizeObserver !== 'undefined') {
        new ResizeObserver(() => { measureSetLength(); }).observe(track);
    }
    window.addEventListener('load',   () => { measureSetLength(); initialScroll(); });
    window.addEventListener('resize', () => { measureSetLength(); });

    function driftTick() {
        const now = performance.now();
        const dtMs = now - lastTickAt;
        lastTickAt = now;

        if (reduceMotion) return;
        if (isHovered()) { nudgePauseTimer(); return; }
        if (Date.now() < pausedUntil) return;
        if (setLength <= 0) return;

        driftAccum += DRIFT_PX_PER_SEC * (dtMs / 1000);
        const whole = Math.floor(driftAccum);
        if (whole < 1) return;
        driftAccum -= whole;

        let next = carousel.scrollLeft + whole;
        // Infinite-loop wrap: keep scrollLeft inside the middle original set.
        // Viewport centre < setLength  → we're in the prefix zone  → +setLength
        // Viewport centre > 2*setLength → we're in the suffix zone → -setLength
        const viewportCentre = next + carousel.clientWidth / 2;
        if (viewportCentre >= 2 * setLength) next -= setLength;
        else if (viewportCentre <  setLength) next += setLength;

        markProgrammatic();
        carousel.scrollLeft = next;
    }
    setInterval(driftTick, DRIFT_INTERVAL_MS);

    // ---- Navigation helpers ----
    function scrollToCard(idx) {
        if (idx < 0) idx = 0;
        if (idx >= originalCount) idx = originalCount - 1;
        // Re-measure before every jump: the track width or set length
        // might have changed since the last resize/observer callback.
        measureSetLength();
        // Always centre the card from one of the three sets (prefix clone,
        // original, suffix clone). Pick whichever instance is closest to
        // the current scroll position so the smooth-scroll takes the shortest
        // path — critical with the 3-set infinite loop.
        const candidates = [
            track.children[idx],                     // prefix clone
            track.children[originalCount + idx],     // original
            track.children[originalCount * 2 + idx]  // suffix clone
        ].filter(Boolean);
        const carouselRect = carousel.getBoundingClientRect();
        let bestTarget = 0, bestDist = Infinity;
        candidates.forEach(card => {
            const cardRect = card.getBoundingClientRect();
            const delta = (cardRect.left - carouselRect.left)
                        - (carouselRect.width - cardRect.width) / 2;
            const t = Math.max(
                0,
                Math.min(carousel.scrollLeft + delta, track.scrollWidth - carousel.clientWidth)
            );
            const d = Math.abs(t - carousel.scrollLeft);
            if (d < bestDist) { bestDist = d; bestTarget = t; }
        });
        markProgrammatic();
        carousel.scrollTo({ left: bestTarget, behavior: 'smooth' });
    }

    function currentCardIndex() {
        // Active dot tracks the card whose centre is closest to the
        // carousel's centre. Walk *every* card slot (originals + clones)
        // so the dot stays in sync even when the reader has drifted into
        // cloned territory; then fold the result back into the 0..N-1
        // original range with a modulo.
        const carouselRect = carousel.getBoundingClientRect();
        const ref = carouselRect.left + carouselRect.width / 2;
        const allSlots = track.querySelectorAll('.screenshot-card');
        let bestSlot = 0;
        let bestDist = Infinity;
        allSlots.forEach((card, i) => {
            const r = card.getBoundingClientRect();
            const cardCentre = r.left + r.width / 2;
            const d = Math.abs(cardCentre - ref);
            if (d < bestDist) { bestDist = d; bestSlot = i; }
        });
        return bestSlot % originalCount;
    }

    function updateDots() {
        const idx = currentCardIndex();
        dots.forEach((d, i) => {
            const on = i === idx;
            d.classList.toggle('active', on);
            d.setAttribute('aria-selected', on ? 'true' : 'false');
        });
    }

    // ---- Scroll listener: keep dots in sync, detect manual scrolls ----
    // Smooth scrolling (scrollTo with behavior: 'smooth') fires many scroll
    // events over ~500ms; tracking "is this programmatic?" with a one-shot
    // flag resets on the first event and mis-classifies the rest as user
    // scrolling — which repeatedly nudged pausedUntil into the future and
    // left auto-drift stuck forever after any button/dot click. Switch to
    // a timestamp window: anything within 800ms of the last programmatic
    // scroll trigger is treated as programmatic.
    let programmaticUntil = 0;
    function markProgrammatic() { programmaticUntil = Date.now() + 800; }

    carousel.addEventListener('scroll', () => {
        updateDots();
        if (Date.now() >= programmaticUntil) {
            // Wheel / trackpad / touch-drag from the reader — give the auto
            // drift some breathing room before it picks up again.
            nudgePauseTimer();
        }
    }, { passive: true });

    prevBtn?.addEventListener('click', () => { scrollToCard(currentCardIndex() - 1); nudgePauseTimer(); });
    nextBtn?.addEventListener('click', () => { scrollToCard(currentCardIndex() + 1); nudgePauseTimer(); });

    carousel.addEventListener('keydown', (e) => {
        if (e.key === 'ArrowRight') { e.preventDefault(); scrollToCard(currentCardIndex() + 1); nudgePauseTimer(); }
        if (e.key === 'ArrowLeft')  { e.preventDefault(); scrollToCard(currentCardIndex() - 1); nudgePauseTimer(); }
    });

    updateDots();
})();

// ====================================================================
// Hero badges — live values
//
// Pulls NuGet (downloads + latest version of Kuestenlogik.Surgewave.Client)
// and optional GitHub-stars (currently parked behind a Liquid `if false`
// in hero.html until the repo is public and has enough stars to brag).
// Failures are silent — wenn die NuGet- oder GitHub-API nicht antwortet
// (z.B. weil das Package noch nicht published ist), bleibt das Value-
// Segment leer und die CSS-Regel ".hero-badge-value[data-badge-value]:empty"
// blendet es aus, sodass das Label allein stehen bleibt.
// Bowire-Pattern 1:1 portiert.
// ====================================================================
(function () {
    const badges = document.querySelectorAll('.hero-badge[data-badge]');
    if (badges.length === 0) return;

    function formatCount(n) {
        if (n == null || isNaN(n)) return null;
        if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace(/\.0$/, '') + 'M';
        if (n >= 1_000)     return (n / 1_000).toFixed(1).replace(/\.0$/, '') + 'k';
        return String(n);
    }

    function setValue(selector, value) {
        const el = document.querySelector(`.hero-badge[data-badge="${selector}"] [data-badge-value]`);
        if (el && value !== undefined && value !== null) {
            el.textContent = value;
            el.setAttribute('data-badge-loaded', 'true');
        }
    }

    // GitHub stargazers — unauthenticated, 60 req/h/IP. Browser cached.
    fetch('https://api.github.com/repos/Kuestenlogik/Surgewave', { headers: { 'Accept': 'application/vnd.github+json' } })
        .then(r => r.ok ? r.json() : null)
        .then(data => { if (data) setValue('gh-stars', formatCount(data.stargazers_count)); })
        .catch(() => {});

    // NuGet azuresearch — totalDownloads + version in one CORS-enabled call.
    fetch('https://azuresearch-usnc.nuget.org/query?q=packageid:Kuestenlogik.Surgewave.Client&prerelease=false&take=1')
        .then(r => r.ok ? r.json() : null)
        .then(data => {
            if (data && data.data && data.data[0]) {
                const pkg = data.data[0];
                setValue('nuget-downloads', formatCount(pkg.totalDownloads));
                setValue('nuget-version', 'v' + pkg.version);
            }
        })
        .catch(() => {});
})();
