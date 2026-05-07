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
