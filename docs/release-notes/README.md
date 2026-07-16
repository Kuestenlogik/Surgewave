# Release notes — pre-tag editorial body

This directory holds the **editorial body** of each upcoming release
*before* the version is tagged and published. The convention is
file-driven (ported from Bowire) so we never ship a release whose body
is just installation boilerplate — the curated prose is the release
description; the install commands and the auto-generated commit list
follow below it.

## Convention

- **`upcoming.md`** — body of the next unreleased version. Grows
  alongside the work that lands in `main` (PRs / commits add their
  headlines as they go). Always carries front-matter with the
  intended `title:` and (optional) `version:`. Required for the
  release pipeline — the `Verify curated release notes` job gates
  on it before any build minute is spent.
- **`vX.Y.Z.md`** — preserved body of a published release. Created
  by renaming `upcoming.md` at release time so the historical text is
  in git, not just in the GitHub Release.
- **`_template.md`** — skeleton for a fresh `upcoming.md`.

## Tag-time flow

1. **Before the tag.** Make sure `upcoming.md` is curated — every
   real highlight has a `###` section with 2-4 sentences of prose,
   breaking changes are listed under their own section, and the
   front-matter `title:` reads well as the release-title theme.
   The release title itself comes from the matching **milestone
   title** (`vX.Y — <theme>` convention), so keep the two in sync.
2. **Push the tag.** `release.yml` verifies a curated body exists
   (`docs/release-notes/v<tag>.md` → `upcoming.md`, placeholder
   detection = at least one `###` section), assembles
   `release-body.md` (curated body + collapsed Installation block +
   seam), resolves the title from the milestone, and publishes with
   the auto-generated commit list appended below the seam.
3. **After the publish.** Open a small PR that:
   - Renames `upcoming.md` → `v<tag>.md` (preserves the body in git).
   - Recreates `upcoming.md` from `_template.md` for the next round.
   - Re-runs `node scripts/ci/generate-release-notes.mjs` to refresh
     the `RELEASE_NOTES.md` mirror.

## Front-matter

```
---
title: Production hardening & trustworthy admin
version: 0.4.0
---
```

- `title` — the theme half of the release title
  (`v<version> — <title>`). Mirrors the milestone-title convention;
  the pipeline resolves the published title from the milestone.
- `version` — optional sanity check against the tag.

Body follows after a blank line. Sections expected:
- **Highlights** — what's new, written for users, one `###` per theme.
- **Fixes** — notable bug fixes, grouped.
- **Breaking changes** — wire / API / package shifts with migration paths.
- **Acknowledgements** — short, optional.

## Related pieces

- `RELEASE_NOTES.md` (repo root) is a **generated mirror** of the
  published GitHub Releases — do not edit it by hand; run
  `node scripts/ci/generate-release-notes.mjs` to refresh.
- The `Draft release notes` workflow (`draft-release-notes.yml`)
  pre-fills `upcoming.md` from the milestone's closed issues + the
  commit log when you want a head start; it refuses to overwrite a
  file that already carries curated `###` sections.
