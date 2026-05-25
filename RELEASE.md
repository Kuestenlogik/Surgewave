# Release Process

How to ship a Surgewave release. The Release workflow (`.github/workflows/release.yml`)
runs on every `v*` tag push and produces NuGet packages, container images, Windows MSI,
Linux .deb/.rpm, self-contained binaries for win/linux/macOS, OCI archives, and a
GitHub Release with auto-generated notes.

## TL;DR

```bash
# 1. Pick a version (follow SemVer)
VERSION=0.1.0

# 2. Sanity-check local
dotnet test Kuestenlogik.Surgewave.slnx -c Release

# 3. Update ROADMAP.md if the release shipped a notable feature
$EDITOR ROADMAP.md
git commit -am "release: notes for v$VERSION"
git push

# 4. Cut and push the tag
git tag -a "v$VERSION" -m "Surgewave v$VERSION"
git push origin "v$VERSION"

# 5. Watch the workflow
gh run watch
```

## Version conventions

- **Stable**: `v0.1.0`, `v1.2.3` — published to nuget.org.
- **Release candidate**: `v0.1.0-rc1`, `v0.2.0-rc.2` — published to GitHub Packages only.
  nuget.org push is skipped by the `!contains(github.ref, '-rc')` gate in `release.yml`.
- **Pre-release labels** other than `-rc*` (e.g. `-alpha`, `-beta`, `-preview`) currently
  fall under the stable branch — guard them manually if you don't want them on nuget.org.

## Pre-flight checklist

Before tagging, verify in order:

- [ ] `main` is green on CI (`gh run list --workflow=ci.yml --branch=main --limit=1`).
- [ ] Coverage workflow green (`coverage.yml` ≥ 70 % line, ≥ 60 % branch — enforced).
- [ ] `dotnet build Kuestenlogik.Surgewave.slnx -c Release` clean locally.
- [ ] `dotnet test Kuestenlogik.Surgewave.slnx -c Release` all green locally.
- [ ] No `TODO` / `FIXME` / `XXX` introduced in the diff since the last release that block ship.
- [ ] `ROADMAP.md` has an entry for the release (or the release is small enough not to warrant one).
- [ ] The CHANGELOG/Release Notes will be auto-generated from PR titles via
      `generate_release_notes: true` in `release.yml`. If a notable PR was force-pushed
      with a bad title, fix it on GitHub first.
- [ ] If the release contains breaking API changes, the major version bumps (SemVer).

## Secrets the release needs

These must exist on the repo or org. Check `gh secret list`:

| Secret           | Used for                            | Required?          |
|------------------|-------------------------------------|--------------------|
| `GITHUB_TOKEN`   | GitHub Packages push, GHCR push     | Auto-provided.     |
| `NUGET_API_KEY`  | nuget.org push                      | Only stable tags.  |

If `NUGET_API_KEY` is missing, the nuget.org step is skipped via the
`env.NUGET_API_KEY != ''` gate — the rest of the release still publishes.

## Cutting the tag

Annotated tags only (the workflow reads the tag, but unsigned/lightweight tags lose history):

```bash
git tag -a "v$VERSION" -m "Surgewave v$VERSION"
git push origin "v$VERSION"
```

Push the tag from the commit on `main` you want released. The workflow uses
`GITHUB_REF_NAME` minus the `v` prefix as the package version — so `v0.1.0` produces
`0.1.0` everywhere.

## Watching the run

```bash
gh run watch                              # latest run
gh run view --log-failed                  # tail failed steps if any
```

Typical wall-clock: ~12–18 minutes (multiple `dotnet publish -r` invocations dominate).

If a step fails after publishing artefacts but before creating the GitHub Release,
the artefacts are still uploaded to the workflow run — download from the Actions UI
and create the release manually with `gh release create`.

## Verifying the release

After the workflow finishes:

```bash
# GitHub Release exists with all assets
gh release view "v$VERSION"

# NuGet package landed on nuget.org (stable only — wait ~5–10 min for indexing)
dotnet nuget search "Kuestenlogik.Surgewave.Client" --source https://api.nuget.org/v3/index.json

# Container images
docker pull "ghcr.io/kuestenlogik/kuestenlogik.surgewave.broker:$VERSION"
docker pull "ghcr.io/kuestenlogik/kuestenlogik.surgewave.control:$VERSION"

# Self-contained binary smoke-test on Linux
curl -L "https://github.com/Kuestenlogik/Surgewave/releases/download/v$VERSION/surgewave-broker-linux-x64-$VERSION.tar.gz" | tar xz
./Kuestenlogik.Surgewave.Broker --version

# MSI smoke-test on Windows
msiexec /i "surgewave-$VERSION-win-x64.msi" /qn
```

## Hotfix workflow

For a single-fix patch on a released version `vX.Y.Z`:

```bash
git checkout -b "hotfix/$VERSION" "vX.Y.Z"
# … fix the bug, commit …
git tag -a "v$VERSION" -m "Surgewave v$VERSION — hotfix"
git push origin "hotfix/$VERSION" "v$VERSION"
```

Then back-merge the hotfix branch into `main` so the fix is on trunk.

## Rolling back a release

GitHub Releases can be deleted but **NuGet packages cannot be unpublished** — they can
only be listed/unlisted. If a release is broken:

1. `gh release delete "v$VERSION"` (removes the Release page + uploaded assets).
2. `dotnet nuget delete Kuestenlogik.Surgewave.Client $VERSION --source https://api.nuget.org/v3/index.json --api-key $NUGET_API_KEY`
   — this **unlists** rather than removes; the version stays addressable for anyone with
   the literal version pin.
3. Ship a new patch release (`v$VERSION` + patch bump) with the fix.

Never reuse a version number. nuget.org rejects duplicates; GitHub Releases would
silently overwrite the assets.

## Related workflows

| File                                      | Trigger              | Purpose                                       |
|-------------------------------------------|----------------------|-----------------------------------------------|
| `.github/workflows/ci.yml`                | push / PR            | Build + test on every commit                  |
| `.github/workflows/coverage.yml`          | push / PR to `main`  | Coverage + Codecov / sticky PR comment        |
| `.github/workflows/release.yml`           | `v*` tag push        | Build all artefacts + GitHub Release          |
| `.github/workflows/docs.yml`              | push to `main`       | Build + deploy the site to GitHub Pages       |
| `.github/workflows/benchmark-regression.yml` | nightly + PR      | Run BenchmarkDotNet, fail on regression       |
