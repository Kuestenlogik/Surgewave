# Maintainer Guide

Internal documentation for Surgewave project maintainers.

## Release Process

### 1. Prepare
```bash
# Ensure main is clean and all tests pass
dotnet test Kuestenlogik.Surgewave.slnx -c Release -v normal

# Review CHANGELOG.md — move [Unreleased] items to new version section
```

### 2. Tag and Push
```bash
# Stable release
git tag v1.0.0
git push --tags

# Beta release
git tag v1.1.0-beta.1
git push --tags
```

### 3. What Happens Automatically
- GitHub Actions builds, tests, and packs with the tag version
- NuGet packages (`*.nupkg`) + symbols (`*.snupkg`) are published to GitHub Packages
- Pre-release versions (`-beta.*`) are marked as pre-release on GitHub Packages

### 4. NuGet.org (future)
When ready for public release:
```bash
dotnet nuget push "artifacts/packages/*.nupkg" --source nuget.org --api-key <KEY>
dotnet nuget push "artifacts/packages/*.snupkg" --source nuget.org --api-key <KEY>
```

### 5. GitHub Release
```bash
gh release create v1.0.0 --title "Surgewave 1.0.0" --notes-file CHANGELOG_EXTRACT.md
```

## Versioning

| Situation | Version | Example |
|-----------|---------|---------|
| Push to main (CI) | `0.1.0-ci.{count}.{sha}` | `0.1.0-ci.905.abc1234` |
| Beta tag | `{tag}` | `1.1.0-beta.1` |
| Stable tag | `{tag}` | `1.1.0` |

- **Patch** (`v1.0.1`): Bug fixes, no API changes
- **Minor** (`v1.1.0`): New features, backwards compatible
- **Major** (`v2.0.0`): Breaking API changes

GitVersion config: `GitVersion.yml` (tag-prefix: `v`)

## NuGet Packages

Surgewave publishes NuGet packages to GitHub Packages on every push to main (pre-release) and on version tags (stable).

## Plugin Packages (.swpkg)

### Building .swpkg files
```bash
# Single plugin
surgewave plugin pack --project src/MyPlugin/

# Via MSBuild (if Kuestenlogik.Surgewave.Sdk is referenced — auto-pack on publish)
dotnet publish src/MyPlugin/ -c Release
```

### Installing .swpkg files
```bash
surgewave plugin install path/to/plugin.swpkg
surgewave plugin install plugins/                    # all in directory
surgewave plugin install plugins/**                  # recursive
```

## Building & Publishing

```powershell
# Compile solution + pack NuGets (artifacts/packages/)
.\scripts\build.ps1

# Publish self-contained executables (artifacts/publish/<Name>/win-x64/)
.\scripts\publish.ps1

# Different runtime
.\scripts\publish.ps1 -Runtime linux-x64

# Only a subset of services
.\scripts\publish.ps1 -Service Broker,Control

# Container images (manual, per service)
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
```

### Publishable Services

| Service | Container | Command |
|---------|-----------|---------|
| Broker | `surgewave/surgewave-broker` | `dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer` |
| Gateway | `surgewave/surgewave-gateway` | `dotnet publish src/Kuestenlogik.Surgewave.Gateway -c Release /t:PublishContainer` |
| Control | `surgewave/surgewave-control` | `dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer` |
| Marketplace | `surgewave/surgewave-marketplace` | `dotnet publish src/Kuestenlogik.Surgewave.Marketplace -c Release /t:PublishContainer` |
| Connect Worker | `surgewave/surgewave-connect` | `dotnet publish src/Kuestenlogik.Surgewave.Connect.Worker -c Release /t:PublishContainer` |
| CLI | — (dotnet tool) | `dotnet tool install -g Kuestenlogik.Surgewave.Cli` |

## License Management

Surgewave uses BSL 1.1 with relative Change Date (5 years per release). No manual date updates needed.

Contributors must sign the CLA (enforced by CLA Assistant bot on PRs). CLA text: `.github/CLA.md`.

## CI/CD

- **Build + Test**: every push to main + every PR
- **Publish to GitHub Packages**: every push to main (pre-release) + tags (stable)
- **Dependabot**: weekly on Sundays, grouped by category

### Monitoring CI
```bash
gh run list --repo Kuestenlogik/Surgewave --workflow ci.yml --limit 5
```
