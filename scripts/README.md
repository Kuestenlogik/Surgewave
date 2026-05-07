# Surgewave Scripts

A focused toolchain. Two groups: **release** (build/publish/run/docs) and
**developer** (tests, coverage, benchmarks).

## Release toolchain

| Script | Purpose |
|--------|---------|
| `build.ps1`   | Compile the solution and pack NuGet packages to `artifacts/pkg/`. Use this when you need fresh `.nupkg`s (e.g. before rebuilding the Samples solution). |
| `publish.ps1` | Publish self-contained Win/Linux executables (Broker, Gateway, Control, Marketplace, Connector, Cli) to `artifacts/pub/`. Independent of `build.ps1` — `dotnet publish` runs its own implicit build. |
| `start.ps1`   | Start the published Broker, Gateway, Control and Marketplace services from `artifacts/pub/` — each in its own console window. Also adds the `surgewave` CLI from `artifacts/pub/cli/` to `$env:PATH` for the current session (see **CLI in PATH** below). |
| `stop.ps1`    | Gracefully stop the services started via `start.ps1` (only those whose `.exe` lives under this repo's `artifacts/pub/`). |
| `update-local-cache.ps1` | Replace the locally cached NuGet copies of Surgewave packages with the freshly built ones in `artifacts/pkg/`. Use this after `build.ps1` so sibling repos (Surgewave.Connectors, Surgewave.Ai, Surgewave.Iceberg, ...) pick up the latest bits without a version bump. |
| `docs.ps1`    | Build the DocFX documentation site to `artifacts/docs/`. Optional `-Serve` for a local preview server. |

### Typical flow

```powershell
# 1. Publish self-contained executables (Release, win-x64)
.\scripts\publish.ps1

# 2. Start services + add surgewave CLI to PATH for this session
#    (dot-source — note the leading ". " — so PATH changes stick in the current shell)
. .\scripts\start.ps1

# 3. ... demo runs ...

# 4. Stop everything
.\scripts\stop.ps1
```

### Cross-repo iteration (sibling repos consuming Surgewave packages)

When working on Surgewave code that other Surgewave repos consume via NuGet (Surgewave.Connectors,
Surgewave.Ai, Surgewave.Iceberg, Surgewave.Bootcamp, ...), bumping the version on every micro-change
is impractical. The global NuGet cache (`~/.nuget/packages/<name>/<version>/`) is
content-addressable: once a `<version>` is cached, NuGet treats it as canonical and
never re-fetches.

`update-local-cache.ps1` overwrites those cached copies in place after a build:

```powershell
# 1. Rebuild + repack Surgewave
.\scripts\build.ps1

# 2. Replace every locally cached Kuestenlogik.Surgewave.* package with the new bits
.\scripts\update-local-cache.ps1

# 3. In the consuming repo: dotnet build picks up the new content automatically.
```

Useful flags:

- `-DryRun` — list what would change without touching the cache.
- `-Filter Kuestenlogik.Surgewave.Streams` — replace a single package (or a wildcard subset).
- `-Install` — also seed packages that are not yet cached locally.

### CLI in PATH (dot-source vs. normal invocation)

`start.ps1` tries to add `artifacts/pub/cli/<runtime>/` to the **front** of
`$env:PATH` so `surgewave` is immediately usable after starting the services.
Because PowerShell scripts run in a child scope, this only works when the
script is **dot-sourced**:

```powershell
# Services start AND surgewave CLI is in PATH afterwards:
. .\scripts\start.ps1

# Services start but PATH is NOT modified — the script prints the
# manual command you can run yourself:
.\scripts\start.ps1
```

The PATH change is **session-local**: closing the pwsh window restores the
previous environment, and nothing is written to the user or system
environment. Use `-SkipCli` to suppress the CLI/PATH logic entirely.

### Build-only (NuGet packages, no executables)

```powershell
# Compile + pack NuGets — required before rebuilding Samples
.\scripts\build.ps1
```

### Docs

```powershell
# Build docs to artifacts/docs/
.\scripts\docs.ps1

# Build and serve at http://localhost:8080
.\scripts\docs.ps1 -Serve

# Regenerate API metadata from source first
.\scripts\docs.ps1 -Clean -WithMetadata
```

## Developer scripts

| Script | Purpose |
|--------|---------|
| `run-coverage.ps1`             | Run all tests with coverlet and generate an HTML coverage report. |
| `run-integration-tests.ps1`    | Start a broker, run Kafka compatibility tests against it, then stop the broker. |
| `run-all-benchmarks.ps1`       | Run every benchmark category sequentially. |
| `update-benchmark-baselines.ps1` | Run benchmarks and refresh the regression baselines under `artifacts/benchmarks/`. |

## Common manual commands (no script needed)

```powershell
# Build
dotnet build Kuestenlogik.Surgewave.slnx -c Release

# Test
dotnet test Kuestenlogik.Surgewave.slnx -v normal

# Run broker from source (with PG wire + materialized views)
dotnet run --project src/Kuestenlogik.Surgewave.Broker -- --Surgewave:PostgreSql:Enabled=true

# Run Control UI from source
dotnet run --project src/Kuestenlogik.Surgewave.Control --urls "http://localhost:5050"

# Pack NuGets only
dotnet pack Kuestenlogik.Surgewave.slnx -c Release

# Install a plugin via the CLI
surgewave plugin install path/to/plugin.swpkg
```
