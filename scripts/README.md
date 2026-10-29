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
| `update-local-cache.ps1` | Replace the locally cached NuGet copies of Surgewave packages with the freshly built ones in `artifacts/pkg/`. Use this after `build.ps1` so sibling repos that consume `Kuestenlogik.Surgewave.*` packages pick up the latest bits without a version bump. |
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

When working on Surgewave code that other repos consume via NuGet, bumping
the version on every micro-change is impractical. The global NuGet cache
(`~/.nuget/packages/<name>/<version>/`) is content-addressable: once a
`<version>` is cached, NuGet treats it as canonical and never re-fetches.

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
| `stack.ps1`                    | The local stack for trying the checkout by hand: one broker, or a cluster (`-Brokers 3`), and the Control UI, each from a copy of the Debug build output in a hidden console. Verbs `start`, `stop`, `restart`, `status`, `log` and `build`; `-Offset` runs stacks side by side. Builds when the build is stale. `./scripts/stack.ps1` alone shows its help. See **Local stack** below. |
| `run-coverage.ps1`             | Run all tests with coverlet and generate an HTML coverage report. |
| `run-integration-tests.ps1`    | Start a broker, run Kafka compatibility tests against it, then stop the broker. |
| `run-all-benchmarks.ps1`       | Run every benchmark category sequentially. |
| `update-benchmark-baselines.ps1` | Run benchmarks and refresh the regression baselines under `artifacts/benchmarks/`. |

### Local stack

```powershell
# One broker and the Control UI: bootstrap localhost:9092, UI http://localhost:5050
./scripts/stack.ps1 start

# A cluster of three beside it, every port moved by 100: bootstrap localhost:9192,localhost:9202,localhost:9212
./scripts/stack.ps1 start -Offset 100 -Brokers 3

./scripts/stack.ps1 status                    # every running stack; exit code 0 running, 1 not running, 2 in part
./scripts/stack.ps1 log -Part broker2 -Offset 100 -Follow
./scripts/stack.ps1 restart -Offset 100       # same brokers, same data, the latest build
./scripts/stack.ps1 stop -Offset 100
```

Every port comes from the parts' own `appsettings.json`, moved by the offset, and by 10 more for each broker before
it. A stack lives in `artifacts/stacks/<offset>/`: `stack.json` (the bootstrap list, every broker's ports, the
Control URL; other scripts read it), `env/`, `bin/`, `data/` and `logs/`. `artifacts/stack.log` records every call
and every process started or ended. `start` makes the brokers' data afresh, `restart` keeps it, and a `start` over a
stack that already runs complete is used as it is. Offsets are multiples of 100; `SURGEWAVE_STACK_OFFSET` sets the
default.

## Common manual commands (no script needed)

```powershell
# Build
dotnet build Kuestenlogik.Surgewave.slnx -c Release

# Test
dotnet test Kuestenlogik.Surgewave.slnx -v normal

# Run broker from source (with PG wire + materialized views)
dotnet run --project src/Kuestenlogik.Surgewave.Broker.App -- --Surgewave:PostgreSql:Enabled=true

# Run Control UI from source
dotnet run --project src/Kuestenlogik.Surgewave.Control --urls "http://localhost:5050"

# Pack NuGets only
dotnet pack Kuestenlogik.Surgewave.slnx -c Release

# Install a plugin via the CLI
surgewave plugins install path/to/plugin.swpkg
```
