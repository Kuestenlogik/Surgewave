# Capturing marketing screenshots + demo video

Modeled on Bowire's Playwright-based capture pipeline, but with **end-to-end
automation** — including auto-starting Broker + Control (no manual prerequisite).

## Quick start

```bash
# One command, everything:
pwsh ./scripts/capture-pipeline.ps1

# Faster iteration (skip build, dark only):
pwsh ./scripts/capture-pipeline.ps1 -SkipBuild -Theme dark
```

The pipeline:

1. Builds the solution (optional, skippable)
2. Starts Surgewave-Broker (Kafka :9092, admin :9093, gRPC :5095)
3. Waits for Broker health
4. Starts Surgewave.Control (configurable port, default :5050)
5. Waits for Control health
6. Seeds demo data (via `scripts/seed-demo-data.ps1` — TODO)
7. Installs Playwright + Chromium if not present
8. Runs `capture-screenshots.js` (9 screens × dark+light)
9. Runs `record-demo.js` (30s WebM × dark+light)
10. Cleans up (kills Broker + Control)

Outputs:

```
site/assets/images/screenshots/control-*-{dark,light}.png
site/assets/videos/surgewave-demo-{dark,light}.webm
```

## Known issues (Stand 2026-05-11)

### Broker-Startup-Race

The broker logs `Broker started successfully` and lists admin endpoints
(:9093), but the HTTP listener doesn't accept connections immediately —
`Wait-ForHealth` against `http://127.0.0.1:9093/admin/audit` times out
even after 120s in some cases. The Kafka-wire-protocol-listener on :9092
similarly delays.

**Root cause** (TBD): probably ASP.NET-Kestrel startup ordering — the
plugin-discovery + intent-config + auto-tuning init runs before the
HTTP-server.Start(). Logs suggest broker reaches steady state but the
HTTP socket is not yet bound when health-probe fires.

**Workarounds until fixed**:

- Run broker manually first and verify ports respond, then run pipeline
  with `-SkipBuild` (assumes broker already running — pipeline tries to
  start a second one which fails, but Wait-ForHealth on existing one succeeds)
- Or run `scripts/start.ps1` (uses published `artifacts/pub/` executables —
  faster startup) before capture-pipeline.ps1

### Seed-data script missing

`scripts/seed-demo-data.ps1` doesn't exist yet. Without it, screenshots
show empty-state views (no topics, no consumer-groups, no pipelines).
For marketing-quality captures, seed-data should produce:

- 6-10 topics with realistic names + retention configs
- Multiple consumer-groups with varying lag (some healthy, some lagging)
- 1-2 pipeline definitions (Source → Filter → Enrich → Sink)
- A few Avro/JSON/Protobuf schemas registered
- A signed Sealbolt-test-plugin (.swpkg) installed (shows "Verified" badge)
- One or two configured connectors (Postgres-CDC, S3-Sink)

### Control-UI maturity

Many screens in `screenshots.html` reference UI routes that may not yet
be fully implemented in Surgewave.Control (Pipeline-Editor, Schema-Registry,
Plugin-Marketplace). Update `capture-screenshots.js` selectors (TODO markers)
as Control-routes solidify.

## Prerequisites

| What | Why | How |
|------|-----|-----|
| Node.js 20+ | Playwright runtime | https://nodejs.org/ |
| Pwsh 7+ | Pipeline orchestrator | Built-in on Windows 11 |
| .NET 10 SDK | Surgewave build | https://dotnet.microsoft.com/ |
| Free ports | 5050, 9092, 9093, 5095 | `netstat -an | grep <port>` |

## Script structure

| File | Purpose |
|------|---------|
| `scripts/capture-pipeline.ps1` | End-to-end orchestrator (build → start → seed → capture → cleanup) |
| `scripts/capture-screenshots.js` | Playwright script: 9 PNGs per theme |
| `scripts/record-demo.js` | Playwright script: 30s WebM per theme (uses `recordVideo` context option) |
| `scripts/seed-demo-data.ps1` | TODO — populates broker with marketing-realistic data |
| `package.json` | npm devDependencies (@playwright/test, cross-env) |

## Comparison with Bowire pattern

Bowire ships:

- `scripts/capture-screenshots.js` — drives Bowire.Samples.Combined SPA
- `scripts/record-demo.js` (in worktrees) — same shape, 3 beats

Differences for Surgewave:

| Aspect | Bowire | Surgewave |
|--------|--------|-----------|
| App-Start | Manual (`dotnet run` in separate terminal) | Automated via `capture-pipeline.ps1` |
| Protocol | HTTPS (with `ignoreHTTPSErrors: true`) | HTTP (no certs needed in dev) |
| UI-Shape | Single-page app | Multi-page (routes: /dashboard, /topics, /pipelines, …) |
| Demo-beats | 3 (REST + gRPC + SignalR round-trips) | 4 (dashboard → topic → pipeline → marketplace) |
| Seed-data | Combined sample has built-in fake data | Separate seed script (TODO) |

## When to re-run

- Before a marketing-site refresh (Pages-deploy)
- After a Control-UI change (component restyling, new pages)
- After a brand change (logo, colors)
- Quarterly snapshot for fresh marketing materials

## CI integration (future)

The pipeline is currently dev-only. Production-CI would need:

1. Docker-based broker + control containers (capespire-style
   `dotnet publish /t:PublishContainer`)
2. Health-checks visible to Docker / GitHub Actions
3. Headless Playwright in the runner
4. Artifact-upload for PNGs + WebMs to Pages

Defer until manual workflow is stable + Broker-Startup-Race fixed.
