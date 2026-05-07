# Performance Regression Suite

The `Kuestenlogik.Surgewave.Benchmarks.Regression` CLI detects performance regressions by comparing BenchmarkDotNet results against a stored baseline. It integrates with CI to automatically flag regressions on pull requests.

## Overview

The regression suite provides three commands:

| Command | Description |
|---------|-------------|
| `compare` | Compare results against baseline, report regressions |
| `update-baseline` | Merge new results into the baseline file |
| `report` | Generate a Markdown regression report |

## Quick Start

```bash
# Run benchmarks and produce a JSON report
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.Unit -c Release -- \
    --filter '*' --exporters json --job short --artifacts artifacts/benchmarks

# Compare against baseline
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.Regression -c Release -- \
    compare artifacts/benchmarks/*-report-full.json \
    benchmarks/baselines/unit-baseline.json \
    -o artifacts/regression-report.md \
    --fail-on-regression
```

## Commands

### compare

Compares benchmark results against a baseline and optionally writes a Markdown report:

```bash
compare <results-json> <baseline-json> [-o report.md] [--config config.json] [--fail-on-regression]
```

| Flag | Description |
|------|-------------|
| `-o <path>` | Write Markdown report to file |
| `--config <path>` | Custom threshold configuration file |
| `--fail-on-regression` | Exit with code 1 if regressions are detected |

Without `--fail-on-regression`, the command reports regressions but exits with code 0.

### update-baseline

Merges new benchmark results into an existing baseline file:

```bash
update-baseline <results-json> <baseline-json>
```

Existing benchmarks are updated with new values. New benchmarks are added. Benchmarks not in the current run are preserved.

### report

Generates a Markdown regression report (always writes to file):

```bash
report <results-json> <baseline-json> -o report.md [--config config.json]
```

## Threshold Configuration

Configure regression detection thresholds in a JSON config file:

```json
{
    "latencyThresholdPercent": 15.0,
    "throughputThresholdPercent": 10.0,
    "allocationThresholdPercent": 20.0,
    "excludedBenchmarks": [
        "BenchmarkWithHighVariance"
    ],
    "categoryOverrides": {
        "Serialization": {
            "latencyThresholdPercent": 20.0,
            "allocationThresholdPercent": 30.0
        },
        "Storage": {
            "throughputThresholdPercent": 5.0
        }
    }
}
```

### Default Thresholds

| Metric | Default Threshold | Description |
|--------|-------------------|-------------|
| Latency (mean time) | 15% | Flagged if mean execution time increases by more than 15% |
| Throughput (ops/sec) | 10% | Flagged if throughput drops by more than 10% |
| Allocations (bytes) | 20% | Flagged if allocations increase by more than 20% |

### Per-Category Overrides

Category is derived from the benchmark's `[BenchmarkCategory]` attribute or the containing class name. Override specific thresholds per category while keeping global defaults for others by setting the override value; use `null` to fall back to the global default.

## Baseline Management

Baselines are stored as JSON files (e.g., `benchmarks/baselines/unit-baseline.json`). To update after intentional performance changes:

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.Regression -c Release -- \
    update-baseline artifacts/benchmarks/*-report-full.json \
    benchmarks/baselines/unit-baseline.json
```

You can also use the helper script:

```powershell
.\scripts\update-benchmark-baselines.ps1
```

## CI Integration

The `benchmark-regression.yml` GitHub Actions workflow runs automatically on pull requests that modify `src/` or `benchmarks/` files:

1. Builds the benchmark project in Release mode
2. Runs benchmarks with `--job short` for CI-friendly execution time
3. Compares results against the committed baseline
4. Uploads the regression report as a build artifact
5. Fails the check if regressions exceed thresholds (via `--fail-on-regression`)

```yaml
# .github/workflows/benchmark-regression.yml
on:
  pull_request:
    branches: [main]
    paths:
      - 'src/**'
      - 'benchmarks/**'
```

The workflow uses the `--fail-on-regression` flag, so the PR check fails if any benchmark regresses beyond the configured thresholds.

## Regression Severity

Each comparison result is classified:

| Severity | Meaning |
|----------|---------|
| `None` | Within acceptable threshold |
| `Warning` | Close to threshold (not a failure) |
| `Regression` | Exceeds threshold (fails CI with `--fail-on-regression`) |

## Next Steps

- [Benchmarks](benchmarks.md) - Running benchmarks manually
- [Tuning Guide](tuning.md) - Performance tuning recommendations
- [Chaos Testing](../testing/chaos-testing.md) - Resilience testing
