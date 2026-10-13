# Reference Baselines

JSON sidecar files (`*.json`) from official public runs on the reference hardware (see `infra/aws-bench/COSTS.md`). Used by `surgewave-bench public --compare <file>.json` as the basis for comparison.

## Naming convention

`reference-<hardware>-<surgewave-version>.json`

Examples:
- `reference-c7i-4xl-v0.2.json` — c7i.4xlarge AWS, Surgewave v0.2
- `reference-c7i-4xl-v0.3.json` — same hardware, newer Surgewave version

A user on different hardware can use the same reference file, but has to take the hardware snapshot difference into account in the compare output — the reports render both hardware snapshots.

## Source data

Each baseline comes from a single `surgewave-bench public` run whose Markdown report is committed under `docs/benchmarks/results-vX.Y.md`. The JSON sidecar here is the machine-readable variant of the same run (produced by `--json <file>.json` during the run).

## Current state

Phase 1 (now): empty. Phase 2 adds the first baselines as soon as the scenarios are complete and a first reference run has been made.
