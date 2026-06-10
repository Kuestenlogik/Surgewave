# Reference Baselines

JSON-Sidecar-Files (`*.json`) aus offiziellen Public-Runs auf der Reference-Hardware (siehe `infra/aws-bench/COSTS.md`). Werden von `surgewave-bench public --compare <file>.json` als Vergleichsbasis genutzt.

## Naming-Convention

`reference-<hardware>-<surgewave-version>.json`

Beispiele:
- `reference-c7i-4xl-v0.2.json` — c7i.4xlarge AWS, Surgewave v0.2
- `reference-c7i-4xl-v0.3.json` — gleicher Hardware, neuere Surgewave-Version

User auf abweichender Hardware kann dasselbe Reference-File nutzen, muss aber im Compare-Output die Hardware-Snapshot-Differenz mit einbeziehen — die Reports rendern beide Hardware-Snapshots.

## Quelldaten

Jede Baseline kommt von einem einzelnen `surgewave-bench public`-Run, dessen Markdown-Report unter `docs/benchmarks/results-vX.Y.md` committed wird. Die JSON-Sidecar hier ist die maschinen-lesbare Variante desselben Runs (durch `--json <file>.json` während dem Run erzeugt).

## Aktuell

Phase 1 (jetzt): leer. Phase 2 ergänzt die ersten Baselines sobald die Scenarios vollständig sind und ein erster Reference-Run gefahren wurde.
