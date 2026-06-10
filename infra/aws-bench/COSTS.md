# Surgewave Public-Benchmark AWS Kosten

> **Stand:** 2026-06-10. AWS-Preise schwanken — Tabelle vor jeder neuen Release-Runde gegen [AWS Pricing](https://aws.amazon.com/ec2/pricing/on-demand/) verifizieren und unten den Stand aktualisieren.

Das `Kuestenlogik.Surgewave.Benchmarks.Public`-Tool ist hardware-agnostisch, aber die **Public Reference Numbers** für jede Release laufen auf einer konsistenten AWS-Instanz, damit Vergleichbarkeit zwischen Releases garantiert ist und Externe die Zahlen unabhängig reproducen können.

## Reference-Hardware (Empfehlung)

| Field | Value | Warum |
|---|---|---|
| Instance type | `c7i.4xlarge` | 16 vCPU / 32 GiB RAM — passt zu Surgewave-Topology mit Broker + 2× Container (Kafka+Redpanda). Industriestandard für Broker-Vergleichs-Benchmarks (Redpanda + Confluent benchen auf c-Klasse). |
| CPU | Intel Xeon 4th-gen Sapphire Rapids @ 3.2-3.8 GHz | Modern + verbreitet, AVX-512 verfügbar (relevant für Surgewave's SIMD-Pfade). |
| Storage | EBS `gp3` 200 GiB, 10k IOPS, 250 MB/s | Schnell genug dass Disk-I/O nicht der Bottleneck wird, klein genug dass die Reservation nicht das Budget sprengt. |
| Network | Bis 12.5 Gbps | Verhindert Network-Bottleneck bei high-throughput Tests. |
| OS | Ubuntu 24.04 LTS x86_64 | Verbreitet, predictable Kernel-Verhalten, .NET 10 supported. |
| Region | `eu-central-1` (Frankfurt) | Niedrige Latenz für Kuestenlogik-Setup. Andere Regionen ggf. günstiger — kosten unten zeigen Frankfurt-Preise. |

Alternative budgetfreundlich: `c6a.4xlarge` (AMD EPYC 3rd-gen) ~30 % günstiger, ähnliche Single-Thread-Performance, ohne AVX-512. Für die Marketing-Zahlen Intel bleiben, für Dev-Iteration AMD ok.

## Kosten pro Public-Run

Ein voller G3-Run umfasst:
- `throughput-1p1c` scenario × 4 Systems × 3 Payload-Größen (100 B / 1 KB / 10 KB) = 12 Sub-Runs
- `latency-acks-all` scenario × 4 Systems × 3 Payload-Größen = 12 Sub-Runs
- Phase 2 scenarios (TailLatency, MultiPartitionFanout, ConsumerLagRecovery) × 4 Systems = ~12 weitere Sub-Runs

Pro Sub-Run: ~3 min Warmup + Measurement + Container-Teardown → ~36 Sub-Runs × 3 min ≈ **108 min**.
Plus initialer Container-Pull (Kafka + Redpanda Image) + Setup: ~12 min.
Plus Save-Output + Cleanup: ~5 min.

**Gesamtdauer: ~2 h 5 min wall-clock pro Public-Run.**

### Cost-Breakdown

| Posten | On-Demand (Frankfurt) | Spot (Frankfurt) | Anmerkung |
|---|---:|---:|---|
| `c7i.4xlarge` Compute, 2 h 5 min | **$1.84** | **~$0.68** | `c7i.4xlarge` = $0.886/h on-demand, ~$0.32/h spot (3-tage Mittel) |
| EBS `gp3` 200 GiB, 1 day Anteil | $0.05 | $0.05 | Volume runs ggf. länger als die Compute-Stunden, ein Tag konservativ |
| Egress (Markdown/JSON Result-Files, ~2 MB) | < $0.01 | < $0.01 | Vernachlässigbar |
| **Total** | **~$1.90** | **~$0.74** | |

### Pro-Release-Kosten

Ein Public-Run pro Major-Release-Tag:
- **v0.2 → v1.0**: Annahme 4 Major-Releases (0.2 / 0.3 / 0.4 / 1.0) → **4 × $1.90 = ~$7.60** on-demand, **~$3 spot**, über das ganze Jahr.
- Plus 2-3 Verification-Runs vor jedem Release-Cut (z.B. nach Performance-PRs): **~$15-20/Jahr** on-demand, **~$6-8/Jahr** spot.

**Erwarteter Jahres-Bedarf: $15-25 on-demand / $5-10 spot.** Vernachlässigbar.

### Was es teurer machen würde

- **Mehr Hardware-Profile parallel**: c7i + c7a + c6i würden 3× kosten. Vorschlag: ein Reference-Profil (c7i) + ein „User-might-have-this" Profil (c6a) für Vergleich, mehr nicht.
- **Längere Iterations**: aktuell 3 Measurement-Rounds pro Scenario. Bei P99.99-Stabilität wären 10 Rounds besser → ~3 h pro Run → ~$2.85 statt $1.90.
- **GP3 IOPS hochdrehen**: 16k IOPS / 1000 MB/s wären präziser für Storage-bound Scenarios, kosten aber ~$10/Monat zusätzlich solange das Volume existiert. Lieber das Volume nach jedem Run löschen.
- **Multi-AZ-Setups** (für Replication-Tests): jeder Cross-AZ-Run nimmt 2-3× Compute-Zeit + Cross-AZ-Egress. Erst relevant wenn Replication-Performance gemessen werden soll (nicht G3-Scope).

## Setup-Sequence (manuell, vorerst)

Bis `terraform.tf` (siehe `main.tf` in diesem Verzeichnis) finalisiert ist:

```bash
# 1) Instanz starten (us-east-1 wäre $0.10 günstiger, Frankfurt für niedrige Setup-Latenz)
aws ec2 run-instances \
  --image-id ami-XXXXXXXX  # Ubuntu 24.04 LTS x86_64 in eu-central-1
  --instance-type c7i.4xlarge \
  --key-name surgewave-bench \
  --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=200,VolumeType=gp3,Iops=10000,Throughput=250}' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Purpose,Value=surgewave-bench}]'

# 2) Setup via SSH
ssh ubuntu@<public-ip>
sudo apt-get update && sudo apt-get install -y docker.io
curl -fsSL https://dot.net/v1/dotnet-install.sh | sudo bash -s -- --version 10.0.7 --install-dir /usr/share/dotnet
sudo ln -s /usr/share/dotnet/dotnet /usr/local/bin/dotnet

# 3) Tool installieren + Run
dotnet tool install -g Kuestenlogik.Surgewave.Benchmarks
~/.dotnet/tools/surgewave-bench public \
  --message-count 1000000 \
  --payload 1024 \
  --output ~/results-v0.2.md \
  --json ~/results-v0.2.json

# 4) Results runter, Instanz terminieren
scp ubuntu@<public-ip>:~/results-v0.2.* docs/benchmarks/
aws ec2 terminate-instances --instance-ids i-XXXXXXXXX
```

Geschätzte Hands-on-Zeit: 15 min Setup + ~2 h unattended Run + 5 min Pull-back-and-cleanup. Wenn das via Terraform automatisiert ist, sinkt das auf 5 min Hands-on.

## Cost-Watch

Bei monatlichen Audits sicherstellen, dass:
- keine vergessene Instance läuft (`aws ec2 describe-instances --filters 'Name=tag:Purpose,Values=surgewave-bench' 'Name=instance-state-name,Values=running'`)
- keine verwaisten EBS-Volumes existieren (`aws ec2 describe-volumes --filters 'Name=status,Values=available'`)

Eine vergessene `c7i.4xlarge`-Instanz für einen Monat = **$640**. Das ist die einzige relevante Fail-Mode dieser Setup-Variante. Terraform mit `terraform destroy` als Mandatory-Cleanup-Step entschärft das.
