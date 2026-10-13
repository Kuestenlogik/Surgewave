# Surgewave Public Benchmark AWS Costs

> **As of:** 2026-06-10. AWS prices fluctuate — verify the table against [AWS Pricing](https://aws.amazon.com/ec2/pricing/on-demand/) before every new release round and update the date below.

The `Kuestenlogik.Surgewave.Benchmarks.Public` tool is hardware-agnostic, but the **public reference numbers** for every release run on a consistent AWS instance, so that comparability between releases is guaranteed and outsiders can reproduce the numbers independently.

## Reference hardware (recommendation)

| Field | Value | Why |
|---|---|---|
| Instance type | `c7i.4xlarge` | 16 vCPU / 32 GiB RAM — fits the Surgewave topology with broker + 2× container (Kafka+Redpanda). Industry standard for broker comparison benchmarks (Redpanda + Confluent benchmark on the c class). |
| CPU | Intel Xeon 4th-gen Sapphire Rapids @ 3.2-3.8 GHz | Modern + widespread, AVX-512 available (relevant for Surgewave's SIMD paths). |
| Storage | EBS `gp3` 200 GiB, 10k IOPS, 250 MB/s | Fast enough that disk I/O does not become the bottleneck, small enough that the reservation does not blow the budget. |
| Network | Up to 12.5 Gbps | Prevents a network bottleneck in high-throughput tests. |
| OS | Ubuntu 24.04 LTS x86_64 | Widespread, predictable kernel behaviour, .NET 10 supported. |
| Region | `eu-central-1` (Frankfurt) | Low latency for the Kuestenlogik setup. Other regions may be cheaper — the costs below show Frankfurt prices. |

Budget-friendly alternative: `c6a.4xlarge` (AMD EPYC 3rd-gen) ~30 % cheaper, similar single-thread performance, without AVX-512. Stay on Intel for the marketing numbers; AMD is fine for dev iteration.

## Cost per public run

A full G3 run comprises:
- `throughput-1p1c` scenario × 4 systems × 3 payload sizes (100 B / 1 KB / 10 KB) = 12 sub-runs
- `latency-acks-all` scenario × 4 systems × 3 payload sizes = 12 sub-runs
- Phase 2 scenarios (TailLatency, MultiPartitionFanout, ConsumerLagRecovery) × 4 systems = ~12 more sub-runs

Per sub-run: ~3 min warmup + measurement + container teardown → ~36 sub-runs × 3 min ≈ **108 min**.
Plus the initial container pull (Kafka + Redpanda image) + setup: ~12 min.
Plus save output + cleanup: ~5 min.

**Total duration: ~2 h 5 min wall-clock per public run.**

### Cost breakdown

| Item | On-demand (Frankfurt) | Spot (Frankfurt) | Note |
|---|---:|---:|---|
| `c7i.4xlarge` compute, 2 h 5 min | **$1.84** | **~$0.68** | `c7i.4xlarge` = $0.886/h on-demand, ~$0.32/h spot (3-day average) |
| EBS `gp3` 200 GiB, 1 day share | $0.05 | $0.05 | The volume may run longer than the compute hours, one day is conservative |
| Egress (Markdown/JSON result files, ~2 MB) | < $0.01 | < $0.01 | Negligible |
| **Total** | **~$1.90** | **~$0.74** | |

### Cost per release

One public run per major release tag:
- **v0.2 → v1.0**: assuming 4 major releases (0.2 / 0.3 / 0.4 / 1.0) → **4 × $1.90 = ~$7.60** on-demand, **~$3 spot**, over the whole year.
- Plus 2-3 verification runs before each release cut (e.g. after performance PRs): **~$15-20/year** on-demand, **~$6-8/year** spot.

**Expected annual need: $15-25 on-demand / $5-10 spot.** Negligible.

### What would make it more expensive

- **More hardware profiles in parallel**: c7i + c7a + c6i would cost 3×. Proposal: one reference profile (c7i) + one "user-might-have-this" profile (c6a) for comparison, nothing more.
- **Longer iterations**: currently 3 measurement rounds per scenario. For P99.99 stability 10 rounds would be better → ~3 h per run → ~$2.85 instead of $1.90.
- **Turning up GP3 IOPS**: 16k IOPS / 1000 MB/s would be more precise for storage-bound scenarios, but cost ~$10/month extra for as long as the volume exists. Better to delete the volume after each run.
- **Multi-AZ setups** (for replication tests): every cross-AZ run takes 2-3× the compute time + cross-AZ egress. Only relevant once replication performance is to be measured (not G3 scope).

## Setup sequence (manual, for now)

Until `terraform.tf` (see `main.tf` in this directory) is finalised:

```bash
# 1) Start the instance (us-east-1 would be $0.10 cheaper, Frankfurt for low setup latency)
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

# 3) Install the tool + run
dotnet tool install -g Kuestenlogik.Surgewave.Benchmarks
~/.dotnet/tools/surgewave-bench public \
  --message-count 1000000 \
  --payload 1024 \
  --output ~/results-v0.2.md \
  --json ~/results-v0.2.json

# 4) Pull the results down, terminate the instance
scp ubuntu@<public-ip>:~/results-v0.2.* docs/benchmarks/
aws ec2 terminate-instances --instance-ids i-XXXXXXXXX
```

Estimated hands-on time: 15 min setup + ~2 h unattended run + 5 min pull-back-and-cleanup. Once this is automated via Terraform, it drops to 5 min hands-on.

## Cost watch

During monthly audits make sure that:
- no forgotten instance is running (`aws ec2 describe-instances --filters 'Name=tag:Purpose,Values=surgewave-bench' 'Name=instance-state-name,Values=running'`)
- no orphaned EBS volumes exist (`aws ec2 describe-volumes --filters 'Name=status,Values=available'`)

A forgotten `c7i.4xlarge` instance for one month = **$640**. That is the only relevant fail mode of this setup variant. Terraform with `terraform destroy` as a mandatory cleanup step defuses it.
