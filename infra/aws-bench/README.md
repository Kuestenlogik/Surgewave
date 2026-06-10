# Surgewave Public Benchmark — AWS Infrastructure

Spin up a clean reference instance, run `surgewave-bench public`, pull the results, destroy. Eine voll-Run kostet ~$1.90 on-demand, dauert ~2 h wall-clock.

## Files

| File | Was |
|---|---|
| `COSTS.md` | Detailed cost breakdown per release, fail-modes, monthly-audit checklist. **Read first.** |
| `main.tf` | Terraform für eine ephemerale c7i.4xlarge in eu-central-1 mit cloud-init das Docker + .NET 10 + surgewave-bench-tool installiert. |
| (vorerst nicht) `terraform.tfvars.example` | Wird ergänzt, sobald die Tool-NuGet-Package public ist und der Tool-Install ohne weitere Auth funktioniert. |

## Voraussetzung — User Side

```bash
aws configure   # ein IAM-User mit ec2:* + iam:PassRole minimal scope
aws ec2 create-key-pair --key-name surgewave-bench \
  --query KeyMaterial --output text > ~/.ssh/surgewave-bench.pem
chmod 600 ~/.ssh/surgewave-bench.pem
```

## Run-Sequence

```bash
cd infra/aws-bench

terraform init
terraform apply -var='key_name=surgewave-bench'
# Outputs zeigen:
#   ssh_command           = …
#   run_command           = …
#   result_pull_command   = …
#   destroy_reminder      = "Run terraform destroy when done."

# 1) SSH einloggen (cloud-init dauert ~3 min — falls "command not found", warten)
$(terraform output -raw ssh_command)

# 2) Auf der Instanz: Bench laufen lassen (~2 h)
~/.dotnet/tools/surgewave-bench public \
  --message-count 1000000 --payload 1024 \
  --output ~/results.md --json ~/results.json

# 3) Lokal: Results pullen
$(terraform output -raw result_pull_command)

# 4) MANDATORY: Instance + EBS-Volume zerstören
terraform destroy
```

## Cost-Gate

Vor jedem `terraform apply`:
- Aktuelle AWS-Preise checken (siehe `COSTS.md`)
- Budget-Alarm im AWS-Account auf $50/Monat setzen (`aws budgets create-budget`) — fängt vergessene Instanzen ab

Nach jedem `terraform destroy`:
- Verify keine Reste:
  ```bash
  aws ec2 describe-instances --filters 'Name=tag:Purpose,Values=surgewave-bench' \
    'Name=instance-state-name,Values=running,pending,stopping,stopped'
  aws ec2 describe-volumes --filters 'Name=tag:Purpose,Values=surgewave-bench'
  ```

## Was Phase 1 NICHT macht

Phase 1 ist Skeleton + Cost-Doc. **Wird nicht ausgeführt** vom CI/Setup-Script automatisch — du startest den Run manuell wenn du eine Release-Public-Number brauchst.

Phase 2 ergänzt:
- Vollständige Scenario-Implementations (aktuell Stubs)
- Reference-Baselines unter `benchmarks/baselines/` für `--compare`
- Optionaler `--public` GitHub-Action-Trigger der via OIDC die Instanz hochfährt + den Run startet + Results in eine PR-Commit packt (eliminiert den Hands-on-Step)
