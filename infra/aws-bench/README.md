# Surgewave Public Benchmark — AWS Infrastructure

Spin up a clean reference instance, run `surgewave-bench public`, pull the results, destroy. A full run costs ~$1.90 on-demand and takes ~2 h wall-clock.

## Files

| File | What |
|---|---|
| `COSTS.md` | Detailed cost breakdown per release, fail-modes, monthly-audit checklist. **Read first.** |
| `main.tf` | Terraform for an ephemeral c7i.4xlarge in eu-central-1 with cloud-init that installs Docker + .NET 10 + surgewave-bench-tool. |
| (not yet) `terraform.tfvars.example` | Will be added as soon as the tool NuGet package is public and the tool install works without further auth. |

## Prerequisite — user side

```bash
aws configure   # an IAM user with ec2:* + iam:PassRole minimal scope
aws ec2 create-key-pair --key-name surgewave-bench \
  --query KeyMaterial --output text > ~/.ssh/surgewave-bench.pem
chmod 600 ~/.ssh/surgewave-bench.pem
```

## Run sequence

```bash
cd infra/aws-bench

terraform init
terraform apply -var='key_name=surgewave-bench'
# Outputs show:
#   ssh_command           = …
#   run_command           = …
#   result_pull_command   = …
#   destroy_reminder      = "Run terraform destroy when done."

# 1) Log in via SSH (cloud-init takes ~3 min — if "command not found", wait)
$(terraform output -raw ssh_command)

# 2) On the instance: run the bench (~2 h)
~/.dotnet/tools/surgewave-bench public \
  --message-count 1000000 --payload 1024 \
  --output ~/results.md --json ~/results.json

# 3) Locally: pull the results
$(terraform output -raw result_pull_command)

# 4) MANDATORY: destroy instance + EBS volume
terraform destroy
```

## Cost gate

Before every `terraform apply`:
- Check current AWS prices (see `COSTS.md`)
- Set a budget alarm in the AWS account at $50/month (`aws budgets create-budget`) — catches forgotten instances

After every `terraform destroy`:
- Verify nothing is left over:
  ```bash
  aws ec2 describe-instances --filters 'Name=tag:Purpose,Values=surgewave-bench' \
    'Name=instance-state-name,Values=running,pending,stopping,stopped'
  aws ec2 describe-volumes --filters 'Name=tag:Purpose,Values=surgewave-bench'
  ```

## What phase 1 does NOT do

Phase 1 is skeleton + cost doc. **It is not run automatically** by CI or the setup script — you start the run manually when you need a public number for a release.

Phase 2 adds:
- Complete scenario implementations (currently stubs)
- Reference baselines under `benchmarks/baselines/` for `--compare`
- An optional `--public` GitHub Action trigger that brings up the instance via OIDC + starts the run + packs the results into a PR commit (eliminates the hands-on step)
