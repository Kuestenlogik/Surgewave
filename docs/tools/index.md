# Tools Overview

Surgewave provides command-line tools for administration and operations.

## Surgewave CLI

The Surgewave CLI provides 65+ commands for complete broker management.

### Installation

```bash
# Build from source
cd src/Kuestenlogik.Surgewave.Cli
dotnet publish -c Release -o ~/.surgewave/bin
export PATH="$PATH:~/.surgewave/bin"

# Verify
surgewave --version
```

### Quick Reference

| Category | Commands |
|----------|----------|
| Topics | list, create, delete, describe, alter-config |
| Produce/Consume | produce, consume, copy |
| Consumer Groups | list, describe, delete |
| Broker | info, health, diagnose, config |
| ACLs | list, add, remove |
| Schema | list, register, describe, compatibility |
| Connect | list, create, describe, pause, resume |
| Cluster | status, nodes, elect-leader |
| Quotas | describe, set |
| Transport | status, shm-info |

### Global Options

```bash
surgewave <command> [options]

Options:
  -b, --bootstrap-servers  Broker address (default: localhost:9092)
  -f, --format             Output format: table, json, plain
  -v, --verbose            Verbose output
  --timeout                Request timeout (ms)
```

## Configuration

Create `~/.surgewave/config`:

```json
{
  "bootstrap_servers": "localhost:9092",
  "default_format": "table"
}
```

### Profiles

```bash
# Create profile
surgewave config profile create prod
surgewave config profile use prod
surgewave config set bootstrap_servers kafka.prod:9092

# Switch profiles
surgewave config profile use dev
```

## Shell Completion

```bash
# Bash
surgewave completion bash > ~/.surgewave/completion.bash
source ~/.surgewave/completion.bash

# Zsh
surgewave completion zsh > ~/.zfunc/_surgewave

# PowerShell
surgewave completion powershell >> $PROFILE

# Fish
surgewave completion fish > ~/.config/fish/completions/surgewave.fish
```

## Next Steps

- [CLI Reference](cli-reference.md) - Complete command reference
- [Quickstart](../quickstart/index.md) - Getting started
