# CLI Reference

Complete reference for all Surgewave CLI commands.

## Topics

### List Topics

```bash
surgewave topics list
surgewave topics list -f json
```

### Create Topic

```bash
surgewave topics create my-topic
surgewave topics create my-topic --partitions 3 --replication-factor 2
surgewave topics create my-topic -p 6 -r 3
```

### Describe Topic

```bash
surgewave topics describe my-topic
surgewave topics describe my-topic -f json
```

### Delete Topic

```bash
surgewave topics delete my-topic
```

### Alter Config

```bash
surgewave topics alter-config my-topic --set retention.ms=86400000
surgewave topics alter-config my-topic --delete cleanup.policy
```

### Describe Config

```bash
surgewave topics describe-config my-topic
```

---

## Produce Messages

### Single Message

```bash
surgewave produce my-topic --value "Hello, World!"
surgewave produce my-topic -k "key1" -m "value1"
surgewave produce my-topic --key user-123 --value '{"action": "login"}'
```

### Piped Input

```bash
echo "message1" | surgewave produce my-topic
cat messages.txt | surgewave produce my-topic
echo "key1:value1" | surgewave produce my-topic --parse-key
```

### Interactive Mode

```bash
surgewave produce my-topic --interactive
# Type messages, Ctrl+D to exit
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--key` | `-k` | Message key |
| `--value` | `-m` | Message value |
| `--partition` | `-p` | Target partition |
| `--interactive` | `-i` | Interactive mode |
| `--separator` | `-s` | Key-value separator |
| `--parse-key` | | Parse key from input |

---

## Consume Messages

### Basic

```bash
surgewave consume my-topic
surgewave consume my-topic --offset earliest
surgewave consume my-topic -o latest -n 100
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--offset` | `-o` | Start offset: earliest, latest, number |
| `--partition` | `-p` | Partition to consume |
| `--max-messages` | `-n` | Max messages (-1 unlimited) |
| `--keys` | `-k` | Show message keys |
| `--timestamps` | `-t` | Show timestamps |
| `--print-offset` | | Print offset |

### Examples

```bash
# From beginning, limit 10
surgewave consume events --offset earliest --max-messages 10

# With timestamps, JSON output
surgewave consume logs -t -f json

# Pipe to jq
surgewave consume events -f plain | jq -r '.data'
```

---

## Consumer Groups

### List Groups

```bash
surgewave groups list
surgewave groups list -f json
```

### Describe Group

```bash
surgewave groups describe my-consumer-group
```

### Delete Group

```bash
surgewave groups delete my-consumer-group
```

---

## Broker Operations

### Broker Info

```bash
surgewave broker info
surgewave broker info -f json
```

### Health Check

```bash
surgewave health
surgewave health -f json
```

### Diagnose

```bash
surgewave diagnose
```

### Broker Config

```bash
surgewave broker config describe
surgewave broker config describe --all
surgewave broker config alter --set log.retention.hours=72
```

---

## ACL Management

### List ACLs

```bash
surgewave acls list
surgewave acls list --principal User:alice
surgewave acls list --resource-type topic --resource my-topic
```

### Add ACL

```bash
surgewave acls add \
    --principal User:alice \
    --resource-type topic \
    --resource my-topic \
    --operation read \
    --permission allow

surgewave acls add \
    --principal User:producer \
    --resource-type topic \
    --resource "*" \
    --operation write
```

### Remove ACL

```bash
surgewave acls remove --principal User:alice --resource-type topic --resource my-topic
```

### Options

| Option | Description |
|--------|-------------|
| `--principal` | Principal (e.g., User:alice) |
| `--resource-type` | topic, group, cluster, transactional-id |
| `--resource` | Resource name (* for all) |
| `--operation` | read, write, create, delete, alter, describe, all |
| `--permission` | allow, deny |
| `--host` | Client host (* for all) |
| `--pattern-type` | literal, prefixed |

---

## Schema Registry

### List Subjects

```bash
surgewave schema list
surgewave schema list --include-deleted
```

### Register Schema

```bash
surgewave schema register user-value --schema '{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}'
surgewave schema register user-value --file user.avsc --type AVRO
surgewave schema register events-value --file events.proto --type PROTOBUF
```

### Describe Subject

```bash
surgewave schema describe user-value
```

### Get Schema

```bash
surgewave schema get --id 1
surgewave schema get --subject user-value --version latest
```

### Compatibility

```bash
surgewave schema compatibility check user-value --file new-user.avsc
surgewave schema compatibility get --subject user-value
surgewave schema compatibility set BACKWARD --subject user-value
```

### Delete

```bash
surgewave schema delete-subject user-value
surgewave schema delete-version user-value 1
```

---

## Kafka Connect

### List Connectors

```bash
surgewave connect list
```

### Create Connector

```bash
surgewave connect create file-source --config '{"connector.class":"FileStreamSourceConnector","file":"/var/log/app.log","topic":"logs"}'
surgewave connect create db-source --file connector-config.json
```

### Connector Operations

```bash
surgewave connect describe my-connector
surgewave connect status my-connector
surgewave connect pause my-connector
surgewave connect resume my-connector
surgewave connect restart my-connector
surgewave connect delete my-connector
```

### Tasks

```bash
surgewave connect tasks list my-connector
surgewave connect tasks restart my-connector 0
```

### Plugins

```bash
surgewave connect plugins
```

---

## Cluster Operations

### Status

```bash
surgewave cluster status
surgewave cluster nodes
```

### Partition Operations

```bash
surgewave partitions elect-leader --topic my-topic
surgewave partitions elect-leader --all
surgewave partitions elect-leader --topic my-topic --partitions 0,1,2
```

---

## Cross-Cluster Replication (Mirror)

MirrorMaker 2.0 compatible cross-cluster replication.

### Create Replication Flow

```bash
surgewave mirror create \
    --name dc1-to-dc2 \
    --source-alias dc1 \
    --source-servers kafka-dc1:9092 \
    --target-alias dc2 \
    --target-servers kafka-dc2:9092

# With topic filtering
surgewave mirror create \
    --name dc1-to-dc2 \
    --source-alias dc1 \
    --source-servers kafka-dc1:9092 \
    --target-alias dc2 \
    --target-servers kafka-dc2:9092 \
    --topics "orders.*" \
    --tasks 8
```

### Options

| Option | Description |
|--------|-------------|
| `--name` | Name for the replication flow |
| `--source-alias` | Alias for source cluster |
| `--source-servers` | Bootstrap servers for source |
| `--target-alias` | Alias for target cluster |
| `--target-servers` | Bootstrap servers for target |
| `--topics` | Regex pattern for topics (default: `.*`) |
| `--topics-whitelist` | Comma-separated whitelist |
| `--topics-blacklist` | Comma-separated blacklist |
| `--tasks` | Number of parallel tasks (default: 4) |
| `--sync-offsets` | Sync consumer group offsets |
| `--emit-heartbeats` | Emit health heartbeats |

### List & Describe

```bash
surgewave mirror list
surgewave mirror describe dc1-to-dc2
```

### Status & Monitoring

```bash
surgewave mirror status dc1-to-dc2
surgewave mirror status dc1-to-dc2 --watch
```

### Pause & Resume

```bash
surgewave mirror pause dc1-to-dc2
surgewave mirror resume dc1-to-dc2
```

### Consumer Group Failover

```bash
# Preview failover (dry run)
surgewave mirror failover \
    --group my-consumer-group \
    --source dc1 \
    --target dc2 \
    --dry-run

# Execute failover
surgewave mirror failover \
    --group my-consumer-group \
    --source dc1 \
    --target dc2 \
    --force
```

### Delete

```bash
surgewave mirror delete dc1-to-dc2
```

---

## Message Operations

### message get

Fetch a single message by topic, partition, and offset.

```bash
surgewave message get <topic> <offset> [options]
```

| Option | Description |
|--------|-------------|
| `--partition, -p` | Partition (default: 0) |
| `--output-format` | Output: raw, json, hex, base64 (default: raw) |
| `--output, -o` | Write payload to file |
| `--headers` | Include headers |
| `--decode` | Auto-detect and decode format |

**Examples:**
```bash
# Pipe to jq
surgewave message get orders 5 | jq .

# Save to file
surgewave message get orders 5 --output message.bin

# JSON envelope with metadata
surgewave message get orders 5 --output-format json

# Hex dump
surgewave message get orders 5 --output-format hex
```

---

## Copy Messages

Copy messages between topics efficiently.

```bash
# Basic copy
surgewave copy source-topic destination-topic

# With options
surgewave copy source-topic destination-topic \
    --offset earliest \
    --max-messages 10000

# Dry run to preview
surgewave copy source-topic destination-topic --dry-run
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--offset` | `-o` | Start offset: earliest, latest, number |
| `--source-partition` | `-sp` | Source partition (default: 0) |
| `--dest-partition` | `-dp` | Destination partition (default: 0) |
| `--max-messages` | `-n` | Max messages to copy (-1 for all) |
| `--preserve-keys` | `-k` | Preserve message keys (default: true) |
| `--key` | | Override all message keys |
| `--dry-run` | | Preview without copying |
| `--batch-size` | | Messages per batch (default: 100) |

---

## Log Operations

### Compaction Status

```bash
surgewave logs compaction-status
surgewave logs compaction-status -f json
```

### Trigger Compaction

```bash
surgewave logs compact
surgewave logs compact --force
```

---

## Transactions

```bash
surgewave transactions list
surgewave transactions describe <transaction-id>
```

---

## Quotas

```bash
surgewave quotas describe
surgewave quotas set --user alice --producer-rate 10485760
```

---

## Transport

```bash
surgewave transport status
surgewave transport shm-info
surgewave transport shm-diagnostics --port 9092
surgewave transport shm-cleanup --dry-run
```

---

## Benchmarking

```bash
surgewave benchmark --messages 100000 --size 1024
surgewave benchmark -n 1000000 -s 100 --batch 1000
```

---

## Configuration

```bash
surgewave config profile create prod
surgewave config profile use prod
surgewave config set bootstrap_servers kafka.prod:9092
surgewave config get bootstrap_servers
```

---

## Shell Completion

Generate shell completion scripts for command auto-completion.

```bash
# Bash
surgewave completion bash > ~/.surgewave-completion.bash
source ~/.surgewave-completion.bash

# Zsh
surgewave completion zsh > ~/.surgewave-completion.zsh
source ~/.surgewave-completion.zsh

# PowerShell
surgewave completion powershell | Out-String | Invoke-Expression

# Fish
surgewave completion fish > ~/.config/fish/completions/surgewave.fish
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Connection error |
| 3 | Authentication error |
| 4 | Authorization error |

---

## Troubleshooting

### Connection Issues

```bash
surgewave diagnose
surgewave topics list -v
```

### Common Errors

| Error | Solution |
|-------|----------|
| "Connection refused" | Check broker is running |
| "Topic not found" | Create topic or enable auto-create |
| "Authentication failed" | Check credentials |
| "Not authorized" | Verify ACLs |
