# Schema Migration

Zero-downtime schema migration with automatic on-read and on-write message transformation.

## Overview

Schema Migration transparently transforms messages between schema versions during produce and fetch operations. When a consumer expects schema v3 but encounters a message written with schema v1, the `SchemaMigrationInterceptor` automatically converts the message on the fly. No consumer restarts required.

Key characteristics:

- **On-read migration**: Transform messages during fetch to match the consumer's expected schema
- **On-write migration**: Optionally upgrade produced messages to the latest schema version
- **Configurable strategies**: MissingField, ExtraField, and TypeMismatch each have independent strategies
- **LRU-cached migrators**: Compiled migrator functions are cached for high throughput
- **Step-by-step paths**: Multi-version jumps are decomposed into incremental migration steps

## How It Works

1. A message arrives at the broker tagged with its schema subject and version.
2. On fetch, the `SchemaMigrationInterceptor` compares the message's version with the consumer's expected version.
3. If they differ, the `SchemaMigrationCache` is checked for a pre-compiled migrator.
4. On cache miss, the `SchemaMigrator` builds a new migrator by extracting JSON Schema properties from both versions and creating a transformation function.
5. The compiled migrator handles field additions (defaults), removals (drop/keep), type coercion, and nested objects recursively.

```mermaid
sequenceDiagram
    participant Consumer as Consumer (expects v3)
    participant Broker
    participant Storage

    Consumer->>Broker: Fetch
    Broker->>Storage: Read message (v1)
    Storage-->>Broker: raw bytes
    Note over Broker: SchemaMigrationInterceptor<br/>v1 &rarr; v3 migration<br/>(cached migrator)
    Broker-->>Consumer: migrated msg
```

## Configuration

```json
{
  "Surgewave": {
    "SchemaMigration": {
      "Enabled": true,
      "AutoMigrateOnRead": true,
      "AutoMigrateOnWrite": false,
      "MissingFieldStrategy": "UseDefault",
      "ExtraFieldStrategy": "Ignore",
      "TypeMismatchStrategy": "Coerce",
      "MaxCachedMigrators": 100
    }
  }
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Enabled` | bool | `false` | Enable schema migration |
| `AutoMigrateOnRead` | bool | `true` | Migrate messages on fetch to consumer's schema |
| `AutoMigrateOnWrite` | bool | `false` | Migrate produced messages to latest schema |
| `MissingFieldStrategy` | enum | `UseDefault` | Strategy for fields in target but not in source |
| `ExtraFieldStrategy` | enum | `Ignore` | Strategy for fields in source but not in target |
| `TypeMismatchStrategy` | enum | `Coerce` | Strategy for type differences |
| `MaxCachedMigrators` | int | `100` | LRU cache size for compiled migrators |

### Strategy Options

**MissingFieldStrategy** -- when the target schema has a field the message lacks:

| Strategy | Behavior |
|----------|----------|
| `UseDefault` | Insert the type's default value (0, "", false, null) |
| `UseNull` | Insert null (requires nullable target type) |
| `Fail` | Reject the migration if a required field is missing |

**ExtraFieldStrategy** -- when the message has a field not in the target schema:

| Strategy | Behavior |
|----------|----------|
| `Ignore` | Drop extra fields silently |
| `Include` | Keep extra fields in the output |
| `Fail` | Reject the migration if extra fields exist |

**TypeMismatchStrategy** -- when a field type changed between versions:

| Strategy | Behavior |
|----------|----------|
| `Coerce` | Attempt automatic type coercion (int to string, etc.) |
| `UseDefault` | Use the target type's default value |
| `Fail` | Reject the migration on type mismatch |

## Usage

### Register Schema Versions

```bash
# v1: name and email
curl -X POST http://localhost:9092/api/schema-registry/subjects/user-value/versions \
  -H "Content-Type: application/json" \
  -d '{"schema": "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"}}}"}'

# v2: added "age" field
curl -X POST http://localhost:9092/api/schema-registry/subjects/user-value/versions \
  -H "Content-Type: application/json" \
  -d '{"schema": "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"age\":{\"type\":\"integer\"}}}"}'
```

Messages written with v1 (`{"name":"Alice","email":"alice@example.com"}`) are automatically migrated when a v2 consumer fetches them:

```json
{
  "name": "Alice",
  "email": "alice@example.com",
  "age": 0
}
```

## Architecture

### SchemaMigrator

The core transformation engine. Parses JSON Schemas, computes field diffs, and builds compiled migrator functions (`Func<byte[], byte[]>`) that can be cached and reused.

### SchemaMigrationCache

An LRU cache keyed by `(subject, fromVersion, toVersion)`. When the cache is full, the least-recently-used migrator is evicted.

### TypeCoercer

Handles automatic type coercion between JSON types: `int` to `string`, `string` to `int`, `boolean` to `int`, etc. Used when `TypeMismatchStrategy` is `Coerce`.

### SchemaMigrationInterceptor

Hooks into the broker's produce and fetch paths. Transparently applies migration without requiring any client-side changes.

## Use Cases

- **Rolling upgrades**: Deploy new consumer versions without waiting for producers to update
- **Schema evolution**: Add/remove fields without downtime or message reprocessing
- **Multi-version consumers**: Different consumer groups can read the same topic with different schema versions
- **Legacy compatibility**: Old producers continue writing old schemas while new consumers read new ones

## Next Steps

- [Schema Registry](schema-registry.md) - Schema management basics
- [Schema Linking](schema-linking.md) - Cross-cluster schema synchronization
- [Transactions](transactions.md) - Exactly-once semantics
