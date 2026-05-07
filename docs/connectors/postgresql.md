# PostgreSQL CDC Connector

The PostgreSQL connector provides Change Data Capture (CDC) using logical replication, enabling real-time streaming of database changes to Surgewave topics.

## Overview

- **Source**: Capture INSERT, UPDATE, DELETE changes via logical replication
- **Sink**: Write records to PostgreSQL tables with upsert support

**Use Cases:**
- Real-time database replication
- Event sourcing from PostgreSQL
- Data synchronization across systems
- Audit logging and compliance

## Quick Start

### PostgreSQL CDC Source

Capture changes from PostgreSQL:

```json
{
  "name": "postgres-cdc",
  "config": {
    "connector.class": "PostgresCdcSourceConnector",
    "postgres.host": "localhost",
    "postgres.port": "5432",
    "postgres.database": "mydb",
    "postgres.user": "replication_user",
    "postgres.password": "secret",
    "postgres.tables": "public.users,public.orders",
    "topic.prefix": "postgres"
  }
}
```

### PostgreSQL Sink

Write records to PostgreSQL:

```json
{
  "name": "postgres-sink",
  "config": {
    "connector.class": "PostgresSinkConnector",
    "postgres.host": "localhost",
    "postgres.port": "5432",
    "postgres.database": "mydb",
    "postgres.user": "app_user",
    "postgres.password": "secret",
    "topics": "events",
    "postgres.table.name": "event_log",
    "write.mode": "upsert"
  }
}
```

## Configuration Reference

### Connection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `postgres.host` | string | Required | PostgreSQL host |
| `postgres.port` | int | `5432` | PostgreSQL port |
| `postgres.database` | string | Required | Database name |
| `postgres.user` | string | Required | Username |
| `postgres.password` | password | Required | Password |

### CDC Source Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `postgres.tables` | string | Required | Tables to capture (comma-separated) |
| `topic.prefix` | string | Required | Topic name prefix |
| `postgres.slot.name` | string | `surgewave_slot` | Replication slot name |
| `postgres.publication.name` | string | `surgewave_pub` | Publication name |
| `snapshot.mode` | string | `initial` | Snapshot: `initial`, `never`, `always` |
| `poll.interval.ms` | long | `1000` | Polling interval |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Source Surgewave topics |
| `postgres.table.name` | string | - | Target table (or use `${topic}`) |
| `write.mode` | string | `insert` | Mode: `insert`, `upsert`, `update` |
| `batch.size` | int | `1000` | Records per batch |
| `pk.fields` | string | - | Primary key fields for upsert |

## PostgreSQL Setup

### Enable Logical Replication

Edit `postgresql.conf`:
```properties
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Edit `pg_hba.conf`:
```
host    replication     replication_user    0.0.0.0/0    md5
```

### Create Replication User

```sql
CREATE ROLE replication_user WITH REPLICATION LOGIN PASSWORD 'secret';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
```

### Create Publication

```sql
-- All tables
CREATE PUBLICATION surgewave_pub FOR ALL TABLES;

-- Specific tables
CREATE PUBLICATION surgewave_pub FOR TABLE users, orders;
```

## Output Format

CDC events use Debezium-compatible JSON format:

```json
{
  "op": "u",
  "before": {
    "id": 1,
    "name": "John",
    "email": "john@old.com"
  },
  "after": {
    "id": 1,
    "name": "John",
    "email": "john@new.com"
  },
  "source": {
    "schema": "public",
    "table": "users",
    "lsn": "0/1234567"
  },
  "ts_ms": 1704067200000
}
```

### Operation Types

| Op | Description |
|----|-------------|
| `c` | Create (INSERT) |
| `u` | Update (UPDATE) |
| `d` | Delete (DELETE) |
| `r` | Read (snapshot) |

## Snapshot Modes

### Initial (Default)

Takes an initial snapshot of existing data, then streams changes:

```json
{
  "snapshot.mode": "initial"
}
```

### Never

Only capture changes after connector starts:

```json
{
  "snapshot.mode": "never"
}
```

### Always

Take a new snapshot on every connector restart:

```json
{
  "snapshot.mode": "always"
}
```

## Examples

### Full CDC Pipeline

Capture all changes from multiple tables:

```json
{
  "name": "full-cdc",
  "config": {
    "connector.class": "PostgresCdcSourceConnector",
    "postgres.host": "db.example.com",
    "postgres.port": "5432",
    "postgres.database": "production",
    "postgres.user": "cdc_user",
    "postgres.password": "secret",
    "postgres.tables": "public.users,public.orders,public.products",
    "topic.prefix": "db.changes",
    "snapshot.mode": "initial",
    "postgres.slot.name": "surgewave_cdc_slot"
  }
}
```

Topics created:
- `db.changes.public.users`
- `db.changes.public.orders`
- `db.changes.public.products`

### Sink with Upsert

Synchronize data with upsert semantics:

```json
{
  "name": "sync-to-replica",
  "config": {
    "connector.class": "PostgresSinkConnector",
    "postgres.host": "replica.example.com",
    "postgres.database": "replica_db",
    "postgres.user": "writer",
    "postgres.password": "secret",
    "topics": "db.changes.public.users",
    "postgres.table.name": "users",
    "write.mode": "upsert",
    "pk.fields": "id",
    "batch.size": "500"
  }
}
```

## LSN Tracking

The connector tracks Log Sequence Numbers (LSN) for offset management:

- Offsets stored in Surgewave's offset storage
- Automatic resume from last committed LSN
- Prevents duplicate processing

## Troubleshooting

### Common Issues

**Replication Slot Errors**
```sql
-- Check existing slots
SELECT * FROM pg_replication_slots;

-- Drop stuck slot
SELECT pg_drop_replication_slot('surgewave_slot');
```

**Permission Denied**
- Ensure user has `REPLICATION` privilege
- Grant `SELECT` on tables to capture
- Check `pg_hba.conf` allows replication connections

**WAL Retention**
- Monitor `pg_current_wal_lsn()` vs slot's `restart_lsn`
- If slot falls behind, WAL accumulates
- Set `max_slot_wal_keep_size` in PostgreSQL 13+

### Monitoring Replication Lag

```sql
SELECT
  slot_name,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots;
```

## See Also

- [MongoDB Connector](mongodb.md)
- [Generic Database Connector](database.md)
- [Custom Connectors](custom-connectors.md)
