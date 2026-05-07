# PostgreSQL Wire Protocol Plugin

`kuestenlogik.surgewave.protocol.postgresql` — speaks the PostgreSQL wire protocol so
psql, pgAdmin, JDBC, npgsql, asyncpg and any other PostgreSQL client can
connect to a Surgewave broker and run SQL queries against topics. Powered by
the materialised-view subsystem in `Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews`.

## Installation

```bash
surgewave plugin install kuestenlogik.surgewave.protocol.postgresql-<version>.swpkg
```

## Configuration

Section: `Surgewave:PostgreSql`. Every field has a recommended default in
`pluginsettings.json`.

| Field | Default | Description |
|---|---|---|
| `Enabled` | `false` | Master switch. Set to `true` to start the PostgreSQL wire-protocol listener. |
| `Port` | `5432` | TCP port. Use a non-default if you also run a real PostgreSQL on the same host. |
| `RequirePassword` | `false` | Require cleartext password authentication. When `false`, all connections are accepted. |
| `Password` | (none) | Password for cleartext authentication. Required when `RequirePassword` is `true`. |
| `MaxConnections` | `100` | Maximum concurrent PostgreSQL client connections. |
| `ServerVersion` | `"16.0"` | Version string reported to clients. Some tools (pgAdmin, JDBC drivers) gate features on this; report whatever your tools expect. |

### Minimal config

```json
{
  "Surgewave": {
    "PostgreSql": { "Enabled": true }
  }
}
```

The PostgreSQL listener starts on port 5432 with no authentication. You can
connect with `psql -h broker -p 5432 -U anyone` immediately and query topics.

### Production-ish config

```json
{
  "Surgewave": {
    "PostgreSql": {
      "Enabled": true,
      "Port": 5432,
      "RequirePassword": true,
      "Password": "use-a-secret-here",
      "MaxConnections": 500,
      "ServerVersion": "16.0"
    }
  }
}
```

## SQL surface

Topics are queryable as tables. Materialised views (created via
`CREATE MATERIALIZED VIEW name AS SELECT ...`) are persistently
re-evaluated by the broker's background refresh loop and can be queried
back via `SELECT * FROM name`. See the
[materialised views](../streams.md) documentation for the SQL dialect
and refresh semantics.

```sql
-- Connect: psql -h broker -p 5432 -U anyone
SELECT * FROM orders WHERE total > 100 LIMIT 10;
CREATE MATERIALIZED VIEW big_orders AS SELECT * FROM orders WHERE total > 100;
SELECT * FROM big_orders;
```

## Authentication

The protocol plugin currently supports cleartext password auth only. SCRAM
and certificate-based auth are roadmap items. For production, run the
broker behind a TLS-terminating proxy or VPN, set `RequirePassword: true`,
and store the password as a secret (env var with
`Surgewave__PostgreSql__Password=...`).

## Operations

```bash
surgewave plugin show kuestenlogik.surgewave.protocol.postgresql
surgewave config view appsettings.json --explain
surgewave config validate appsettings.json
```

## Reference

- Source: `src/Kuestenlogik.Surgewave.Protocol.PostgreSql/`
- Config class: `PostgreSqlConfig.cs` (note the cross-property check:
  `RequirePassword` requires a non-empty `Password`)
- Materialised views: `src/Kuestenlogik.Surgewave.Streams/Sql/MaterializedViews/`
