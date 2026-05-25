# REST API Reference

All Surgewave REST endpoints. The HTTP API listens on port **9093** by default.

Base URL: `https://localhost:9093`

---

## Schema Registry

Confluent-compatible Schema Registry API. Enabled by default (`Surgewave:SchemaRegistry:Enabled=true`).

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/subjects` | List all subjects |
| `GET` | `/subjects/{subject}/versions` | List versions for a subject |
| `POST` | `/subjects/{subject}/versions` | Register a schema |
| `GET` | `/subjects/{subject}/versions/{version}` | Get schema by version number |
| `GET` | `/subjects/{subject}/versions/latest` | Get latest schema version |
| `DELETE` | `/subjects/{subject}` | Delete a subject (all versions) |
| `DELETE` | `/subjects/{subject}/versions/{version}` | Delete a specific version |
| `POST` | `/subjects/{subject}` | Look up schema (returns id if exists) |
| `GET` | `/schemas/ids/{id}` | Get schema by global ID |
| `GET` | `/schemas/ids/{id}/versions` | Get all subjects/versions for a schema ID |
| `GET` | `/schemas/types` | List supported schema types |
| `POST` | `/compatibility/subjects/{subject}/versions/{version}` | Check compatibility |
| `POST` | `/compatibility/subjects/{subject}/versions/latest` | Check compatibility against latest |
| `GET` | `/config` | Get global compatibility config |
| `PUT` | `/config` | Set global compatibility config |
| `GET` | `/config/{subject}` | Get per-subject compatibility |
| `PUT` | `/config/{subject}` | Set per-subject compatibility |
| `DELETE` | `/config/{subject}` | Delete per-subject compatibility |
| `GET` | `/mode` | Get global mode |
| `GET` | `/schemas/infer/{topic}` | Infer schema from topic messages |
| `POST` | `/schemas/infer/{topic}/register` | Infer and register schema |
| `GET` | `/schemas/inferred` | List inferred schemas |

### Schema Evolution (requires `Surgewave:SchemaEvolution:Enabled=true`)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/schema-evolution/changes` | List all schema evolution events |
| `GET` | `/api/schema-evolution/changes/{subject}` | Evolution history for a subject |
| `GET` | `/api/schema-evolution/report/{subject}/{version}` | Full evolution report |
| `GET` | `/api/schema-evolution/code/{subject}/{version}` | Generated migration code |
| `POST` | `/api/schema-evolution/analyze` | Analyze schema for breaking changes |
| `POST` | `/api/schema-evolution/generate-model` | Generate .NET model from schema |

### Schema Migration (requires `Surgewave:SchemaMigration:Enabled=true`)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/schema-migration/migrate` | Migrate a message between schema versions |
| `GET` | `/api/schema-migration/path/{subject}/{from}/{to}` | Get migration path between versions |
| `POST` | `/api/schema-migration/test` | Test a migration |
| `GET` | `/api/schema-migration/config` | Get migration configuration |
| `PUT` | `/api/schema-migration/config` | Update migration configuration |
| `GET` | `/api/schema-migration/cache/stats` | Migration cache statistics |

---

## Kafka Connect

Confluent Connect-compatible API. Requires `Surgewave:Connect:Enabled=true`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Connect cluster info |
| `GET` | `/connectors` | List all connectors |
| `POST` | `/connectors` | Create a connector |
| `GET` | `/connectors/{name}` | Get connector details |
| `DELETE` | `/connectors/{name}` | Delete a connector |
| `GET` | `/connectors/{name}/config` | Get connector configuration |
| `PUT` | `/connectors/{name}/config` | Update connector configuration |
| `GET` | `/connectors/{name}/status` | Get connector status |
| `POST` | `/connectors/{name}/restart` | Restart connector |
| `PUT` | `/connectors/{name}/pause` | Pause connector |
| `PUT` | `/connectors/{name}/resume` | Resume connector |
| `GET` | `/connectors/{name}/tasks` | List connector tasks |
| `POST` | `/connectors/{name}/tasks/{taskId}/restart` | Restart a specific task |
| `GET` | `/connector-plugins` | List available connector plugins |
| `GET` | `/connectors/{name}/offsets` | Get source connector offsets |
| `DELETE` | `/connectors/{name}/offsets` | Delete source connector offsets |
| `POST` | `/connectors/{name}/offsets/reset` | Reset source connector offsets |

---

## Pipelines

Visual pipeline designer API. Requires `Surgewave:Connect:Enabled=true`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/pipelines` | List all pipelines |
| `GET` | `/api/pipelines/{id}` | Get pipeline |
| `POST` | `/api/pipelines` | Create pipeline |
| `PUT` | `/api/pipelines/{id}` | Update pipeline |
| `DELETE` | `/api/pipelines/{id}` | Delete pipeline |
| `POST` | `/api/pipelines/{id}/start` | Start pipeline |
| `POST` | `/api/pipelines/{id}/stop` | Stop pipeline |
| `GET` | `/api/pipelines/{id}/status` | Pipeline runtime status |
| `GET` | `/api/pipelines/{id}/metrics` | Pipeline metrics |
| `GET` | `/api/pipelines/{id}/versions` | Version history |
| `GET` | `/api/pipelines/{id}/versions/{version}` | Get specific version |
| `GET` | `/api/pipelines/{id}/versions/{from}/diff/{to}` | Diff two versions |
| `POST` | `/api/pipelines/{id}/rollback/{version}` | Rollback to version |
| `POST` | `/api/pipelines/{id}/dry-run` | Dry-run (test without producing) |
| `GET` | `/api/pipelines/{id}/export` | Export pipeline as JSON |
| `POST` | `/api/pipelines/import` | Import pipeline from JSON |
| `GET` | `/api/pipelines/templates` | List pipeline templates |
| `GET` | `/api/pipelines/templates/{templateId}` | Get template |
| `POST` | `/api/pipelines/templates/{templateId}/create` | Create from template |
| `GET` | `/api/pipelines/{id}/executions` | List executions |
| `GET` | `/api/pipelines/{id}/executions/{executionId}` | Get execution |
| `GET` | `/api/pipelines/{id}/executions/stats` | Execution statistics |
| `GET` | `/api/pipelines/{id}/schedule` | Get pipeline schedule |
| `PUT` | `/api/pipelines/{id}/schedule` | Update pipeline schedule |
| `POST` | `/api/pipelines/{id}/analyze-changes` | Analyze hot-deploy impact |
| `POST` | `/api/pipelines/{id}/hot-deploy` | Hot-deploy updated pipeline |
| `POST` | `/api/pipelines/{id}/debug/breakpoints` | Set debug breakpoints |
| `DELETE` | `/api/pipelines/{id}/debug/breakpoints/{nodeId}` | Remove breakpoint |
| `GET` | `/api/pipelines/{id}/debug/state` | Get debug state |
| `POST` | `/api/pipelines/{id}/debug/step/{nodeId}` | Step through node |
| `POST` | `/api/pipelines/{id}/debug/resume/{nodeId}` | Resume from node |
| `POST` | `/api/pipelines/{id}/debug/resume` | Resume all paused nodes |
| `POST` | `/api/pipelines/{id}/provenance/enable` | Enable data lineage tracking |
| `POST` | `/api/pipelines/{id}/provenance/disable` | Disable data lineage tracking |
| `GET` | `/api/pipelines/{id}/ports` | Get pipeline I/O ports |
| `GET` | `/api/connectors` | List connector types |
| `GET` | `/api/connectors/{type}/config` | Get connector config schema |

---

## Partition Operations

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/partitions/reassign` | Submit reassignment plan |
| `GET` | `/api/partitions/reassign/{planId}` | Get plan status |
| `DELETE` | `/api/partitions/reassign/{planId}` | Cancel reassignment plan |
| `GET` | `/api/partitions/reassign` | List all plans |
| `POST` | `/api/partitions/balance` | Trigger manual balance |
| `POST` | `/api/partitions/decommission/{brokerId}` | Decommission a broker |
| `GET` | `/api/partitions/assignment` | Get current partition assignment |
| `POST` | `/api/partitions/reassign/validate` | Validate a reassignment plan |

---

## Cluster Operations

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/cluster/version` | Get cluster version info |
| `POST` | `/api/cluster/upgrade/check` | Check upgrade feasibility |
| `POST` | `/api/cluster/upgrade/prepare` | Prepare for rolling upgrade |
| `POST` | `/api/cluster/broker/shutdown/graceful` | Initiate graceful broker shutdown |
| `GET` | `/api/cluster/broker/shutdown/status` | Get shutdown status |

---

## Interactive Queries (Streams)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/streams/stores` | List all state stores |
| `GET` | `/api/streams/stores/{name}` | Get store metadata |
| `GET` | `/api/streams/stores/{name}/entries` | List entries (offset, limit params) |
| `GET` | `/api/streams/stores/{name}/entries/{key}` | Get entry by key |
| `GET` | `/api/streams/stores/{name}/count` | Get entry count |

---

## QueueView

Requires `Surgewave:QueueView:Enabled=true`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/queue/topics` | List enrolled topics |
| `GET` | `/api/queue/{topic}/status` | Queue status for topic |
| `POST` | `/api/queue/{topic}/purge` | Purge in-flight state (log untouched) |
| `GET` | `/api/queue/{topic}/inflight` | List in-flight messages |
| `GET` | `/api/queue/{topic}/dlq` | Browse DLQ messages (offset, limit) |
| `GET` | `/api/queue/{topic}/metrics` | QueueView metrics |

---

## Message Browser

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/messages/{topic}/{partition}` | Browse messages (offset, limit params) |
| `GET` | `/admin/messages/{topic}/{partition}/{offset}` | Get single message |
| `GET` | `/admin/messages/{topic}/{partition}/download/{offset}` | Download message payload |

---

## Quotas (Admin)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/quotas/config` | Get quota configuration |
| `PUT` | `/admin/quotas/config` | Update quota configuration |
| `GET` | `/admin/quotas/clients` | List all client quota stats |
| `GET` | `/admin/quotas/clients/{clientId}` | Get quota stats for client |

---

## Bandwidth Quotas

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/quotas/bandwidth` | List all bandwidth quotas |
| `GET` | `/api/quotas/bandwidth/{clientId}` | Get quota for client |
| `PUT` | `/api/quotas/bandwidth/client/{clientId}` | Set client bandwidth quota |
| `PUT` | `/api/quotas/bandwidth/user/{user}` | Set user bandwidth quota |
| `DELETE` | `/api/quotas/bandwidth/client/{clientId}` | Remove client quota |
| `GET` | `/api/quotas/bandwidth/metrics` | Bandwidth quota metrics |
| `GET` | `/api/quotas/bandwidth/config` | Get bandwidth quota config |

---

## ACLs

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/acls` | List all ACLs |
| `GET` | `/admin/acls/filter` | Filter ACLs by principal/resource/operation |
| `POST` | `/admin/acls` | Create an ACL |
| `POST` | `/admin/acls/batch` | Create multiple ACLs |
| `DELETE` | `/admin/acls` | Delete ACLs matching filter |

---

## Audit Logging

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/audit` | Browse audit log (filtered) |
| `GET` | `/admin/audit/stats` | Audit log statistics |
| `GET` | `/admin/audit/config` | Get audit configuration |

---

## Cross-Topic Transactions

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/transactions` | Begin a transaction |
| `POST` | `/api/transactions/{id}/write` | Add writes to transaction |
| `POST` | `/api/transactions/{id}/commit` | Commit transaction |
| `POST` | `/api/transactions/{id}/abort` | Abort transaction |
| `GET` | `/api/transactions` | List active transactions |
| `GET` | `/api/transactions/{id}` | Get transaction state |

---

## SQL Query Engine

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/sql/execute` | Execute a SQL query (synchronous) |
| `POST` | `/api/sql/queries` | Create a streaming SQL query |
| `GET` | `/api/sql/queries` | List active streaming queries |
| `DELETE` | `/api/sql/queries/{id}` | Stop a streaming query |

---

## Auto-Tuning

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/auto-tuning/status` | Current recommendations |
| `GET` | `/api/auto-tuning/history` | Recommendation history |
| `POST` | `/api/auto-tuning/apply/{ruleId}` | Apply a recommendation |

---

## Cruise Control

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/cruise-control/status` | Current balance status |
| `GET` | `/api/cruise-control/history` | Rebalance history |
| `POST` | `/api/cruise-control/analyze` | Trigger analysis now |
| `POST` | `/api/cruise-control/apply` | Apply balance suggestion |
| `GET` | `/api/cruise-control/config` | Get Cruise Control config |
| `PUT` | `/api/cruise-control/config` | Update Cruise Control config |

---

## Intent-Based Configuration

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/intent/resolve` | Resolve intent to configuration |
| `POST` | `/api/intent/create` | Create topic from intent |
| `GET` | `/api/intent/keywords` | List supported intent keywords |
| `GET` | `/api/intent/rules` | List resolution rules |

---

## Multi-Tenancy

Requires `Surgewave:MultiTenancy:Enabled=true`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/tenants` | List all tenants |
| `POST` | `/api/tenants` | Create tenant |
| `GET` | `/api/tenants/{id}` | Get tenant |
| `PUT` | `/api/tenants/{id}` | Update tenant |
| `DELETE` | `/api/tenants/{id}` | Delete tenant |
| `GET` | `/api/tenants/{id}/quota` | Get tenant quota usage |
| `GET` | `/api/tenants/{id}/namespaces` | List namespaces |
| `POST` | `/api/tenants/{id}/namespaces` | Create namespace |
| `GET` | `/api/tenants/{id}/namespaces/{ns}` | Get namespace |
| `PUT` | `/api/tenants/{id}/namespaces/{ns}/policy` | Update namespace policy |
| `DELETE` | `/api/tenants/{id}/namespaces/{ns}` | Delete namespace |
| `GET` | `/api/tenants/{id}/namespaces/{ns}/topics` | List topics in namespace |

---

## CDC (Change Data Capture)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/cdc/sources` | Create a CDC source |
| `GET` | `/api/cdc/sources` | List CDC sources |
| `GET` | `/api/cdc/sources/{id}/status` | Get CDC source status |
| `DELETE` | `/api/cdc/sources/{id}` | Remove CDC source |

---

## Data Mesh

Requires `Surgewave:DataMesh:Enabled=true`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/data-mesh/products` | List data products |
| `POST` | `/api/data-mesh/products` | Register a data product |
| `GET` | `/api/data-mesh/products/{id}` | Get data product |
| `PUT` | `/api/data-mesh/products/{id}` | Update data product |
| `POST` | `/api/data-mesh/products/{id}/publish` | Publish data product |
| `POST` | `/api/data-mesh/products/{id}/deprecate` | Deprecate data product |
| `DELETE` | `/api/data-mesh/products/{id}` | Delete data product |
| `GET` | `/api/data-mesh/products/{id}/quality` | Get quality metrics |
| `GET` | `/api/data-mesh/products/{id}/lineage` | Get data lineage |
| `POST` | `/api/data-mesh/products/{id}/validate` | Validate data contract |
| `GET` | `/api/data-mesh/search` | Search data products |
| `GET` | `/api/data-mesh/domains` | List data domains |

---

## ML Scoring

Requires `Surgewave:ML:Enabled=true`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/ml/models` | List loaded models |
| `POST` | `/api/ml/models/load` | Load an ONNX model |
| `DELETE` | `/api/ml/models/{id}` | Unload a model |
| `POST` | `/api/ml/models/{id}/score` | Score input data |
| `GET` | `/api/ml/models/discover` | Discover models from `ModelsDirectory` |

---

## Privacy

Requires `Surgewave:Privacy:Enabled=true`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/privacy/policies` | List privacy policies |
| `POST` | `/api/privacy/policies` | Create privacy policy |
| `PUT` | `/api/privacy/policies/{index}` | Update privacy policy |
| `DELETE` | `/api/privacy/policies/{index}` | Delete privacy policy |
| `GET` | `/api/privacy/status` | Get all topic privacy statuses |
| `GET` | `/api/privacy/status/{topic}` | Get privacy status for topic |
| `POST` | `/api/privacy/scan/{topic}` | Trigger PII scan on topic |
| `POST` | `/api/privacy/erasure` | Submit right-to-erasure request |
| `GET` | `/api/privacy/audit` | Browse privacy audit events |
| `GET` | `/api/privacy/report` | Generate compliance report |

---

## WASM Plugins

Requires `Surgewave:Wasm:Enabled=true`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/wasm/plugins` | List loaded plugins |
| `POST` | `/api/wasm/plugins/load` | Load a WASM plugin |
| `POST` | `/api/wasm/plugins/{id}/reload` | Reload plugin |
| `POST` | `/api/wasm/plugins/{id}/stop` | Stop plugin |
| `DELETE` | `/api/wasm/plugins/{id}` | Unload plugin |
| `GET` | `/api/wasm/plugins/{id}/metrics` | Plugin metrics |
| `GET` | `/api/wasm/plugins/discover` | Discover plugins from `WasmDirectory` |

---

## Cluster Linking & Geo-Replication

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/cluster-links` | Create cluster link |
| `GET` | `/api/cluster-links` | List cluster links |
| `GET` | `/api/cluster-links/{id}` | Get cluster link |
| `PUT` | `/api/cluster-links/{id}/pause` | Pause cluster link |
| `PUT` | `/api/cluster-links/{id}/resume` | Resume cluster link |
| `DELETE` | `/api/cluster-links/{id}` | Delete cluster link |
| `POST` | `/api/cluster-links/{id}/mirrors` | Add mirror topic |
| `GET` | `/api/cluster-links/{id}/mirrors` | List mirror topics |
| `GET` | `/api/mirrors` | List all mirrors |
| `GET` | `/api/mirrors/{topic}` | Get mirror details |
| `POST` | `/api/mirrors/{topic}/stop` | Stop mirroring |
| `POST` | `/api/mirrors/{topic}/promote` | Promote mirror to primary |
| `GET` | `/api/mirrors/{topic}/lag` | Get mirror lag |
| `GET` | `/api/replication/status` | Geo-replication status |
| `GET` | `/api/replication/clusters` | List remote clusters |
| `POST` | `/api/replication/clusters` | Add remote cluster |
| `DELETE` | `/api/replication/clusters/{id}` | Remove remote cluster |
| `GET` | `/api/replication/rules` | List replication rules |
| `POST` | `/api/replication/rules` | Add replication rule |
| `GET` | `/api/replication/metrics` | Replication metrics |
| `GET` | `/api/replication/conflicts` | List replication conflicts |

---

## Observability

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/metrics` | Prometheus metrics endpoint |
| `GET` | `/sd-targets` | Prometheus HTTP service discovery targets |
| `GET` | `/health` | Health check endpoint |
| `GET` | `/health/ready` | Readiness probe |
| `GET` | `/health/live` | Liveness probe |
