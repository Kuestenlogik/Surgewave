# Surgewave vs. Spark / Lakehouse / Data Platform — Lückenanalyse

> **Scope**: Surgewave + Surgewave.AI + Surgewave.Connectors + alle Enterprise-Repos (Surgewave.Iceberg, Surgewave.Storage.*, Surgewave.Replication, Surgewave.Functions, Surgewave.Operator, Surgewave.Fleet, Surgewave.Mesh, Surgewave.Tactical, Surgewave.Agent, Surgewave.Governance, Surgewave.Transport, Surgewave.Samples, Surgewave.Bootcamp, Surgewave.Templates).
>
> **Stand**: 2026-04-07. Diese Analyse ist ein Snapshot — die Roadmap ändert sich, vor Priorisierung bitte aktuellen ROADMAP.md gegenchecken.

Surgewave hat sehr viel: Kafka-4.2-Protokoll-Parität, Streams (CEP, Watermarks, Joins, EOS, IQS), 117 Connectoren, 11 Serialisierungsformate, AI-Nodes, Governance, Tiered Storage (Arrow/DuckDB/Parquet/NvmeDirect + Azure/S3/GCP), Replication, Functions, K8s-Operator, Fleet-Control-Plane. Die folgenden Lücken bleiben gegenüber vergleichbaren Plattformen.

---

## 1) vs. Apache Spark

| Bereich | Lücke | Bemerkung |
|---|---|---|
| **Batch-Compute-Engine** | Kein klassischer DAG-Scheduler für Ad-hoc-Batch-Jobs über große Iceberg-Tabellen | Streams ist kontinuierlich; "scan 10 TB, joine, aggregiere" fehlt |
| **DataFrame-API** | `Surgewave.Linq` + `Surgewave.Streams.Linq` existieren, aber kein Spark-DataFrame-Äquivalent mit Lazy-Evaluation, Projection-Pushdown, Repartition | |
| **Catalyst / CBO** | Kein echter Cost-Based Optimizer mit Statistiken-Integration, Join-Reordering, Adaptive Query Execution | DuckDB-Plugin bringt etwas, aber nicht als Unified Engine |
| **Vectorized Execution** | Kein Photon-Äquivalent als Default-Engine | Arrow/DuckDB-Plugins existieren, aber sind opt-in |
| **MLlib** | Nur ONNX-Inference (`Surgewave.AI.ML`), kein verteiltes Training | |
| **GraphX** | Komplett fehlend | |
| **Spark Connect** | Kein generischer Thin-Client mit Remote-Query-Execution | `ISurgewaveClient` ist für Producer/Consumer, nicht für Compute |
| **Multi-Language Clients** | .NET-first; PySpark/Java/Scala/R-Äquivalente fehlen | Native Client SDKs sind im Roadmap geplant |
| **Notebooks** | Keine Jupyter/Zeppelin/Polyglot-Notebook-Integration | |

## 2) als Data Lakehouse

| Bereich | Lücke | Bemerkung |
|---|---|---|
| **Iceberg READ-Path** | `Surgewave.Iceberg` schreibt (Materialization), aber **Iceberg-Tabellen lesen** mit Predicate Pushdown, Snapshot-Travel, Partition Pruning fehlt | |
| **Delta Lake** | Komplett fehlend (read + write) | |
| **Apache Hudi** | Komplett fehlend | |
| **Time Travel SQL** | `SELECT … FOR TIMESTAMP AS OF …` / `VERSION AS OF …` | |
| **Branches & Tags** | Iceberg-Branches/Tags (git-style über Tabellen) | |
| **OPTIMIZE/VACUUM/Z-ORDER** | Kein User-facing `OPTIMIZE TABLE` mit File-Compaction, Z-Ordering, Bloom-Indizes | |
| **MERGE/UPSERT/DELETE** | Kein SQL-Äquivalent zu `MERGE INTO target USING source …` über Iceberg-Tabellen | |
| **Schema-Evolution auf Tabellen** | Add/Drop/Rename Column, Type Widening, Reorder über existierende Iceberg-Tabellen | Schema Registry deckt nur Topic-Schemas ab |
| **Materialized Views auf Iceberg** | Streaming MV mit Auto-Refresh über Iceberg-Tabellen | |
| **Catalog-Integration** | Hive Metastore, AWS Glue, Unity Catalog, Polaris, Lakekeeper, Nessie als Clients | Eigener REST-Catalog existiert, aber nicht als Bridge zu Fremd-Catalogs |
| **Iceberg REST Catalog SERVER** | Surgewave.Iceberg als REST-Catalog-Server, den Spark/Trino/DuckDB/Flink ansprechen können | |
| **Delta Sharing Protocol** | Cross-Org Data Sharing | |
| **Statistik-Sammlung** | `ANALYZE TABLE` für CBO | |
| **Column-Level Lineage** | DataLineage existiert, aber Column-Level (welche Spalte hängt von welcher Quellspalte ab) | |

## 3) als Data Platform

| Bereich | Lücke | Bemerkung |
|---|---|---|
| **Workflow-Orchestrierung** | Airflow/Dagster/Prefect/Databricks-Workflows-Äquivalent: DAGs mit Dependencies, Schedules, Retries, Sensors, SLAs | Connect-Pipelines sind keine General-Purpose Workflow Engine |
| **dbt-Style Transformations** | Model-Layer mit Tests, Docs, Lineage, `ref()`-Macros | |
| **JDBC/ODBC Server** | Spark Thrift Server / Impala-ODBC-Äquivalent, damit Tableau/PowerBI/Superset Surgewave-Tabellen abfragen | PG-Wire ist im Roadmap (Item #3) — JDBC/ODBC zusätzlich |
| **SQL Workbench** | Web-SQL-Editor mit Autocomplete, History, Query-Saving, Result Charts | Control-UI hat Message Browser, nicht SQL-Workbench |
| **RBAC: Row-Level Security** | RLS / Column-Level Masking / ABAC | Privacy hat PII-Scanning + Field-Encryption, aber nicht RLS-Policies |
| **Federated Queries** | Query-Federation: Surgewave-SQL über PostgreSQL/Snowflake/BigQuery/MongoDB ohne Ingestion | |
| **Reverse ETL** | Produktisierter Workflow "Warehouse → SaaS-Apps" (Salesforce/Hubspot/Stripe) | Sink-Connectors existieren, aber kein Hightouch/Census-Pendant |
| **Feature Store** | Feast/Tecton-Äquivalent: Online + Offline Features mit Point-in-Time Correct Joins | Surgewave.AI hat Retrieval, aber kein Feature-Store |
| **Vector Index Store** | First-Class Vector-DB (pgvector/Weaviate/Pinecone-Pendant) | Surgewave.AI.Retrieval ist da, aber nicht als IStorageEnginePlugin |
| **Model Registry / MLOps** | Versionierung, A/B-Testing, Drift Detection, Retraining Triggers | |
| **Time-Series Engine** | TSDB-Funktionen: Downsampling, Gap-Filling, Continuous Aggregates (Timescale-Style) | |
| **Geospatial** | H3/S2/PostGIS-Style-Functions, Spatial Index | |
| **Catalog-Föderation** | OpenMetadata/DataHub/Apache Atlas Bridge | DataMesh-Catalog existiert, aber kein Federation-Adapter |
| **OpenLineage** | OpenLineage-Events emittieren (Marquez-kompatibel) | |
| **Cost Attribution** | Per-Tenant Compute/Storage-Chargeback | Quotas existieren, aber keine Kostenzurechnung |
| **CMK / KMS-Integration** | AWS KMS / Azure Key Vault / GCP KMS / HSM | |
| **Backup & PIT-Restore** | Vollständiges Cluster-Backup mit Point-in-Time Restore (über Replication hinaus) | |
| **Semantic Layer** | Cube.dev / dbt-Semantic-Layer / AtScale-Äquivalent für BI | |

## 4) Surgewave.AI Lücken

- **Verteiltes Training** (nicht nur ONNX-Inference)
- **Feature Store** (Online + Offline mit Point-in-Time Joins)
- **Model Registry + MLOps Lifecycle**
- **GPU-Scheduling** für Inference-Workloads
- **First-Class Vector-Index Engine** als `IStorageEnginePlugin`
- **Multi-Modal Ingest-Pipelines** (Images/Audio/Video → Embeddings)

## 5) Surgewave.Connectors Lücken

- **Reverse-ETL-Workflow** (UI/Templates für Warehouse → SaaS)
- **No-Code Connector Builder** in der Pipeline-UI (Airbyte CDK-Äquivalent)
- **Connector-Anzahl**: 117 vs. Airbyte/Fivetran ~400+ — primär SaaS-APIs (CRM/Marketing/Billing/Support)

---

## Empfehlung zur Priorisierung

### Wenn Surgewave Richtung **Spark-Konkurrent**

1. **Iceberg READ + MERGE INTO** — höchster Hebel, `Surgewave.Iceberg` schreibt schon
2. **DataFrame-API + CBO**
3. **JDBC/ODBC Server** + **PG-Wire** (BI-Tooling-Anbindung) — PG-Wire steht eh schon im Roadmap
4. **Spark Connect-Style Thin Client**

### Wenn Richtung **Lakehouse**

1. **Iceberg REST Catalog Server** (Bridge zu Spark/Trino/DuckDB)
2. **Time Travel + Branches/Tags**
3. **OPTIMIZE/VACUUM/Z-ORDER**
4. **Delta Sharing Protocol**

### Wenn Richtung **Data Platform**

1. **Workflow-Orchestrierung** (`Surgewave.Workflow` als neues Repo)
2. **dbt-kompatibler Adapter** — sehr hoher Hebel mit kleinem Aufwand
3. **`Surgewave.FeatureStore`** (Surgewave.AI ergänzen)
4. **Row-Level Security + Column Masking** in Privacy

---

## Quellen / Referenzen

- `ROADMAP.md` (Surgewave Core) — bestätigt vorhandene Features, listet "Competitive Gaps" und "Competitive Parity & Innovation (2026)"
- `Surgewave.Ai/ROADMAP.md` — bestätigt aktuellen AI-Stand (Inference, RAG, Agents, A2A)
- Memory: `project_enterprise_extraction.md`, `project_connector_architecture.md`
