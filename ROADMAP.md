# Surgewave Roadmap

This roadmap tracks the development status and future direction for Surgewave, a drop-in Kafka replacement.

> **See [docs/](docs/) for comprehensive documentation of all implemented features.**
> Completed milestones are summarised; only open or in-flight items carry their full descriptions.

---

## Current Status

**Kafka 4.0 API Compatibility: 100%** | **Kafka 4.2 API Compatibility: In Progress**

Surgewave is fully compatible with the Confluent.Kafka .NET client (librdkafka-based):
- Producer/Consumer roundtrip verified
- Consumer group coordination (JoinGroup, SyncGroup, Heartbeat, LeaveGroup)
- Offset management (OffsetCommit, OffsetFetch)
- All 75 Kafka 4.0 APIs implemented with matching version ranges
- Kafka 4.1/4.2 APIs (76-92): see [Kafka 4.1/4.2 Protocol Parity](#kafka-4142-protocol-parity) below

---

## Completed Milestones

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Core Client APIs (Produce, Fetch, ListOffsets, Metadata) | Done |
| 2 | Consumer Group APIs (7 APIs) | Done |
| 3 | Admin APIs | Done |
| 4 | Protocol Version Updates (Kafka 4.0 ranges) | Done |

### Key Achievements
- 100% Kafka 4.0 protocol compatibility
- Native high-performance protocol — low-latency framed transport for .NET clients
- Multi-broker clustering with Raft consensus
- Tiered storage (S3, Azure, GCP, local file system) with Apache Arrow columnar engine
- Zero-Disk Object Store mode (S3, Azure Blob, GCP Cloud Storage)
- Zero-copy storage abstraction; shared-memory IPC for same-host clients
- Clustered EOS with cross-topic transactions (two-phase commit, auto-abort on timeout)
- Rack-aware replication with hierarchical failure domains
- Schema Registry (Confluent-compatible) with Avro/Protobuf/FlatBuffers/JSON serializers
- Live Schema Inference (JSON Schema auto-derivation, format detection, auto-registration)
- AI-Assisted Schema Evolution (rule-based + optional LLM, C# migration code generation)
- Zero-Downtime Schema Migration (transparent message transformation between versions)
- Schema Linking (cross-cluster schema synchronization, bidirectional/export/import)
- Kafka Connect framework (distributed coordination, plugin system, standalone + distributed workers)
- Kafka Streams library (full join support, multi-thread, EOS, CEP, Flink/Spark-style APIs)
- Interactive Query Service (IQS) — REST API for state stores, paginated key/value/window queries
- gRPC streaming API (11 services, 75 methods); GraphQL API (HotChocolate, real-time subscriptions)
- 70+ CLI commands; MCP Server (20+ tools, stdio + SSE transports)
- SIMD-optimized hot paths (AVX2/SSE2); Native AOT compilation
- Kubernetes operator, Helm chart, Docker images (GHCR), MSI/deb/rpm installers
- Comprehensive Grafana dashboards, Prometheus alerts, OpenTelemetry OTLP export
- OAuth2/OIDC, mTLS, SAML 2.0, Azure AD/Entra ID, Okta, Google Workspace, LDAP/AD
- Fine-grained ACL authorization (Literal, Prefix, Suffix, Regex patterns)
- Ephemeral topics, delayed delivery, per-message TTL, broker-level deduplication
- Dead Letter Queue (broker-native Nack, retry backoff, auto-creation)
- Client quotas (token bucket), delegation tokens (HMAC), network bandwidth quotas
- API Gateway (REST + gRPC JSON transcoding, Confluent v3 compatible, WebSocket streaming)
- Surgewave Control UI (Blazor Server, MudBlazor) — full feature management dashboard
- Visual Pipeline Designer (DAG editor, 100+ connector nodes, pro debugging features)
- AI/LLM connectors (OpenAI, Anthropic, Ollama, xAI Grok, GCP Vertex AI, Qdrant, pgvector)
- Broker-native Geo-Replication (active-passive mirroring + active-active multi-DC)
- Cluster Linking (topic-level cross-cluster mirroring with offset translation)
- Serverless Functions (event-driven, typed, retry+DLQ, stateful, parallelism)
- Broker-Native KV Store & Object Store (NATS JetStream-style KV buckets, chunked object storage, REST + native protocol)
- Stateless auto-scaling deployment mode (zero-disk, shared WAL, instant scale-up/down) for elastic clusters
- Edge-to-Cloud Sync (embedded broker, offline buffer, delta-sync, bidirectional)
- Rolling Upgrades (zero-downtime, leadership transfer, graceful shutdown orchestrator)
- Intent-Based Configuration (natural language → topic config, EN+DE, 16 built-in rules)
- Namespace-Level Multi-Tenancy (`tenant/namespace/topic`, quotas, admin delegation)
- Low-Code Data Mesh (topics as Data Products, SLOs, data contracts, quality monitoring)
- Cruise Control (auto-balance, partition/leader/disk/network scoring)
- Privacy-by-Design / GDPR (PII detection, AES-GCM field encryption, right-to-erasure)
- ONNX ML Scoring (real-time inference in pipeline nodes)
- WebAssembly Plugin System (Wasmtime engine, hot-deploy, Source/Sink/Transform/Function)
- Built-in CDC (PostgreSQL WAL, MySQL binlog, SQL Server CT)
- Multi-Protocol Gateway (MQTT 5.0 on port 1883, WebSocket produce/consume, AMQP 0.9.1 on port 5672, PostgreSQL wire protocol on port 5432 with `CREATE MATERIALIZED VIEW` support)
- Repository split into 12 independent repos (Surgewave, Connectors, AI, Samples, Bootcamp, Templates, Mesh, Tactical, Storage repos, Replication, Functions, Operator, Transport)
- Enterprise module extraction: Replication, Functions, Operator, Transport.SharedMemory → standalone repos with NuGet PackageReference
- Surgewave Plugin Package (.swpkg) system with role-based targets, REST API upload, broker→worker distribution, worker capabilities & tag-based placement
- Surgewave Marketplace (self-hosted plugin registry with Blazor UI, BaGetter-inspired architecture, signature enforcement, SBOM-aware)
- Reference connectors (Stdio, FileStream, Generator, Script) — migrated to Surgewave.Connectors, plugin-based deployment
- ConfigDef EditorHints — metadata-driven UI (Code, Cron, Expression, Condition, Topic, Select, Multiline, FilePath, Sql)
- `surgewave chat` / `surgewave message get` CLIs — interactive AI agent chat, message inspection, download buttons
- Plugin icon support, `pluginsettings.json` defaults layered via `AddPluginDefaults`, MSBuild-driven packaging via `Kuestenlogik.Surgewave.Build`
- Plugin Development Guide — comprehensive developer documentation with examples for all plugin types, packaging, testing, and publishing
- `.swpkg` signing (ECDSA P-256 + Charter X.509/CMS/RFC-3161), SBOM (CycloneDX 1.5), marketplace signature enforcement, Surgewave.Control "Verified" badge — full supply-chain story
- Namespace/package prefix migration `KL.*` → `Kuestenlogik.*` (NuGet requires ≥ 4-letter prefixes); branding "KL Surgewave" verkürzt zu "Surgewave" in Lizenz, Installern und Docs
- Repo-Hygiene Go-Live-Pass — `beachwalker` → `Kuestenlogik` Sweep (41 Dateien: GitHub-URLs, GHCR/Docker-Tags, Pages-Domain), Directory.Build.props ASCII-Authors + Copyright 2026 + `DebugType=embedded` (PDB im Assembly), `.gitattributes` LF-Policy, CODE_OF_CONDUCT.md (Contributor Covenant 2.1), SECURITY.md mit Surgewave-Scope, GitVersion.yml entfernt (Tag-driven Release-Workflow)
- Bowire-Migration — `Kuestenlogik.Bowline 0.9.4` → `Kuestenlogik.Bowire 1.0.3` in Broker und Gateway: Package-Refs, `using`-Statements, `MapBowline` → `MapBowire`, Route `/bowline` → `/bowire`, `__bowlineMtls__` → `__bowireMtls__`-Marker, Code-Kommentare und ROADMAP nachgezogen
- Marketing-Site Phase 2 (Bowire-Vorbild) — CSS-Token-Refactor (`:root`-Variablen für `--bg`/`--text`/`--accent`/`--accent-secondary`/`--lightning`), Pagefind-UI 1.4 (Header-Trigger + Modal-Overlay + dedizierte `/search.html`, `/`- und `Ctrl+K`-Shortcuts), 5 neue Includes (`comparison`, `protocols`, `use-cases`, `install`, `launch`) Surgewave-spezifisch geschrieben, visuelle Differenzierung zu Bowire (Surgewave-Cyan `#0ea5e9` + Violett `#a78bfa` + Yellow `#facc15`), `scripts/build-site.ps1` mit `-Local`-Switch baut DocFX + Jekyll + Pagefind nach `_combined/`
- DocFX `surgewave-theme` — Vollintegration analog Bowire-Theme: Master-Template + ~1950 Zeilen CSS; Surgewave-Header / Footer / Sidebar / Search-Box, gemeinsamer `localStorage['theme']`-Key zwischen Marketing-Site und API-Docs für synchronen Theme-Toggle
- Site- und Docs-Repositioning — "Surgewave zuerst, Kafka-Compat als Migrationspfad": Hero-Tagline `Your Stream. At Scale. In Control.`, Lede-Reihenfolge native Transport → AI-Pipelines → Embedded Broker → Plugin-Marketplace → Kafka-4.x-Drop-In-Closer; Surgewave Native Protocol als Default-Story, Kafka-Wire-Compat als Migration Path positioniert
- Hero V2 (Force-Graph-Live-Animation) — `_includes/hero.html` rendert ~120 Nodes + ~180 Edges mit Coulomb-Repulsion + Hooke-Springs + Centripetal-Pull, Eye-of-the-Surgewave mit konzentrischen Glow-Ringen, ~12 Feature-Reticles mit Mono-Labels, 60-fps-rAF-Loop, `prefers-reduced-motion` Static-Frame-Fallback; vertikales Surgewave-Lockup inline (`surgewave-lockup-v.svg`) auf Eye-Höhe positioniert mit Light-Cyan + Weiß analog Header-Mark
- CI-Hardening — `CliIntegrationTests` über `artifacts/`-Tree-Scan robust gegen Linux/Windows OutputPath-Unterschiede; `RaftApiHandler.SupportedApiKeys` Test auf 8 RPCs nachgezogen (KIP-853 Voter-Management); `TcpTransportRegistrationTests` direkte `RegisterTcpTransport`-Aufrufe statt `Register()` (Static-State-Isolation zwischen Test-Klassen); `CiSkip`-Helper für `LossyTcpProxy` / `LossyUdpProxy` Tests, deren Loopback-Timing auf GitHub-Actions-Linux unzuverlässig ist

---

## Completed Features

_Narrative summary of implemented subsystems. See git history, CHANGELOG, and docs/ for
per-feature detail._

- **Kafka 4.2 native protocol parity** — Share Groups, Consumer Group v2 (KIP-848), Client
  Telemetry (KIP-714), Streams Groups (KIP-1071), Key-Value & Object Store opcodes.
- **Broker group coordinators** — ConsumerGroupV2Coordinator, StreamsGroupCoordinator with
  stale-member cleanup and topology-aware task assignment.
- **Native protocol streaming** — Push-streaming handler, subscription manager, client
  streaming consumer with credit-based flow control.
- **Performance** — SIMD hot paths, Native AOT, ArrayPool everywhere, System.IO.Pipelines,
  FrozenDictionary lookups, combined response writes, shared-memory IPC.
- **Kafka Connect** — Distributed/Standalone workers, 30+ connector types, MirrorMaker 2.0,
  Pipeline Error Handling, pipeline nodes (Repartition, Priority Queue, Schema Decode,
  Inspector, Deduplication, Retry, Rate Limiter), P50/P95/P99 metrics, Flink/Spark/SignalR
  connectors.
- **Kafka Streams** — All join shapes (Stream-Stream/Stream-Table/Table-Table/Foreign Key),
  CEP, Flink/Spark-style APIs, EOS, Watermarks, Retry/Backoff+Circuit Breaker, Caching,
  Named Topologies, Topology Optimization, RocksDB/SQLite/MappedFile state stores,
  Multi-Thread Processing, Remote Interactive Queries, Streaming SQL Engine,
  Materialized Views over PG wire, TopologyTestDriver.
- **Architecture** — Arrow + Tiered Storage, SurgewaveRuntime fluent builder, layered
  `.Abstractions/.Hosting/.Packaging` pattern, `IPeerQueryProvider` opt-in,
  `IValidatableConfig` + `surgewave config validate` CLI + `/api/config/validate`,
  `pluginsettings.json` layered defaults, MSBuild plugin packaging.
- **IBrokerPlugin architecture** — 6 features promoted (AutoTuning, CruiseControl, Schema
  Registry, Connect, Geo-Replication), async `ConfigureAsync`, full DI migration of broker
  internals, plugin timing telemetry, graceful handling of unknown Kafka API keys.
- **CLI plugin tooling** — `surgewave config {view,init,validate}`, `surgewave plugin
  {show,defaults,diff,install --validate-config}`, `surgewave cluster status --wait`.
- **Plugin ecosystem** — JSON Schema for manifests, per-protocol-plugin defaults,
  `plugin.json` rename across 20+ repos, MSBuild migration in Surgewave.Connectors,
  `update-local-cache.ps1`, ReloadOnChange for plugin defaults, marketplace defaults
  endpoint.
- **Documentation** — build-your-first-plugin tutorial, plugin-defaults guide,
  per-protocol-plugin pages, CONTRIBUTING Plugin Development section, signing guide
  (`docs/security/plugin-signing.md`).
- **Queue semantics (QueueView)** — Visibility timeout, Ack/Nack/Reject, DLQ,
  MaxDeliveryCount, REST API, AMQP-QueueView integration (30+16 tests).
- **Testing infrastructure** — TestWaitHelpers, event-based waiting across integration
  suites, xUnit 3 migration.
- **Client SDK** — Unified `ISurgewaveClient` (SurgewaveNative/Kafka/Auto), Confluent.Kafka drop-in
  compatibility, schema-registry serializers (Avro/Protobuf/FlatBuffers/JSON +
  Hyperion/MessagePack/CBOR/Bond/Thrift/MemoryPack/Cap'n Proto/Orleans), Priority Lanes,
  plugin repository sources, message content deserialization in Message Browser,
  producer/consumer interceptors, admin ops, headers, batch offset commit, async serializers.
- **Developer experience** — Surgewave Bootcamp (34 units), 6 project templates,
  System.CommandLine 2.0.1 CLI (82 command files), DocFX (75+ pages), Microsoft Agent
  Framework integration, 5 BenchmarkDotNet suites, real-world benchmark suite (7 scenarios),
  8-platform comparison benchmark, serializer benchmarks, `surgewave` as `dotnet tool`,
  `Kuestenlogik.Surgewave.Sdk` MSBuild package, READMEs for all root folders.
- **Cloud / ops** — K8s manifests, Helm chart, Operator with SurgewaveCluster CRD, Grafana +
  Prometheus alerts.
- **Enterprise features** — Audit logging, resilience patterns (CB/Retry/Bulkhead/Pipeline),
  OAuth2/OIDC, mTLS, enhanced ACL patterns.
- **Exactly-once semantics** — Clustered coordination, WriteTxnMarkers replication,
  `__transaction_state` log, metrics, exactly-once source connectors.
- **Rack-aware replication** — Hierarchical failure domains (Region > DC > Zone > Rack),
  multi-level format support, consumer rack tracking, leader locality strategies,
  placement constraints.
- **Release infrastructure** — Apache 2.0 + BSL, GitVersion, GitHub Actions (ci, release,
  coverage, benchmark-regression, docs), Dependabot, CHANGELOG, CONTRIBUTING, SECURITY,
  issue/PR templates, Gateway container publishing, R2R/SelfContained publish, `publish.ps1`
  + `start.ps1` + `stop.ps1`.

---

## Testing Summary

| Test Suite | Count | Status |
|------------|-------|--------|
| Confluent.Kafka integration | 28 | Passing |
| Tiered storage | 25 | Passing |
| Arrow storage | 13 | Passing |
| Multi-broker replication | 7 | Passing |
| Raft consensus | 29 | Passing |
| CLI integration | 14 | Passing |
| Kafka Streams | 196 | Passing |
| Kafka Connect | 34 | Passing |
| Connect Transform Nodes | 117 | Passing |
| Redis Connector | 27 | Passing |
| Elasticsearch Connector | 42 | Passing |
| PostgreSQL CDC Connector | 35 | Passing |
| MongoDB Connector | 41 | Passing |
| MQTT Connector | 43 | Passing |
| Azure.Blob Connector | 42 | Passing |
| Gcp.Storage Connector | 42 | Passing |
| S3 Connector | 34 | Passing |
| HTTP Webhook Connector | 75 | Passing |
| OpenAI Connector | 40 | Passing |
| Qdrant / Ollama Connectors | 86 | Passing |
| Cloud Pub/Sub (GCP/AWS/Azure) | 76 | Passing |
| Stdio Connector | 16 | Passing |
| Csv / Parquet / Excel Connectors | 78 | Passing |
| Generator / NATS / RabbitMQ Connectors | 106 | Passing |
| MySQL/SQL Server/Oracle CDC | 157 | Passing |
| DynamoDB / Kinesis Connectors | 173 | Passing |
| Azure CosmosDb/Table/Queue Connectors | 301 | Passing |
| Akka.NET / Gcp.Firestore Connectors | 198 | Passing |
| Snowflake / Gcp.BigQuery / Cassandra | 278 | Passing |
| InfluxDB / Neo4j / GraphQL Connectors | 225 | Passing |
| iCal / Git / SFTP / SMTP / IMAP Connectors | 560 | Passing |
| Social Connectors (Slack, Teams, WhatsApp, etc.) | 258 | Passing |
| TCP/UDP/Socket/InProc/Script Connectors | 145 | Passing |
| Text Chunking / Batching / Sequence Connectors | 210 | Passing |
| AI Connectors (Anthropic, Grok, Vertex, etc.) | 175 | Passing |
| Drive/OneDrive Connectors | 116 | Passing |
| ONNX ML Scoring | 15 | Passing |
| A2A Integration / Agents Runtime | 18 | Passing |
| Vector Store Connector | 10 | Passing |
| Audit logging | 17 | Passing |
| Resilience patterns | 27 | Passing |
| Security (ACL, OAuth2) | 46 | Passing |
| EOS transactions | 24 | Passing |
| EOS Connect integration | 25 | Passing |
| Rack-aware replication | 91 | Passing |
| Data integrity / Backup/restore | 30 | Passing |
| **Additional test suites (Broker, MultiTenancy, Schema, Arrow, Tiering, GraphQL, Stdio, VectorStore)** | ~500 | Passing |
| QueueView (visibility timeout, Ack/Nack/Reject, DLQ, concurrent access) | 30 | Passing |

**Total: ~5,120+ tests passing (4,738 Surgewave + 382 Surgewave.AI)**

---

## Performance Benchmarks

Public head-to-head benchmark results land with the 1.0 release. Internal regression baselines live under
`benchmarks/baselines/` and are exercised by `.github/workflows/benchmark-regression.yml` so drift is caught
on every PR. The benchmark suite (`Kuestenlogik.Surgewave.Benchmarks.Comparison`) covers Surgewave in
embedded / standalone / container deployments across native and Kafka protocols.

The reproducible scenarios that already run in CI:

- Throughput regression on the embedded broker (`Kuestenlogik.Surgewave.Benchmarks.Throughput`)
- QUIC vs TCP transport (`Kuestenlogik.Surgewave.Benchmarks.QuicVsTcp`)
- Replication and peer-transport overhead (`Kuestenlogik.Surgewave.Benchmarks.RealWorld`)

Comparative numbers vs Kafka, Redpanda, and others are intentionally omitted from this roadmap until a
documented head-to-head run on identical hardware is published alongside the release.

---

## Done Domains (collapsed)

Sections that once tracked many work items are now complete. One-line summaries replace the
per-row tables; git history has the detail.

- **Kafka Streams — Production Readiness (Prio 1)** — All done. TopologyTestDriver, three
  state stores (RocksDB/SQLite/MappedFile), multi-thread processing, remote IQ, streams
  benchmarks.
- **Kafka Streams — Competitive Advantage (Prio 2)** — All done. Streaming SQL engine,
  StreamsUncaughtExceptionHandler, proper task assignment, state store changelog restore.
- **Broker — Competitive Features (Prio 2)** — All done. Delayed delivery, broker-level
  dedup, automatic partition rebalancing, 5 subscription types, topic bundles/unloading.
- **Architecture — Next Generation (Prio 3)** — All done. Inline transforms (C# + WASM),
  stream lineage / data governance, zero-disk broker mode, native multi-tenancy, serverless
  scaling, cloud object-store providers (S3 / Azure Blob / GCP).
- **Surgewave.AI — AI Application Layer (Prio 2)** — All done. Document processing, RAG
  orchestration, retrieval + hybrid search, AI pipeline nodes (9), agent tools + memory +
  caching, workflow nodes (6), prompt templates, RAG evaluation, multimodal, pipeline chat,
  vector store connector, AI templates + seeder, guardrails, streaming chat.
- **Storage engines (Future Considerations)** — All done. RocksDB, SQLite, Parquet, LMDB,
  DuckDB, S3-Direct, NVMe-Direct.
- **Streams processing nodes & pipeline designer** — All done. SQL Transform node, exception
  handler node, Streams/AI/Workflow palette, special node rendering, auto-save, link labels,
  alignment guides, node groups, live data preview, pipeline versioning, pipeline chat UI.
- **Plugin & Connector Marketplace foundations** — All done. Package system (.surgewavepackage →
  .swpkg), repository + CLI commands, pre-built pipeline templates, dependency resolution,
  marketplace UI, native-protocol plugin opcodes, SHA256 signing, publish command, rating &
  reviews.
- **Surgewave Plugin Package (.swpkg) system** — All done. Format definition, role-based targets,
  `TargetSpec`/`PluginRole`, plugin REST API (upload/download), Control UI upload,
  broker→worker distribution, on-demand worker sync, MSBuild `.swpkg` packaging, `ICliPlugin`,
  `surgewave` as `dotnet tool`.
- **Worker capabilities & pipeline placement** — All done. Role tags, `AllowAutoInstall`,
  tag-aware registry, `PlacementStrategy` (Auto/TagBased/Manual), pipeline validator with
  `PLUGIN_NOT_AVAILABLE`/`NO_WORKER_WITH_TAG`/`WORKER_NOT_FOUND`/`AUTO_INSTALL_DISABLED`
  codes, `PlacementResult`.
- **Connector architecture** — All done. Reference connectors moved to Surgewave repo, all
  connectors as `.swpkg` plugins, Script connector (Source/Transform/Sink via Roslyn), plugin
  discovery from `plugins/` only, HTTP/3 via Kestrel, gRPC over HTTP/3 on :9094, raw QUIC
  transport plugin, `ISurgewaveStreamHandler` / `ISurgewaveTransport` QUIC, `IPeerTransport`
  abstraction + QUIC inter-broker + QUIC geo-replication, `NetworkLossScenario` chaos
  primitive, A/B transport benchmarks, inter-broker QUIC fallback with TCP, Surgewave.Edge QUIC
  cloud transport, Control-UI peer-transport chip, self-contained single-file publish,
  performance regression baselines v3.0, Surgewave.Fleet gRPC/QUIC upgrade, standalone Schema
  Registry on :8081 with broker `ExternalUrl` proxy.
- **Plugin hot-swap & CLI enhancements** — All done. AssemblyLoadContext unload + reinstall,
  dependent-pipeline restart endpoint, CLI `produce --input` + `consume --output` with
  format selectors, process-stop scripts handle self-contained EXEs.
- **Surgewave Marketplace** — All done. Server (:5060), full REST API, storage + metadata
  abstractions, Blazor controller UI (:5061), browse/search/detail/publish pages, Control UI
  renamed to Plugins at `/plugins`.
- **SSO & enterprise authentication** — All done. OIDC/Keycloak, RBAC, SAML 2.0, Azure AD /
  Entra ID, Okta, Google Workspace, LDAP/AD Bind, session management, multi-IdP.
- **Surgewave Core — Performance & production hardening (A)** — All done. Chaos/fault-injection
  framework, performance regression suite, hot-path profiling + 5 zero-alloc optimisations.
- **Surgewave.AI — Next Level (B)** — All done. Streaming chat (SSE/WebSocket), guardrails (4
  types, 27 tests), agent memory + tool caching (6 memory types, 58 tests).
- **Control UI enhancements (E)** — All done. Theme toggle, 14 dashboard widgets, visual
  debugging timeline, agent design studio, standalone message browser with live tail + wire
  format detection + hex dump + queue badge.
- **Differenzierung — Tier 1 / 2 / 3** — All done. Surgewave Assistant (metrics analyzer +
  tuning advisor + NL-to-SQL), Edge-to-Cloud Sync, Live Schema Inference, Natural Language
  Queries, WASM plugin system, built-in CDC, multi-protocol gateway, request-reply, adaptive
  auto-tuning, Cruise Control, GraphQL API, Serverless Functions, native geo-replication.
- **Operations & production maturity** — All done. Rolling upgrades, online partition
  reassignment, broker decommission, Cruise Control auto-balance, zero-downtime schema
  migration.
- **Enterprise features (competitive gaps)** — All done. Namespace multi-tenancy, cross-topic
  transactions, schema linking, cluster linking, network bandwidth quotas, exactly-once
  source connectors.
- **Performance verification** — All done. Real-world benchmark suite, linear scaling
  verification (1→3→5 brokers), 8-platform comparison, Interactive Query Service.
- **Kafka 4.1/4.2 protocol surface** — RPC-level done. API key enum fixes, version
  advertisement aligned to Kafka 4.2 trunk, all 67 advertised APIs wired, Share Groups +
  Raft Voters + Streams Groups request/response classes + handlers + broker logic
  (KIPs 932 / 853 / 1071 / 848). Coordinator semantics (reconciliation protocol, additional
  assignors, persistence, conformance tests) tracked under "Kafka 4.x semantics hardening"
  in Open Items.
- **Unique differentiators (Phase 2)** — All done. Low-Code Data Mesh, AI-Assisted Schema
  Evolution, Embedded ML Scoring (ONNX), Privacy-by-Design (PII/AES-GCM/erasure),
  Collaborative Pipeline Editor, Intent-Based Configuration.
- **Supply-chain security** — All done (3 sessions). `.swpkg` signing (ECDSA P-256 builtin +
  Charter CMS/PKCS#7 with X.509 + RFC-3161 timestamping + long-lived-signature semantics),
  pluggable `ISppSigner` / `ISppSignerProvider` abstraction with `AssemblyLoadContext`
  discovery, install-path verification, marketplace upload enforcement, Surgewave.Control
  "Verified" badge, CycloneDX 1.5 SBOM in every `.swpkg`, signed-connector sample, full
  `docs/security/plugin-signing.md` walkthrough, 11 Marketplace integration tests + 4 SBOM
  tests.
- **HotChocolate/Bowire dep hygiene** — HotChocolate 15.1.12 → 15.1.15 (CVE GHSA-qr3m-xw4c-jqw3),
  Bowire local.97 → local.160 — full solution builds 0-warn / 0-error.
- **Bowire embedded observability hook** — `Kuestenlogik.Surgewave.Core.Observability` carries
  `ISurgewaveBrokerObservability` + `SurgewaveBrokerEvent`; `SurgewaveBrokerObservability` does
  bounded-channel fan-out with `DropOldest` per subscriber so a slow observer never
  back-pressures the hot path. `DataApiHandler` publishes `Produced` (on successful
  `AppendBatchAsync`), `Rejected` (on produce failures, carrying `ex.Message`), and
  `Consumed` (on non-empty partition fetches). `ConsumerGroupCoordinator` publishes
  `Rebalanced` on the leader's SyncGroup with the rebalanced group id in `Consumers`.
  5 tests in `ObservabilityWiringTests` + `ObservabilityMultiplexTests` cover wiring,
  multi-subscriber fan-out, DropOldest under a stuck subscriber, and clean cancellation.
  Pairs with `Bowire.Protocol.Surgewave`'s `surgewave://embedded` URL which resolves the
  interface out of DI.

---

## Open Items

Only forward-looking work tracked below. All Done items collapsed above.

### Planned enhancements

Forward-looking work tracked at the feature level. Each item is independently scoped; ordering is
illustrative and may shift with operator feedback.

| # | Gap | Effort | Priority |
|---|-----|--------|----------|
| G1 | **Native non-.NET clients** (Python, Go, Rust). Without these the "Kafka drop-in" story is hollow for ~80% of Kafka users. Already on roadmap; nothing in flight. | Multi-week per language | High |
| G2 | ~~**Published Kafka conformance test results**~~ — "drop-in" must be testable. Shipped as `CONFORMANCE.md` at the repo root: full per-RPC matrix (51 wired Kafka 4.0 RPCs out of ~55, 60 advertised across 93 enum entries), per-KIP coverage (14 KIPs), conformance-test inventory (Wire encoder/decoder, Confluent.Kafka 2.x round-trip, librdkafka 2.14 KIP-848 e2e, KIP-892/994 transaction defense), and an honest gap section calling out the EOS transaction RPCs (22, 24, 25, 26, 28) that have full semantics but no Kafka-wire handler binding yet. README and `docs/transport/kafka-protocol.md` link back. Source-of-truth pointers (`ApiVersionsResponse.CreateDefault`, per-handler `SupportedApiKeys`) make CI drift detection trivial. | ~~Days~~ Done | ~~High~~ |
| G3 | **Public benchmarks** on identical hardware, not just internal numbers — Surgewave native and Kafka wire on the same broker, full configuration disclosed. | Days (hardware + repeatability) | High |
| G4 | **Real Jepsen run** — own checker is in (#4a Done), external Jepsen is the industry trust stamp. | ~1 week setup + run + report | Medium |
| G5 | ~~**S3-only broker mode**~~ (zero-disk deployment) — already shipped under `Kuestenlogik.Surgewave.Storage.Engine.S3`: `S3StorageEngine` writes batches directly to S3 (batched-flush + local read cache + JSON index), `S3LogSegmentFactory` plugs into `ILogSegmentFactory`, `WithS3Storage()` / `WithS3StorageLocalStack()` extensions on `SurgewaveRuntimeBuilder`. Operators run zero-disk against AWS, MinIO, or LocalStack with `builder.WithS3Storage("bucket")`. The earlier "Surgewave doesn't have this" line was wrong — verified by `S3StorageExtensionsTests` (3 smoke tests). End-to-end LocalStack-fixture test is wanted but not blocking; would live in a `Kuestenlogik.Surgewave.IntegrationTests.S3` project. | ~~Multi-week~~ Done | ~~Medium-High~~ |
| G6 | ~~**Broker-native Iceberg topics**~~ (Confluent Tableflow / Redpanda Iceberg Topics parity) — Audit revealed Surgewave.Iceberg already ships an `IBrokerPlugin` whose `IcebergMaterializationEngine` background-service exports Surgewave topics as Iceberg v2 tables (Parquet data files + manifest lists + commits to a pluggable `IIcebergCatalog`). Missing piece was the per-topic toggle: opt-in was a global `IcebergConfig.Topics` allow-list, forcing a broker restart per topic. Closed in `Surgewave.Iceberg` commit `df52591`: new per-topic config keys `iceberg.materialize.enabled=true` and `iceberg.materialize.table.name=...` give operators the "topic = table" toggle without restart. Resolution order: explicit per-topic opt-in → legacy global allow-list → every non-internal topic. Slash-namespaced topic names (`orders/eu`) sanitise to underscores. 6 toggle tests + 45 total Iceberg tests green. | ~~2 weeks~~ Done | ~~Medium~~ |
| G7 | ~~**On-broker WASM record transforms**~~ (broker-side record-level hot-path hooks). New `IRecordTransformPipeline` in `Kuestenlogik.Surgewave.Core.Pipeline` — three-state contract (return input slice = no change, return new bytes = replace, return null = drop). `DataApiHandler.HandleProduceAsync` calls it after dedup and before `AppendBatchAsync`. `WasmRecordTransformPipeline` in `Kuestenlogik.Surgewave.Wasm` resolves a per-topic plugin id from the `wasm.transform.plugin.id` topic config, dispatches to the existing `WasmPluginManager`, caches the binding, and invalidates the cache via `ITopicLifecycleHook` so config changes go live without a broker restart. The no-binding hot-path is one dictionary read + bool check, so producer throughput is unaffected on topics that don't opt in. 5 tests on a non-Wasm in-process pipeline cover the cache, lifecycle invalidation, drop path, replace path, and pass-through. | ~~2 weeks~~ Done | ~~Medium~~ |
| G8 | ~~**OAuth2 SASL OAUTHBEARER on the wire**~~ — Kafka wire-protocol auth via OIDC. Audit found the OAUTHBEARER plumbing (`OAuthBearerAuthenticator`, `JwksTokenValidator`, `OAuthBearerConfig`, RFC 7628 frame parser, KIP-936 alignment) already shipped in `Kuestenlogik.Surgewave.Broker.Security.OAuthBearer` and the `SaslAuthenticator` already accepting an optional `OAuthBearerAuthenticator` constructor argument — but never wired up at broker startup. Closed by registering `IHttpClientFactory` in DI, exposing `OAuthBearerConfig` under `Surgewave:Security:OAuthBearer`, and constructing `JwksTokenValidator` + `OAuthBearerAuthenticator` whenever `OAuthBearer.Enabled=true` and `OAUTHBEARER` is listed in `SaslMechanisms`. JWT validation supports OIDC discovery (`OidcAuthority` → `.well-known/openid-configuration`) and direct JWKS (`JwksUri`); issuer / audience / principal-claim are configurable; JWKS refresh interval defaults to 30 min. New `SaslOAuthBearerWiringTests` (6 tests) pin down mechanism advertisement, single-step dispatch, the "validator missing" rejection path, and the "validator's internal reason must not leak to the wire" envelope. Combined with the existing `OAuthBearerAuthenticatorTests` (6 tests) the OAUTHBEARER surface is at 12 green unit tests; an IdP-fixture e2e test against a librdkafka client remains a follow-up. | ~~1 week~~ Done | ~~Medium~~ |
| G9 | ~~**Kafka 4.x semantics hardening**~~ — KIP-848 reconciliation protocol + assignors (uniform/sticky/cooperative-sticky), KIP-932 Renew semantics + persistence, conformance tests against the librdkafka next-gen client. Tracked in detail across 9a–9u (next section); every sub-item is Done. The headline: KIP-848 reconciliation + server-side assignors (range / roundrobin / sticky / cooperative-sticky), KIP-932 Renew + on-disk persistence, KIP-848 + KIP-932 + KIP-1071 wire-tests + Confluent.Kafka 2.14 e2e green, KIP-892 transaction defense, KIP-955 init-metadata monotonicity, KIP-985 reverse iteration, KIP-994 ListTransactions filters, KIP-996 Pre-Vote, KIP-1059 ListOffsets earliest-local, KIP-936 OAUTHBEARER server callback. Final hardening (9u) closes the silent-stale-heartbeat gap that the existing fence on the OffsetCommit/Fetch path had not extended to ConsumerGroupHeartbeat itself. | ~~1-2 weeks~~ Done | ~~High~~ |
| G10 | ~~**Field-Level Encryption + KMS**~~ (Confluent CSFLE parity) — Shipped in the `Surgewave.Governance` repo (commit `f03152b`). New `IKmsProvider` abstraction (Wrap / Unwrap / GenerateDataKey, stable `ProviderName` for wire routing); `InMemoryKmsProvider` backs dev / tests; `EnvelopeFieldEncryptor` does per-call DEK + KMS-wrapped envelope encryption against named JSON field paths. Wire format `csfle:v1:{kekId}:{base64(u16-BE wrappedLen | wrappedDek | nonce | tag | ciphertext)}` carries the wrapped DEK on every encrypted value so readers decrypt independently of schema co-ordination. AES-GCM-256 throughout, DEK zeroed in memory after each operation, tampering trips `AuthenticationTagMismatchException`. AWS / Azure / GCP / Vault providers slot in against the same interface as future plugins. 8 round-trip / negative-path tests. | ~~2 weeks~~ Done | ~~Medium~~ |
| G11 | ~~**Stream Lineage as a first-class feature**~~ with auto-discovered topic→pipeline→topic graphs and time-travel. Shipped in the `Surgewave.Governance` repo (commit `f7ff338`). New `IStreamLineageRegistry` contract carries an append-only history of `StreamLineageObservation`s — every recorded edge keeps its `ObservedAt` timestamp so callers can `QueryAtAsync(nodeId, asOf)` for incident postmortems and audit trails. Edge identity is `(Source.Id, Target.Id, TransformationType)`; refreshed observations replace older ones via latest-wins dedup so a transformation rename surfaces as one updated edge instead of growing the graph. `InMemoryStreamLineageRegistry` uses a `ConcurrentBag` for lock-free writes; `BuildGraph` walks both directions out from the focal node so a single query returns upstream + downstream + edges. 10 unit tests cover record/query, time-travel, dedup, mixed node types, empty-id guard, and concurrent recording (8×250 writes). The producer-path / pipeline-manager / connector-framework auto-wiring is the remaining surface; the registry is the first-class layer Confluent Stream Lineage required. | ~~1-2 weeks~~ Done | ~~Medium~~ |
| G12 | **Cluster-Linking-grade geo-replication** — verify parity with Confluent Cluster Linking (active-active offset translation, conflict resolution). Surgewave.Replication exists; gap-audit pending. | Days (audit) + variable (fix) | Medium |
| G13 | ~~**Audit-Log-to-Topic**~~ — operational compliance feed. Audit revealed an existing `AuditLogger` that already buffers events through a `Channel<AuditEvent>` and writes to `audit.log` (file) plus an in-memory ring for `/admin/audit` REST queries. The missing piece was a parallel topic sink so SIEM / compliance pipelines can consume audit events through the regular Kafka wire (Confluent Audit Logs parity). New `AuditTopicSink` builds a Kafka v2 RecordBatch from each audit-event batch (key = principal, falling back to `broker-{id}` for anonymous events; value = JSON-serialised `AuditEvent` — same shape as `audit.log`) and appends it to a configurable internal topic (`Surgewave:Security:Audit:TopicName`, default `_audit_events`). Topic auto-creation is idempotent (handles broker restart). The sink rides on top of the file write — the file remains authoritative; topic-write failures are logged and never bring down the broker or drop events from the file sink. Two new `AuditConfig` knobs: `TopicSinkEnabled` (default false) and `TopicName`. 6 unit tests (`AuditTopicSinkTests`) cover empty-batch no-op, topic auto-create, JSON-shape round-trip, idempotent re-create, partition-hash spread, anonymous-event key fallback. | ~~Days~~ Done | ~~Medium~~ |
| G14 | ~~**Disaster-Recovery tooling**~~ — backup/restore CLI, point-in-time restore. Audit revealed an existing backup/restore stack (`BackupService`, `RestoreService`, `BackupManifest` with SHA256 checksums; `surgewave backup create / restore / list / verify` CLI subcommands) that already covered the full-snapshot story. The G14 gap was point-in-time restore: the existing path is all-or-nothing per topic, with no way to roll a partition back to "the state before the bad write started". Closed by recording `MaxTimestampMs` per segment in the manifest at backup time (read from the segment's `.timeindex` last entry) and adding a new `RestoreOptions` with `TargetTimestampMs` (Unix ms cutoff — segments newer than this are skipped) and `TargetOffsetsPerPartition` (`{topic}/{partitionId}` → max-`BaseOffset`). PIT operates at segment-boundary granularity — a segment whose `BaseOffset` ≤ cutoff is fully restored; one whose `BaseOffset` is past the cutoff is fully skipped. Within-segment truncation is documented as a follow-up; in practice segment rotation defaults (~1 GB or hourly) make boundary-granularity precise enough for the disaster-recovery use case where the operator targets "before the disaster started" with hour-scale tolerance. CLI grows `--target-timestamp <unixMs>` and `--target-offset 'topic:partition=offset'` (repeatable). `RestoreResult` exposes `SegmentsSkipped` for operator visibility. 7 new `PointInTimeRestoreTests` (boundary, both-cutoffs, max-timestamp-zero-fallback, full e2e from fake backup); 14 existing `BackupRestoreTests` non-regressed. | ~~1 week~~ Done | ~~Medium~~ |
| G15 | **CLI polish** — Surgewave CLI is functional, but per-command ergonomics still lag. | Continuous | Low-Medium |
| G16 | ~~**Confluent Schema Registry wire-compat audit**~~ — existing CSR-based tooling should work unchanged against Surgewave. Audit finding: Surgewave's Schema Registry is wire-compatible at every contract surface — magic byte (`0x00`), big-endian int32 schemaId, REST API paths (`/subjects`, `/schemas/ids/{id}`, `/compatibility/...`, `/config`), JSON-shape conventions (`error_code` snake_case, `is_compatible` snake_case, `compatibilityLevel` camelCase — exactly tracking Confluent's own inconsistencies), all seven Confluent compatibility levels, `AVRO`/`JSON`/`PROTOBUF` schemaType strings (case-insensitive in, uppercase out). New `ConfluentSchemaRegistryContractTests` (28 tests) pin the JSON-shape contract against silent regressions — a stray `errorCode` (camelCase) instead of `error_code` would break every Confluent client without surfacing as a type or unit-test failure elsewhere. Documented in `CONFORMANCE.md` "Confluent Schema Registry compatibility" section with per-path table and known gaps. Two open items: (1) Multi-message Protobuf MessageIndex (Surgewave always writes index 0; rare in practice but tracked); (2) Live e2e round-trip with `CachedSchemaRegistryClient` is the next confidence layer beyond contract pinning. | ~~Days (audit)~~ Done | ~~Medium~~ |
| G17 | **Flink connector** — analytical bridge for Surgewave topics into Flink jobs. | ~1 week | Low-Medium |
| G18 | **AI-Tier-Resplit — Primitives in Community ziehen** — Hero-Tagline `Built-in AI pipelines` ist heute eine leichte Überspannung: AI-Connectors (OpenAI/Anthropic/Ollama/…) sind Community, aber sämtliche Surgewave.AI-Pipeline-Nodes sind Enterprise. Schnitt neu ziehen: ein `Surgewave.AI.Primitives`-NuGet (Community / lizenzfrei) bündelt fünf Nodes — `LlmChatNode`, `EmbeddingNode`, `VectorStoreReadNode`, `VectorStoreWriteNode`, `PromptTemplateNode`, plus ein `SingleShotRagNode` (Retrieve→Augment→Generate in *einem* Node, ohne Multi-Step-Orchestrierung). Enterprise behält den Wertkern: Agent-Framework, RAG-Orchestrierung (Chains/Hybrid/Rerank), Guardrails, Document-Processing, Pipeline-Chat-UI, Streaming-Chat-Infra, RAG-Eval, Multimodal. `SurgewaveFeatures` bekommt `AiPrimitives` (Community) + `AiOrchestration` (Enterprise) statt heutigem flachen `AI`. Nach dem Schnitt schlägt Surgewave Free die Konkurrenz klar (Redpanda Connect hat AI-Connectors, aber keine Pipeline-Nodes; Confluent OSS hat gar nichts) und die Hero-Tagline ist sauber tragfähig. | ~3-5 days | Medium-High |

### Kafka 4.x semantics hardening (G9)

The wire surface for KIP-848 (Consumer Group v2) and KIP-932 (Share Groups) is shipped
under `src/Kuestenlogik.Surgewave.Broker/ConsumerGroupV2` and `src/Kuestenlogik.Surgewave.Broker/ShareGroups`. These
items track the semantic completeness:

| # | Item | Notes | Priority |
|---|------|-------|----------|
| 9a | ~~**KIP-848 reconciliation protocol**~~ | New `ConsumerGroupV2Reconciler` advertises only the subset of a member's target that no other member still reports owning, so a partition is never handed to its new owner before the previous one revokes. Group state distinguishes Stable from Reconciling based on whether every member's owned set matches its target. | ~~High~~ Done |
| 9b | ~~**KIP-848 additional assignors**~~ | `ConsumerGroupV2Coordinator` now resolves `request.ServerAssignor` via the existing `PartitionAssignorFactory` (`range`, `roundrobin`, `sticky`, `cooperative-sticky`); unknown names safely fall back to `range`. Previous target is forwarded as `MemberSubscription.UserData` so sticky variants preserve continuity. | ~~High~~ Done |
| 9c | ~~**KIP-848 / KIP-932 persistence**~~ | New `IGroupStateStore<T>` + JSON-file-per-group `JsonFileGroupStateStore<T>` with debounced writes (1 s). `ConsumerGroupV2Coordinator` and `ShareGroupCoordinator` accept an optional persistence dependency, save after every state-mutating heartbeat / offset alter / leave / sweep, and rehydrate on construction. State files live under `data/.metadata/consumer-groups-v2/` and `data/.metadata/share-groups/`. Empty groups are deleted from disk so the directory doesn't grow unbounded. The compacted-topic variant tracked in the original spec is parked in favour of this simpler on-disk shape. 3 persistence tests. | ~~Medium-High~~ Done (file-based) |
| 9d | ~~**KIP-932 Renew semantics**~~ | `IQueueView.ExtendVisibility(messageId, extension)` extends the lease in place without bumping `DeliveryCount`. `ShareGroupCoordinator` routes `AcknowledgeType=4` to the new method instead of aliasing to Nack+requeue. | ~~Medium~~ Done |
| 9e | ~~**Stale-member background sweep**~~ | New `GroupCoordinatorSweepService` (`IHostedService`) drives `SweepStaleMembers()` on `ConsumerGroupV2Coordinator`, `ShareGroupCoordinator`, and `StreamsGroupCoordinator` every 30 s. Empty groups GC even if no member ever heartbeats again. 3 sweep tests. | ~~Low-Medium~~ Done |
| 9f | ~~**Conformance tests**~~ | Unit-test scope: `ConsumerGroupV2CoordinatorTests` (8), `QueueViewTests.ExtendVisibility*` (3), `ConsumerGroupV2PersistenceTests` (3), `StreamsGroupStickyTests` (2), `GroupCoordinatorSweepServiceTests` (3), `TopicLifecycleHookTests` (4), `Kip892TransactionDefenseTests` (5), `OAuthBearerAuthenticatorTests` (6). Wire-level: `Kip848WireTests` (5), `Kip932WireTests` (4). **e2e against Confluent.Kafka 2.14 with `group.protocol=consumer` is now GREEN.** Eight wire-level bugs from successive live-capture debug sessions: (1) OffsetCommit/Fetch v9 routing to v2 coordinator (error codes 110-113). (2) `group.version=1` capability advertise. (3) ConsumerGroupHeartbeat v1 parser with `SubscribedTopicRegex`. (4) `ProtocolVersions.IsFlexible` registered for every KIP-848/932/1071/853/714 RPC. (5) `MetadataResponse.TopicId` was hardcoded `Guid.Empty`. (6) `MetadataApiHandler` resolves `t.TopicId` → name when the client looks up topic-by-id. (7) `MemberAssignment` was wire-encoded as a compact array; per spec it is a 1-byte presence marker (`-1`=null, `1`=present) plus the struct fields. (8) **KIP-903 (Fetch v15+)** removed the top-level `ReplicaId` field — Surgewave was still reading those 4 bytes and mis-aligning every subsequent field, silently dropping the request. After all fixes both `Kip848ConsumerProtocolTests` pass in ~1 s; classic-protocol roundtrip (`ConfluentKafkaCompatibilityTests`) still green. | ~~High~~ Done |
| 9g | ~~**DRY `ProcessAcknowledgementBatches`**~~ | Both overloads now project into a common `AckBatch` record and dispatch through a single generic implementation. | ~~Low~~ Done |
| 9h | ~~**KIP-892 Transactions Server-Side Defense**~~ | `TransactionCoordinator.HandleInitProducerIdAsync` rewritten so the broker — not the client — owns the producer epoch. A request that names a stale `(producer-id, epoch)` for an existing `transactional-id` is fenced with `InvalidProducerEpoch`; a fresh-init from the same txn-id over no in-flight txn forces the epoch upward so any zombie still using the previous incarnation is fenced on its very next request. Idempotent retries (current pid + epoch, Empty state) are recognised and return as-is. The wire-facing `InvalidProducerEpoch` response carries the current authoritative `(pid, epoch)` so a polite client can update its bookkeeping. 5 new `Kip892TransactionDefenseTests` (fresh init, idempotent re-init, zombie-with-old-epoch, foreign pid, epoch-bump on fresh-init). 94 existing transaction tests still green. | ~~High~~ Done |
| 9i | ~~**KIP-936 SASL OAUTHBEARER server callback**~~ | New `IOAuthBearerTokenValidator` plugin contract; built-in `JwksTokenValidator` does JWT signature validation against a remote JWKS endpoint (OIDC discovery or direct JWKS URL), with `Microsoft.IdentityModel`-backed caching for `JwksRefreshInterval` (default 30 min). `OAuthBearerAuthenticator` parses the RFC 7628 client-first frame and routes the bearer token through the validator; the wire-facing failure reason is intentionally generic so a malicious client can't fingerprint the IdP. `SaslAuthenticator` exposes `OAUTHBEARER` as a single-step mechanism alongside `PLAIN`. The HTTP-backed end-to-end IdP test is parked under 9f. 6 unit tests cover frame parsing + valid/invalid token dispatch. | ~~High~~ Done |
| 9j | ~~**KIP-955 ConsumerGroupHeartbeat init metadata**~~ | The founding member's first heartbeat seeds `RebalanceTimeoutMs` on the group; subsequent heartbeats can only shrink it monotonically — slow members can't push the group's tolerance back up. New `Initialized` flag on `ConsumerGroupV2State` distinguishes init from rejoin. | ~~Medium~~ Done |
| 9k | ~~**KIP-919 SASL credential rotation**~~ | Audit done. Surgewave's `SaslAuthenticateResponse` already carries `SessionLifetimeMs` (KIP-368), the wire mechanism that signals clients when re-authentication is due. The broker currently emits `0` (no re-auth required). KIP-919 itself is primarily a client-library concern (token refresh via SASL OAUTHBEARER callbacks); the server-side hook is the existing `SaslAuthenticate` v2+ which Surgewave already handles. Tightening the lifetime value is wired-up work for the OAUTHBEARER auth path tracked under 9i. | ~~Medium~~ Done (audit) |
| 9l | ~~**KIP-1102 Client rebootstrap on errors**~~ | Audit verified: `CoordinatorNotAvailable` (15), `NotCoordinator` (16), and `UnknownTopicId` (100) are emitted on the right paths in `ShareGroupCoordinator`, `ConsumerGroupCoordinator`, and `NativeTransactionHandler` — librdkafka 2.4+ rebootstraps on these without driver-specific hacks. No server-side code change needed. | ~~Medium~~ Done (audit) |
| 9m | ~~**KIP-1133 Group-coordinator resilience**~~ | Audit done. KIP-1133 hardens Kafka against transient `__consumer_offsets` partition unavailability — Surgewave doesn't use that topic. Classic `OffsetStore` and the new `JsonFileGroupStateStore<T>` both write to local disk with debounced flushes; a brief disk hiccup delays a flush by one tick rather than failing a heartbeat. No equivalent failure mode to harden against; the architectural choice already side-steps the KIP. | ~~Medium~~ Done (audit) |
| 9n | ~~**KIP-947 Streams rebalance protocol generations**~~ | `StreamsGroupCoordinator.RebalanceTasks` rewritten as sticky+balance: snapshot the previous active assignment, re-assign each task to its previous owner if still present, distribute orphans to the least-loaded member, then balance until no member has more than ⌈N/M⌉ or fewer than ⌊N/M⌋ tasks. State stores stay warm across minor membership changes. 2 new sticky tests. | ~~Medium~~ Done |
| 9o | ~~**KIP-996 Raft Pre-Vote**~~ | Already implemented in `RaftNode.HandlePreVoteAsync` + `StartElectionAsync` Phase 1 (term-not-incremented pre-vote round, leader-active rejection, no follower-state change on receive). Verified by `RaftIntegrationTests.RaftNode_HandlePreVote_*`. | ~~Medium~~ Done |
| 9p | ~~**KIP-405 wire conformance audit**~~ | Audit done. `IRemoteStorageProvider` mirrors `RemoteStorageManager` (CopyLogSegmentData / FetchLogSegment / FetchIndex / DeleteSegment / ListSegments / SegmentExists). `RemoteLogSegmentMetadata` + `RemoteLogSegmentState` + `RemotePartitionDeleteState` mirror Kafka's metadata model. Surgewave's tiering is broker-internal (plugin-loaded), not exposed over a Kafka wire RPC, so external tools that talk *only* the Kafka client wire (Produce/Fetch/ListOffsets with `OffsetSpec.earliestLocalSpec`) work transparently — they never see the tiered tier. Gap: `OffsetSpec.earliestLocalSpec` (KIP-1059) on `ListOffsets` is not yet a separate code path; today Surgewave reports the same earliest offset whether the segment is local or tiered. Tracked as a follow-up. | ~~Medium~~ Done (audit) |
| 9q | ~~**Topic Hooks plugin interface (Surgewave-specific, not wire-compat)**~~ | New `ITopicLifecycleHook` plugin contract with `OnTopicCreatedAsync`, `OnTopicConfigChangedAsync`, `OnTopicDeletedAsync`. `LogManager.RegisterTopicHook` bag fires every hook in registration order; a throwing hook is logged but never aborts the operation. **Note**: this is a Surgewave-internal plugin interface (analogous to Kafka's `Authorizer` / `BrokerInterceptor` — broker-process-only, no Kafka wire surface). The earlier "KIP-1010" label in this row was inaccurate; there is no upstream Kafka KIP for topic-lifecycle hooks. 4 tests. | ~~Low~~ Done |
| 9r | ~~**KIP-985 Streams `reverseAll` / `reverseRange`**~~ | New `ReverseAll()` / `ReverseRange(from, to)` on `IReadOnlyKeyValueStore` with default impls (buffer + `.Reverse()`); `InMemoryKeyValueStore` overrides with a comparer-based descending sort. RocksDB / Sqlite / MappedFile backends inherit the default; can be specialised later for native reverse iterators. 4 tests. | ~~Low~~ Done |
| 9s | ~~**KIP-1059 ListOffsets earliest-local-spec**~~ | Closes the follow-up that was open under 9p. Surgewave's `HandleListOffsets` now recognises every reserved timestamp constant: `-1` Latest, `-2` Earliest, `-3` MaxTimestamp (KIP-734), `-4` EarliestLocal (KIP-1059), `-5` LatestTiered (KIP-1005), `-6` EarliestPendingUpload (KIP-1023). Resolution logic extracted into the static helper `DataApiHandler.ResolveListOffsetTimestamp` so it is testable without the full handler dependency graph. On a non-tiered broker `-2` and `-4` agree (both → `LogStartOffset`); `-5` and `-6` return `-1` so tiered-aware admin tools degrade gracefully. 8 tests in `ListOffsetsTimestampTests`. | ~~Medium~~ Done |
| 9t | ~~**KIP-994 ListTransactions DurationFilter + KIP-1152 TransactionalIdPattern**~~ | `ListTransactionsRequest` parser now reads `DurationFilter` at v1+ (already there) and `TransactionalIdPattern` at v2+ (new). `TransactionCoordinator.ListTransactions` accepts both filters: duration becomes a "last-activity older than X ms" cutoff; the pattern is compiled with `RegexOptions.CultureInvariant` and a 50 ms timeout, with bad patterns silently degrading to "no filter" (matches librdkafka's permissive client). The classic per-state and per-producer-id filters keep working unchanged. 6 tests in `Kip994ListTransactionsFiltersTests`. | ~~Low~~ Done |
| 9u | ~~**KIP-848 heartbeat-epoch fencing**~~ | The `ValidateMemberForOffsetOperation` fence on the OffsetCommit/Fetch v9+ path was already returning `STALE_MEMBER_EPOCH (113)` / `FENCED_MEMBER_EPOCH (110)` / `UNKNOWN_MEMBER_ID` correctly, but the same checks were never applied to `HandleConsumerGroupHeartbeat` itself — a heartbeat carrying a stale or future epoch silently inherited the current group epoch on assignment, walking the member back into reconciliation with state the broker never authorised. New `TryValidateHeartbeatEpoch` runs before any group mutation and fences three cases: non-zero epoch with unknown memberId → `UNKNOWN_MEMBER_ID`; known member with `request.epoch < member.epoch` → `STALE_MEMBER_EPOCH`; known member with `request.epoch > member.epoch` → `FENCED_MEMBER_EPOCH`. Static-membership rejoin (`epoch=0` with explicit memberId) still passes through. 5 tests in `ConsumerGroupV2HeartbeatEpochFenceTests`; existing 8 `ConsumerGroupV2CoordinatorTests` and the librdkafka 2.14 e2e (`Kip848ConsumerProtocolTests`) are non-regressed. | ~~Medium~~ Done |

### Correctness & innovation track

| # | Feature | Description | Priority |
|---|---------|-------------|----------|
| 4 | **Jepsen / Formal Correctness Testing** | Independent linearizability and consistency verification via the upstream Jepsen framework (Clojure/Knossos/Elle) run against a Surgewave `docker-compose.cluster.yml`. The in-house checker below catches anomalies in our own test runs; a real-Jepsen run is the industry-standard trust signal. | Medium |
| 4a | ~~**In-house linearizability checker**~~ | `Kuestenlogik.Surgewave.Testing.Chaos.Linearizability` ships a `History` recorder + `LinearizabilityChecker` that verifies per-partition Kafka invariants (offset-collision, divergent reads, inconsistent reads, offset gaps, potentially-lost writes). 12 synthetic unit tests prove the checker catches every injected anomaly, 3 integration tests run it against a real `ChaosCluster` (clean + latency-injected + tampered). Complements #4 — lets chaos tests fail fast on anomalies without needing the full Jepsen toolchain. | ~~Medium~~ Done |
| 8 | ~~**TLS by default on gRPC/REST endpoint (port 9093)**~~ | Phase A: `Surgewave:GrpcUseTls` toggle + `GrpcCertificatePath` / `GrpcCertificatePassword`; broker startup rewrites `Kestrel:Endpoints:Grpc:Url` to `https://*:{GrpcPort}` and lifts the Kestrel log suppression. Phase B: production default flipped — broker `appsettings.json` now ships `GrpcUseTls=true` with `https://*:9093`; Surgewave.Control + ConfigValidation page default URLs switched to `https://localhost:9093`; GrpcProducer / GrpcConsumer samples updated; 19 doc files swept with a bulk replace. Deployments (Helm / K8s / Docker Compose) reference `9093` only as port numbers, not URLs, so they were unaffected. Tests start Surgewave in-process via `SurgewaveRuntime` and never read the broker's `appsettings.json`, so integration suites continued to run against cleartext. Operator migration walk-through: [docs/deployment/tls.md](../docs/deployment/tls.md). | ~~Medium~~ Done |

### Transport & peer-to-peer hardening (QUIC follow-ups)

| Feature | Description | Priority |
|---------|-------------|----------|
| QUIC transport benchmark on real LAN/WAN | The current `Kuestenlogik.Surgewave.Benchmarks.QuicVsTcp` runs both transports on localhost. A proper benchmark should target two hosts across real LAN (sub-ms RTT) and WAN (20–50 ms RTT) with kernel-level loss injection (`tc netem` / Clumsy). | Medium |
| QUIC retransmit statistics | Expose `QuicConnection.GetStatistics()` (retransmits, RTT, congestion window) as additional Prometheus gauges for QUIC peer connections. | Low |

### Ecosystem & developer experience

| Feature | Description | Priority |
|---------|-------------|----------|
| Native Python Client (`surgewave-python`) | asyncio-based producer/consumer, Surgewave Native Protocol, pip-installable. | High |
| Native Go Client (`surgewave-go`) | Goroutine-based consumer, context-aware API, Surgewave Native Protocol. | Medium |
| Native Rust Client (`surgewave-rust`) | Zero-copy deserialization, tokio async runtime, Surgewave Native Protocol. | Low |

### Plugin SDK expansion

`Kuestenlogik.Surgewave.Sdk` is today a meta-package bundling `Kuestenlogik.Surgewave.Plugins` + `Kuestenlogik.Surgewave.Build` + `Kuestenlogik.Surgewave.Testing`. Plugin authors write one `<PackageReference Include="Kuestenlogik.Surgewave.Sdk" />` and get the full kit. Future expansions slot into the same meta-package without breaking the consumer csproj.

| Phase | Feature | Description | Priority |
|---|---------|-------------|----------|
| B | **Schema-validation as build task** | `plugin.json` is validated today only at runtime by `PluginPackageManager`. Surface the existing JSON-schema check (`schemas/plugin-manifest/v1.json`) as a `Kuestenlogik.Surgewave.Build` MSBuild task that fails the plugin csproj's build on a malformed manifest, before the `.swpkg` is packed. ~1 day. | Medium |
| C | **`surgewave plugin scaffold` + `dotnet new` templates** | Three templates registered via `dotnet new --install`: `surgewave-broker-plugin`, `surgewave-protocol-plugin`, `surgewave-storage-engine`. Each scaffolds csproj + plugin.json + a stub class + a passing test. CLI wrapper: `surgewave plugin scaffold --kind broker --name MyPlugin`. Templates live in `Surgewave.Templates`; SDK pulls them transitively. ~1 day. | Medium |
| D | **`surgewave sdk install --version X.Y.Z`** | Versioned local feed for plugin authors. CLI downloads a Surgewave-tagged release's `.nupkg` assets from GitHub Releases into `~/.surgewave/sdk/<version>/` and writes a `nuget.config` so the plugin csproj resolves `Kuestenlogik.Surgewave.Sdk@X.Y.Z` without hardcoding `C:\Projekte\Kuestenlogik\Surgewave\artifacts\pkg`. Solves the "external plugin author needs the Surgewave repo cloned" pain. Requires the release workflow to publish nupkg assets. ~2 days. | High |
| E | **Roslyn analyzers (`Kuestenlogik.Surgewave.Plugins.Analyzers`)** | 10 lint rules: `STORM001` (IBrokerPlugin should be sealed), `STORM002` (Configure must not block), `STORM003` (plugin.json class must resolve), `STORM004` (parameterless ctor required), `STORM005` (PluginId uniqueness), `STORM006` (XML doc on public API), `STORM007` (CancellationToken on async lifecycle), `STORM008` (no Surgewave internals import), `STORM009` (manifest version matches package), `STORM010` (logger injection over Console.WriteLine). Packagiert als own NuGet, gezogen transitiv durch Surgewave.Sdk. ~1 week. | Medium |
| F | **Sample plugin reference repo** | Golden examples for each plugin shape — broker, protocol, storage-engine — with full csproj, plugin.json, signed .swpkg, sample test, README walking through every choice. Lives in `Surgewave.Samples`. | Medium |

### Operator setup wizard

A separate, complementary track from the plugin SDK — the SDK is for people *building* plugins, the wizard is for people *combining* plugins.

| Variant | Where | What it does | Priority |
|---|---|---|---|
| `surgewave setup` interactive CLI | `Kuestenlogik.Surgewave.Cli` subcommand | Walks the operator through choices ("Which storage engine? Which connectors? Auth method? Telemetry?"), then emits a Bash + PowerShell script with the `surgewave plugin install` calls plus an `appsettings.json` skeleton. Output is checked into the operator's deployment repo. | Medium |
| Browser wizard | `Surgewave.Control` page | Same logic as a Blazor form with a plugin-marketplace browser; output is a downloadable ZIP with both scripts. Discoverable for non-CLI operators. | Medium |
| Dependency graph | New `Surgewave.Plugins.Marketplace` index | Pre-requisite for both variants: a registry of available plugins with their versions, descriptions, and dependency graph (e.g. "`Kuestenlogik.Surgewave.Storage.Engine.S3` requires `Kuestenlogik.Surgewave.Storage.Tiering`"). The marketplace service is partially designed; needs the dependency-graph schema. | Medium |

### Adoption & community

| Feature | Description | Priority |
|---------|-------------|----------|
| GitHub Pages deploy | Deploy combined site (Jekyll landing + DocFX technical docs + surgewave-theme) to `https://kuestenlogik.github.io/Surgewave/`. Workflow + `scripts/build-site.ps1` + Pagefind index + Jekyll/Hero/Surgewave-DocFX-Theme alle fertig; bleibt: Pages im Repo-Setting aktivieren + ersten `workflow_dispatch` triggern. | High |
| NuGet.org publish | Publish all Surgewave packages to nuget.org for public consumption. | High |
| Public repos | Make community repos public (Surgewave, Connectors, Samples, Bootcamp, Templates) — enables branch protection, public NuGet, CLA enforcement. | High |
| Branch protection | Require CI pass for external PRs, admin bypass for maintainer — needs public repos or GitHub Pro. | Medium |
| Getting started video | 5-minute demo script showcasing USPs (AI, Pipeline Editor, Schema Inference, Edge). | Medium |
| README demo screenshot | Screenshot/GIF of Control UI in README — shows Surgewave in action. | Medium |
| NuGet download badge | Add NuGet version + download count badges to README (after nuget.org publish). | Low |
| CLI quick start | Primary Quick Start via `dotnet tool install -g Kuestenlogik.Surgewave.Cli && surgewave start` (after nuget.org publish). | Low |

### Licensing & extension system

Concept document: [`concept/extension-system.md`](concept/extension-system.md). Phase 1
(licensing infrastructure) and Phase 2 (IPlugin hierarchy) are complete; see git history.

**Phase 2 follow-up — broker plugin hot-reload** — **Parked**. Connectors are already
collectible via `PluginAssemblyLoadContext(isCollectible: true)` + `PluginLoader.UnloadPlugin`,
so the 95% use case (install/update/remove a connector live) works. Broker plugins
(`IBrokerPlugin`) are a different problem: `Configure(IServiceCollection)` registers
services into the main DI container, and .NET `IServiceCollection` has no clean un-register
path. Making them hot-reloadable would require an `IBrokerPlugin` redesign with explicit
`LoadAsync`/`UnloadAsync` lifecycle, per-plugin child service providers, and tracking
every service a plugin registers — roughly a week of focused work plus thorough tests,
and a breaking change for all existing `IBrokerPlugin` implementations. Operators who
need to swap a broker plugin today restart the broker; `systemd` / Kubernetes
`restart-on-failure` + partition replication make that effectively zero-downtime.
Re-open this item if a real operator workflow needs in-place broker-plugin reload.

**Phase 3 — Complete feature gating**

| Feature | Description | Priority |
|---------|-------------|----------|
| Control UI license page | Show edition, features, expiry, installed extensions in Blazor UI. | Medium |

Charter-side items (multi-asset license generation, academic tier) live in the Charter
repository's [`TODO.md`](https://github.com/Kuestenlogik/Charter/blob/main/TODO.md#licensing-phase-3-surgewave-integration).

The originally-listed "Guard checks for Clustering/Replication/Streams/CDC in Program.cs"
item is closed as obsolete: `Kuestenlogik.Surgewave.Plugins.Licensing.SurgewaveFeatures` splits Community
(Clustering, Streams, Wasm, Cdc, Edge, ApiGraphQL, ApiGrpc, Gateway, Control, ChaosTesting,
ConnectEnterprise) from Enterprise (Replication, tiered storage, engines, AI, MultiTenancy,
DataMesh, Functions, Privacy, shared-memory transport, Operator), and enterprise features
that are `IBrokerPlugin` / `IStorageEnginePlugin` are gated automatically by
`BrokerPluginActivator` against the discovered `ILicenseProvider`. Moving
Clustering/Streams/CDC to Enterprise would be a pricing decision, not a guard-insertion
task.

---

## Example Applications

Samples live under `samples/` and are referenced from the Surgewave.Samples slnx. Existing
samples: KafkaCompatibility, ConfluentKafkaMigration, Agents, FleetTracker, SurgewaveChat,
NativeClient, IotDashboard, ConnectorPipeline, EventSourcing, MultiProtocol, RagPipeline,
DigitalTwin, MassFleetTracker — and the recent **SignedConnector** walkthrough
(keygen → publish+sign → marketplace upload → install with `--require-signed`).

---

## Notes

- Kafka 4.0 removed support for several old protocol versions
- Surgewave advertises Kafka 4.0 compatible version ranges
- Native protocol APIs are Surgewave-specific
- All required fields per Kafka JSON schemas are implemented
