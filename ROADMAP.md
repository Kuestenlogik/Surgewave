# Surgewave Roadmap

Development status and forward-looking work for the Surgewave event streaming
platform. Completed features are summarised; open and planned work carries its
full description.

> See [`docs/`](docs/) for the per-feature documentation and [CHANGELOG.md](CHANGELOG.md)
> for the release history.

---

## Current Status

**Kafka 4.0 API compatibility: 100 %** &nbsp;·&nbsp; **Kafka 4.2 surface: wired, semantics hardening in progress**

Surgewave is wire-compatible with the Confluent.Kafka .NET client (librdkafka-based)
and any other Kafka 4.0 client. Producer/consumer roundtrip, consumer group
coordination (JoinGroup, SyncGroup, Heartbeat, LeaveGroup), offset management
(OffsetCommit, OffsetFetch) and all 75 Kafka 4.0 APIs are implemented with
matching version ranges. Kafka 4.1 / 4.2 APIs (76–92) are wired at the RPC
layer; coordinator semantics for the new protocol versions are tracked under
*Open Items* below.

---

## Capability overview

A condensed view of what ships in the Apache-2.0 core. See [docs/](docs/) for
per-feature pages, configuration knobs, and worked examples.

### Protocol & transport

- **Kafka 4.0 wire compatibility** at the broker level — drop-in replacement for
  existing producers/consumers, tools, and monitoring.
- **Native protocol** — low-latency framed transport for .NET clients with a
  public [wire specification](docs/transport/native-protocol.md) so third-party
  clients in any language can implement it. Magic byte `SRWV` selects the native
  handler; anything else falls through to the Kafka handler on the same port.
- **Multi-protocol gateway** — MQTT 5.0 on port 1883, AMQP 0.9.1 on 5672,
  WebSocket produce/consume, PostgreSQL wire protocol on 5432 with
  `CREATE MATERIALIZED VIEW` support.
- **Transport plugins** — TCP (default), QUIC (HTTP/3-capable), shared-memory
  IPC for same-host clients, custom transports via `ISurgewaveTransport`.

### Storage & replication

- **Pluggable storage engines** — file system, in-memory, Apache Arrow columnar,
  RocksDB, SQLite, Parquet, LMDB, DuckDB, S3-direct, NVMe-direct (`io_uring`).
- **Tiered storage** — local hot tier with S3 / Azure Blob / GCP Cloud Storage
  cold tier, transparent across the read path.
- **Zero-disk object store mode** — broker as compute, all data lives in cloud
  object storage.
- **Multi-broker clustering** with Raft consensus (KIP-853 voter management).
- **Rack-aware replication** with hierarchical failure domains
  (Region > DC > Zone > Rack), consumer rack tracking, leader locality
  strategies, placement constraints.
- **Geo-replication** — active-passive mirroring + active-active multi-DC.
- **Cluster linking** with offset translation.

### Stream processing

- **Streams library** — all join shapes (Stream-Stream / Stream-Table /
  Table-Table / Foreign Key), CEP, EOS, watermarks, retry+backoff+circuit
  breaker, caching, named topologies, topology optimisation, multi-thread
  processing, remote interactive queries, TopologyTestDriver.
- **Streaming SQL engine** with materialised views accessible over PostgreSQL
  wire protocol.
- **State stores** — RocksDB, SQLite, MappedFile, with changelog restore.
- **Pipeline designer** (DAG editor in the Control UI) with 100+ nodes,
  versioning, live preview, alignment guides, auto-save, link labels,
  collaborative editing.

### Connectors & integration

- **Connect framework** — distributed and standalone workers, plugin system,
  MirrorMaker 2.0, pipeline error handling, pipeline nodes
  (Repartition, Priority Queue, Schema Decode, Inspector, Deduplication, Retry,
  Rate Limiter), P50/P95/P99 metrics.
- **100+ connectors** as `.swpkg` plugins — databases (Postgres CDC, MySQL CDC,
  SQL Server CDC, Oracle CDC, Mongo, Cassandra, Snowflake, BigQuery, ...),
  cloud storage (S3, Azure Blob, GCS, AWS EFS), message brokers (NATS,
  RabbitMQ, MQTT, AMQP), Kinesis, DynamoDB, CosmosDB, social (Slack, Teams,
  WhatsApp), AI (OpenAI, Anthropic, Ollama, Qdrant, Vertex), files (CSV,
  Parquet, Excel, iCal), more.
- **MassTransit rider** — drop-in `IRiderConfigurator` integration via
  `Kuestenlogik.Surgewave.Integration.MassTransit`.
- **Built-in CDC** — PostgreSQL WAL, MySQL binlog, SQL Server CT.

### Schema management

- **Schema Registry** — Confluent-compatible REST API on `:8081`, standalone
  or broker-bundled.
- **12 serialisation formats** — Avro, Protobuf, JSON, FlatBuffers, Hyperion,
  MessagePack, CBOR, Bond, Thrift, MemoryPack, Cap'n Proto, Orleans.
- **Live schema inference** — JSON Schema auto-derivation, format detection,
  auto-registration.
- **AI-assisted schema evolution** — rule-based + optional LLM, C# migration
  code generation.
- **Zero-downtime schema migration** — transparent message transformation
  between versions.
- **Schema linking** — cross-cluster schema synchronisation
  (bidirectional / export / import).

### AI & ML

- **AI pipeline node connectors** (OpenAI, Anthropic, Ollama, xAI Grok, GCP
  Vertex AI, Qdrant, pgvector) ship in `Surgewave.Connectors` as Apache 2.0.
- **ONNX ML scoring** — real-time inference inside pipeline nodes.
- **MCP server** (separately-licensed AI extension) exposing 56 tools across
  Cluster / Connector / Consumer / Pipeline / Producer / Schema / SQL / Topic
  over stdio + SSE transports.
- Advanced AI (premium) — RAG orchestration, agent runtime, guardrails,
  document processing, pipeline chat, streaming chat, multimodal.

### Operations & deployment

- **Surgewave Control UI** (Blazor Server + MudBlazor) — full feature
  management dashboard with theme toggle, 14 dashboard widgets, visual
  debugging timeline, message browser with live tail / wire format detection /
  hex dump, agent design studio (premium).
- **Operator** — Kubernetes CRD controller (`SurgewaveCluster`) with Helm chart.
- **Container images** on GHCR, **MSI / .deb / .rpm** installers,
  self-contained binaries for win / linux / macOS.
- **Cruise Control** — auto-balance partitions / leaders / disk / network.
- **Rolling upgrades** — zero-downtime leadership transfer, graceful shutdown
  orchestrator.
- **Online partition reassignment**, **broker decommission**, **online
  schema migration**, **stateless auto-scaling deployment mode**.
- **Intent-based configuration** — natural language → topic config
  (EN + DE, 16 built-in rules).
- **Edge-to-cloud sync** — embedded broker, offline buffer, delta-sync,
  bidirectional.

### Observability

- **OpenTelemetry OTLP** export (metrics, traces, logs).
- **Grafana dashboards**, **Prometheus alerts**.
- **Stream lineage / data governance** (premium).
- **Broker observability events** (`ISurgewaveBrokerObservability`) — bounded
  channel fan-out for in-process subscribers without backpressuring the
  hot path.

### Security & compliance

- **Authentication** — OAuth2/OIDC, mTLS, SAML 2.0, Azure AD/Entra ID, Okta,
  Google Workspace, LDAP/AD bind, session management, multi-IdP.
- **Authorisation** — fine-grained ACLs (Literal, Prefix, Suffix, Regex
  patterns).
- **Client quotas** (token bucket), **delegation tokens** (HMAC), **network
  bandwidth quotas**.
- **Supply-chain security** — `.swpkg` signing (ECDSA P-256 built in;
  CMS/PKCS#7 with X.509 + RFC-3161 timestamps via the premium signing
  provider), CycloneDX 1.5 SBOM in every package, marketplace upload
  enforcement, "Verified" badge in the Control UI, install-path verification.
- **Privacy-by-design** (premium) — PII detection, AES-GCM field encryption,
  right-to-erasure.

### Reliability semantics

- **Exactly-once semantics** — clustered coordination, WriteTxnMarkers
  replication, cross-topic transactions (two-phase commit, auto-abort on
  timeout), EOS source connectors.
- **Queue semantics (QueueView)** — visibility timeout, Ack/Nack/Reject, DLQ,
  MaxDeliveryCount, REST API, AMQP-QueueView integration.
- **Dead Letter Queue** — broker-native Nack, retry backoff, auto-creation.
- **Ephemeral topics**, **delayed delivery**, **per-message TTL**,
  **broker-level deduplication**.

### Multi-tenancy & data products

- **Namespace-level multi-tenancy** — `tenant/namespace/topic`, quotas,
  admin delegation.
- **Low-code data mesh** — topics as data products, SLOs, data contracts,
  quality monitoring.

### Developer experience

- **Unified `ISurgewaveClient`** (SurgewaveNative / Kafka / Auto switch),
  Confluent.Kafka drop-in compatibility, async serialisers, batch offset
  commit, producer/consumer interceptors, priority lanes.
- **Channel-based extensions** — `consumer.AsChannelReader()`,
  `consumer.ToAsyncEnumerable()`, `producer.AsProducerChannel()` for idiomatic
  `await foreach` loops.
- **Source generators** — `[SurgewaveSchema(Topic="…")]` emits per-type codec
  + producer/consumer extensions, zero-reflection, AOT-ready.
- **70+ CLI commands** (`Kuestenlogik.Surgewave.Cli`), available as a global
  `dotnet tool`. Consistent verb aliases (`show` / `ls`), confirmation
  prompts on destructive commands with global `--yes/-y`.
- **`Kuestenlogik.Surgewave.Sdk`** meta-package — plugin contracts, MSBuild
  build tasks for `.swpkg` packaging, embedded-runtime test fixtures.
- **`SurgewaveRuntime.CreateBuilder()`** embedded fixture for in-process
  tests.
- **6 `dotnet new` templates** in the `Surgewave.Templates` repo
  (producer / consumer / worker / aspnet / streams / connect).
- **Surgewave Bootcamp** — 34 hands-on units across 8 learning paths in the
  `Surgewave.Bootcamp` repo.

---

## v0.1.0 — initial public release

The Apache-2.0 core ships in version `0.1.0` (NuGet:
`Kuestenlogik.Surgewave.Client` and friends). All capabilities listed above
are present; the public benchmark numbers and a few quality-of-life polish
items land before `1.0.0` (see *Open Items*).

---

## Testing

| Area | Tests | Status |
|---|---|---|
| Kafka protocol & Confluent.Kafka integration | 28 + 75 + ~200 | Passing |
| Native protocol | ~100 | Passing |
| Tiered + Arrow + cloud object storage | 80+ | Passing |
| Raft / clustering / replication | 127 | Passing |
| Kafka Streams (joins, EOS, IQ, SQL) | 196 | Passing |
| Kafka Connect + transform nodes | 151 | Passing |
| Schema Registry (12 formats, evolution, linking) | 80+ | Passing |
| Plugins (packaging, repository, signing, SBOM) | 440 | Passing |
| Connectors (~30 connector test suites) | 3500+ | Passing |
| Security (ACL, OAuth2, mTLS, transactions) | 70+ | Passing |
| QueueView (visibility timeout, Ack/Nack/Reject, DLQ) | 46 | Passing |
| Resilience patterns (CB / Retry / Bulkhead) | 27 | Passing |
| Chaos / linearizability | 60+ | Passing |
| CLI integration | 14 | Passing |

**Total: 5,000+ tests passing in the core repo, plus the per-extension test
suites in `Surgewave.Connectors` and the other sister repositories.**

CI enforces **70 % line / 60 % branch** coverage on the core via
`.github/workflows/coverage.yml`. The `Kuestenlogik.Surgewave.Plugins.*`
assemblies sit at 89 %+ line coverage as of this release.

---

## Performance

Public head-to-head benchmark results land alongside the `1.0` release.
Internal regression baselines live under `benchmarks/baselines/` and are
exercised by `.github/workflows/benchmark-regression.yml` so drift is caught
on every PR. The benchmark suite (`Kuestenlogik.Surgewave.Benchmarks.*`)
covers Surgewave in embedded / standalone / container deployments across
native and Kafka protocols.

Reproducible scenarios already running in CI:

- Throughput regression on the embedded broker (`Benchmarks.Throughput`)
- QUIC vs TCP transport (`Benchmarks.QuicVsTcp`)
- Replication and peer-transport overhead (`Benchmarks.RealWorld`)
- 8-platform broker comparison (`Benchmarks.Comparison`)

Comparative numbers vs Kafka, Redpanda, and others are intentionally
omitted until a documented head-to-head run on identical hardware is
published alongside the release.

---

## Open Items

Forward-looking work tracked at the feature level. Each item is independently
scoped; ordering is illustrative and may shift with operator feedback.

### Planned enhancements

| # | Gap | Effort | Priority |
|---|---|---|---|
| G1 | **Native non-.NET clients** (Python, Go, Rust) — broadens client-language reach beyond .NET so the Kafka-compatibility story covers the full Kafka client ecosystem. | Multi-week per language | High |
| G3 | **Public benchmarks** on identical hardware — Surgewave native and Kafka wire on the same broker, full configuration disclosed. | Days (hardware + repeatability) | High |
| G4 | **Real Jepsen run** — the in-house `LinearizabilityChecker` is in; an external Jepsen run is the industry trust stamp. | ~1 week setup + run + report | Medium |
| G12 | **Cluster-linking-grade geo-replication** — verify parity with Confluent Cluster Linking (active-active offset translation, conflict resolution). The active-active replication extension exists; gap audit pending. | Days (audit) + variable (fix) | Medium |
| G15 | **CLI polish — remaining** — verb aliases, confirmation prompts, and the `--format plain` audit are largely done; ~22 commands still need a `--format plain` branch instead of falling through to the table renderer. Help-text consistency + tab-completion coverage sweep round out the item. | Continuous | Low-Medium |
| G17 | **Flink connector** — analytical bridge for Surgewave topics into Flink jobs. | ~1 week | Low-Medium |
| G18 | **Basic AI primitives in the Apache-2.0 core** — five basic AI pipeline nodes (`LlmChatNode`, `EmbeddingNode`, `VectorStoreReadNode`, `VectorStoreWriteNode`, `PromptTemplateNode`) plus a `SingleShotRagNode` (retrieve→augment→generate in one node) move into the open core so a minimal AI pipeline runs without any commercial extension. Advanced AI (agent framework, multi-step RAG orchestration, guardrails, document processing, pipeline-chat UI, streaming-chat infrastructure, RAG eval, multimodal) stays in the separately-licensed AI extension. | ~3–5 days | Medium-High |
| G20 | **Adaptive compression per topic — broker integration** — the `AdaptiveCompressionSampler` component is in `Kuestenlogik.Surgewave.Core.Util`; the broker hot-path wiring (`compression.type=auto` topic config, per-auto-topic sampler in `DataApiHandler.HandleProduceAsync`, decision write-back via `AlterConfigs`) is the remaining work. | ~2–3 days | Medium-High |
| G21 | **Disaggregated compute/storage mode** — broker as pure coordination/metadata layer, producer writes straight to object storage without ISR replication, reader fetches from object storage. A recognised direction in the wider broker market (alternatives that operate this way typically report substantial savings in storage and cross-AZ traffic versus ISR-replicated architectures). Builds on the existing `Storage.Engine.S3`. Topic-level opt-in (`storage.mode=disaggregated`). | ~4–6 weeks | High |
| G23 | **Pipeline-as-code (C# DSL)** — alongside the visual pipeline editor: a declarative C# DSL, e.g. `Pipeline.From<OrderEvent>("orders").Filter(o => o.Amount > 1000).Enrich(LoadCustomer).To<HighValueOrder>("high-value")`. Roslyn hot-reload on save. Pipelines become code-review-able, git-versionable, refactorable. The visual editor stays for prototyping and the non-developer audience. | ~2–3 weeks | Medium |
| G24 | **Lineage-driven impact analysis** — `AlterSchema` walks the `IStreamLineageRegistry`, computes all downstream pipelines, checks schema compatibility against each downstream schema, blocks with a list of affected pipelines on incompatibility. Today schema evolution is "fingers crossed"; this turns it into a CI / pre-deploy check with `--force` override for emergencies. | ~1–2 weeks | Medium |
| G25 | **Vector type as first-class schema primitive** — `vector(dim=768, dtype=f32)` as a native Avro/Protobuf/JSON type so producers write embeddings as a typed field instead of opaque `bytes`. Schema Registry validates dim/dtype compatibility. RAG pipelines read `record.Embedding` instead of `MemoryMarshal.Cast<byte, float>(...)`. Makes Surgewave the natural choice for AI workloads. | ~1–2 weeks | Medium-High |
| G26 | **AI pipeline cost tracking** — per-token / per-inference / per-MB cost telemetry per AI pipeline node, aggregated per pipeline + tenant in an internal `_ai_costs` topic. Budget alerts (`Surgewave:Ai:CostBudget=$500/day`) block pipeline execution on overrun with an override workflow. Without this, per-pipeline AI cost is opaque and cost control of long-running AI workloads is difficult to operate. | ~1–2 weeks | High |
| G27 | **Cold-start auto-tune — service integration** — the profiler and recommender (`ColdStartWorkloadProfiler` + `ColdStartTuningRecommender` in `Broker.AutoTuning`) are in; the remaining work is the `AutoTuningService` wiring (24h-window lifecycle, hook in `DataApiHandler.HandleProduceAsync` + replication path for live observations, write-back to `auto-tuned.json` with operator audit trail, optional `Surgewave:AutoTune:ColdStart:AutoApply=true`). | ~3–4 days | Medium |

### Correctness & innovation track

| # | Feature | Description | Priority |
|---|---|---|---|
| 4 | **Jepsen / formal correctness testing** | Independent linearizability and consistency verification via the upstream Jepsen framework (Clojure/Knossos/Elle) run against a Surgewave `docker-compose.cluster.yml`. The in-house `LinearizabilityChecker` catches anomalies in our own test runs; a real Jepsen run is the industry-standard trust signal. | Medium |

### Transport & peer-to-peer hardening (QUIC follow-ups)

| Feature | Description | Priority |
|---|---|---|
| QUIC transport benchmark on real LAN/WAN | The current `Benchmarks.QuicVsTcp` runs both transports on localhost. A proper benchmark should target two hosts across real LAN (sub-ms RTT) and WAN (20–50 ms RTT) with kernel-level loss injection (`tc netem` / Clumsy). | Medium |
| QUIC retransmit statistics | Expose `QuicConnection.GetStatistics()` (retransmits, RTT, congestion window) as additional Prometheus gauges for QUIC peer connections. | Low |

### Ecosystem & developer experience

| Feature | Description | Priority |
|---|---|---|
| Native Python client (`surgewave-python`) | asyncio-based producer/consumer, Surgewave Native Protocol, pip-installable. | High |
| Native Go client (`surgewave-go`) | Goroutine-based consumer, context-aware API, Surgewave Native Protocol. | Medium |
| Native Rust client (`surgewave-rust`) | Zero-copy deserialisation, tokio async runtime, Surgewave Native Protocol. | Low |

### Plugin SDK expansion

`Kuestenlogik.Surgewave.Sdk` is a meta-package bundling
`Kuestenlogik.Surgewave.Plugins` + `Kuestenlogik.Surgewave.Build` +
`Kuestenlogik.Surgewave.Testing`. Plugin authors write one
`<PackageReference Include="Kuestenlogik.Surgewave.Sdk" />` and get the full
kit. Future expansions slot into the same meta-package without breaking
existing consumers.

| Phase | Feature | Description | Priority |
|---|---|---|---|
| B | **Schema-validation as build task** | Surface the existing JSON-Schema check (`schemas/plugin-manifest/v1.json`) as a `Kuestenlogik.Surgewave.Build` MSBuild task that fails the plugin csproj's build on a malformed manifest, before the `.swpkg` is packed. | Medium |
| C | **`surgewave plugin scaffold` + `dotnet new` templates** | Three templates registered via `dotnet new --install`: `surgewave-broker-plugin`, `surgewave-protocol-plugin`, `surgewave-storage-engine`. Each scaffolds csproj + plugin.json + a stub class + a passing test. | Medium |
| D | **`surgewave sdk install --version X.Y.Z`** | Versioned local feed for plugin authors. CLI downloads a Surgewave-tagged release's `.nupkg` assets from GitHub Releases into `~/.surgewave/sdk/<version>/` and writes a `nuget.config` so the plugin csproj resolves `Kuestenlogik.Surgewave.Sdk@X.Y.Z` without hardcoding a local path. | High |
| E | **Roslyn analysers (`Kuestenlogik.Surgewave.Plugins.Analyzers`)** | 10 lint rules with `SRWV`-prefix (in sync with the native-protocol magic byte): `SRWV001` IBrokerPlugin should be sealed, `SRWV002` Configure must not block, `SRWV003` plugin.json class must resolve, `SRWV004` parameterless ctor required, `SRWV005` PluginId uniqueness, `SRWV006` XML doc on public API, `SRWV007` CancellationToken on async lifecycle, `SRWV008` no Surgewave-internals import, `SRWV009` manifest version matches package, `SRWV010` logger injection over `Console.WriteLine`. Shipped as its own NuGet, pulled transitively through the SDK. | Medium |
| F | **Sample plugin reference repo** | Golden examples for each plugin shape — broker, protocol, storage engine — with full csproj, plugin.json, signed `.swpkg`, sample test, README walking through every choice. Lives in `Surgewave.Samples`. | Medium |

### Operator setup wizard

A separate, complementary track from the plugin SDK — the SDK is for people
*building* plugins, the wizard is for people *combining* plugins.

| Variant | Where | What it does | Priority |
|---|---|---|---|
| `surgewave setup` interactive CLI | `Kuestenlogik.Surgewave.Cli` subcommand | Walks the operator through choices ("Which storage engine? Which connectors? Auth method? Telemetry?"), then emits a Bash + PowerShell script with the `surgewave plugin install` calls plus an `appsettings.json` skeleton. Output is checked into the operator's deployment repo. | Medium |
| Browser wizard | `Surgewave.Control` page | Same logic as a Blazor form with a plugin-marketplace browser; output is a downloadable ZIP with both scripts. Discoverable for non-CLI operators. | Medium |
| Dependency graph | New `Surgewave.Plugins.Marketplace` index | Pre-requisite for both variants: a registry of available plugins with their versions, descriptions, and dependency graph. | Medium |

### Adoption & community

| Feature | Description | Priority |
|---|---|---|
| GitHub Pages deploy | Combined Jekyll landing + DocFX technical docs at `https://surgewave.io` (CNAME ready, workflow + Pagefind index in place). | High |
| NuGet.org publish | All Surgewave packages live on nuget.org for public consumption. | High |
| Public sister repos | `Surgewave.Connectors`, `Surgewave.Samples`, `Surgewave.Bootcamp`, `Surgewave.Templates` move to public — enables branch protection, public NuGet, CLA enforcement. | High |
| Branch protection | Require CI pass for external PRs, admin bypass for maintainer. | Medium |
| Getting-started video | 5-minute demo showcasing the USPs (AI, pipeline editor, schema inference, edge). | Medium |
| NuGet download badge | Add NuGet version + download count badges to README (after nuget.org publish). | Low |
| CLI quick start | Primary Quick Start via `dotnet tool install -g Kuestenlogik.Surgewave.Cli && surgewave start`. | Low |

### Licensing & extension system

Phase 1 (licensing infrastructure) and Phase 2 (`IPlugin` hierarchy) are
complete; see git history.

**Phase 2 follow-up — broker plugin hot-reload** — *Parked*. Connectors are
already collectible via `PluginAssemblyLoadContext(isCollectible: true)` +
`PluginLoader.UnloadPlugin`, so the 95 % use case (install/update/remove a
connector live) works. Broker plugins (`IBrokerPlugin`) are a different
problem: `Configure(IServiceCollection)` registers services into the main DI
container, and .NET `IServiceCollection` has no clean un-register path. Making
them hot-reloadable would need an `IBrokerPlugin` redesign with explicit
`LoadAsync`/`UnloadAsync` lifecycle, per-plugin child service providers, and
tracking every service a plugin registers — roughly a week of focused work
plus thorough tests, and a breaking change for all existing `IBrokerPlugin`
implementations. Operators who need to swap a broker plugin today restart the
broker; `systemd` / Kubernetes `restart-on-failure` + partition replication
make that effectively zero-downtime. Re-open this item if a real operator
workflow needs in-place broker-plugin reload.

**Phase 3 — feature gating**

| Feature | Description | Priority |
|---|---|---|
| Control UI license page | Show edition, features, expiry, installed extensions in the Blazor UI. | Medium |

Each plugin self-declares its licence tier via `IBrokerPlugin.RequiresLicense`
(default `false`). `BrokerPluginActivator` consults the discovered
`ILicenseProvider` against `plugin.FeatureId` only for plugins where
`RequiresLicense` is `true`. The core makes no enumeration of which feature
ids are licensed — that decision lives in each extension's own repository.
When no licence provider is registered, the broker runs in community mode:
plugins with `RequiresLicense = false` load unconditionally, plugins that
require a licence are skipped with a warning.

---

## Example applications

Samples live in the separate `Surgewave.Samples` repository (sibling clone
alongside this one). The roster includes Kafka compatibility, Confluent.Kafka
migration, agents, fleet tracker, chat, native client, IoT dashboard,
connector pipeline, event sourcing, multi-protocol, RAG pipeline, digital
twin, mass fleet tracker, and the **SignedConnector** walkthrough
(keygen → publish+sign → marketplace upload → install with `--require-signed`).
See the Samples repo's README for the up-to-date list and run instructions.

---

## Notes

- Kafka 4.0 removed support for several old protocol versions; Surgewave
  advertises Kafka 4.0 compatible version ranges.
- Native protocol APIs are Surgewave-specific and documented at
  [`docs/transport/native-protocol.md`](docs/transport/native-protocol.md).
- All required fields per Kafka JSON schemas are implemented.
