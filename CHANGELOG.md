# Changelog

All notable changes to Surgewave will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.13] - 2026-06-08

### Fixed
- **Leader-Reelection-Latenz nach Broker-Shutdown (G28)** — `ClusterController.ElectLeaderAsync` rief den `LeaderAndIsr`-Broadcast an die Replica-Brokers fire-and-forget auf (`_ = SendLeaderAndIsrAsync(...)`). Folge: der Controller's `_clusterState` hatte den neuen Leader sofort, aber die anderen Brokers brauchten mehrere Sekunden bis ihr lokaler `_clusterState` aktualisiert war. Da `MetadataApiHandler` direkt aus `_clusterState` liest, lieferten Metadata-Requests an Followers für mehrere Sekunden den toten alten Leader — Producer connecteten ins Leere und timeoute mit `Local: Message timed out`. Beide Aufrufstellen (Z. 667 + 885 in `ClusterController.cs`) jetzt awaited. Tote Replicas blockieren den `Task.WhenAll`-Broadcast nicht, weil `SendLeaderAndIsrToBrokerAsync` Connection-Refused-Exceptions in einem try/catch fängt. `ReplicationTests.Cluster_BrokerShutdown_RemainingBrokersContinue` ist wieder aktiviert (Skip-Marker aus 0.1.12 entfernt).

## [0.1.12] - 2026-06-06

### Added
- **Adaptive Compression — broker integration (`compression.type=auto`)** — neuer `IBrokerPlugin` (`Surgewave.AdaptiveCompression`, opt-in via `Surgewave:AdaptiveCompression:Enabled=true`) startet einen `AdaptiveCompressionService` als `BackgroundService`. Der Service enumeriert periodisch alle Topics mit `compression.type=auto`, liest die letzten Batches je Partition über `LogManager.ReadBatchesAsync`, dekomprimiert sie und füttert einen per-Topic `AdaptiveCompressionSampler`. Sobald genug Samples vorliegen, schreibt der Service den gewählten Codec via `LogManager.UpdateTopicConfig` zurück und entfernt den Sampler — der Produce-Hot-Path bleibt unangetastet. Verdrahtet G20 aus der Roadmap.
- **Cold-Start Auto-Tune — Service-Integration** — der bislang headless `ColdStartWorkloadProfiler` + `ColdStartTuningRecommender` werden jetzt über den `SurgewaveAutoTuningBrokerPlugin` lebendig: opt-in via `Surgewave:AutoTune:ColdStart:Enabled=true`. Neuer `ColdStartAutoTuneService` (`BackgroundService`) hält den Profiler, wartet auf den Ablauf des `ObservationWindow` (Default 24 h), baut das `WorkloadProfile`, ruft den Recommender, persistiert den Audit-Trail nach `auto-tuned.json` und (bei `AutoApply=true`) wendet die Empfehlungen direkt über `DynamicBrokerConfig` an. `DataApiHandler.HandleProduceAsync` füttert den Profiler allokationsfrei pro erfolgreich appendetem Batch. Verdrahtet G27.

## [0.1.11] - 2026-06-02

### Fixed
- **Native-Wire-Linux-Deadlock im single-message `SendAsync(string, string)`-Pfad** — der string-Overload aus dem 0.1.7 Header-Refactor hat den Pflicht-Header-Block (`int32 count` nach Value) vergessen. Server las dann garbage als header-count und blieb im Produce-Ack hängen; auf dem Linux-CI deadlockten alle `Interop_*`-Tests und `Performance_NativeVsKafka_*`-Tests in `NativeProtocolIntegrationTests`. Fix: 4-Byte-Empty-Block anhängen, identisch zum byte[]-Overload.
- **`SurgewaveRuntime`-Default ist jetzt deterministisch IPv4-Loopback** — bisher war Host `"localhost"` + `EnableDualMode = true`. Auf Linux liefert `getaddrinfo("localhost")` typischerweise `::1` (IPv6) zuerst; Confluent.Kafka-Clients trafen je nach Container-Konfig auf "Connection refused" trotz Dual-Stack-Listen. Neuer Default: `Host = "127.0.0.1"`, `EnableDualMode = false`. Wer Dual-Stack braucht, opt-in via `.WithDualMode()` — das setzt den Host auch wieder auf `"localhost"`, damit Clients beide Stacks probieren können.
- **Kafka-v2-RecordBatch-Header sind zigzag-signed varints** — `WriteHeadersFromNativeBlock` schrieb sie als raw unsigned varint (intern konsistent, aber Confluent.Kafka dekodiert zigzag und sah falsche Header-Counts → Stream-Resync-Loop). Sowohl Write-Pfad als auch die drei Read-Pfade (`StreamRecordsToWriter`, `StreamBatchRawToWriter`, `parseRecord`) jetzt mit `ZigzagEncode`/`ZigzagDecode`.

### Changed
- **CI: `--blame-hang-timeout 300s` + `--blame-hang-dump-type full`** in `ci.yml` + `release.yml`. Bei hängendem Test wird der Test-Host nach 5 Min Inaktivität gekillt, ein Full-Memory-Dump erzeugt und als Artefakt hochgeladen. Damit gibt's fail-fast statt 6h Workflow-Timeout, plus thread-trace-fähige Diagnose.
- **CI: `dotnet test -m:1`** — MSBuild serialisiert die Test-Assemblies. Verhindert Port-/Process-Exhaustion auf 4-vCPU-Linux-Runnern, wenn mehrere Test-Assemblies gleichzeitig in-process broker hochziehen.

## [0.1.10] - 2026-05-31

### Changed
- **Broker package renamed to `Kuestenlogik.Surgewave.Broker`** — previously the NuGet package shipped with id `surgewave-broker` because the csproj only set `<AssemblyName>` and MSBuild fell back to it as PackageId. Now an explicit `<PackageId>Kuestenlogik.Surgewave.Broker</PackageId>` brings the package in line with the rest of the namespace (Bowire pattern: kebab assembly name for the linux binary and container image, dotted PackageId for the NuGet catalogue). Container repository (`kuestenlogik/surgewave-broker`), linux package names (`.deb`/`.rpm`), and the on-disk binary are unchanged. Consumers who referenced `surgewave-broker` transitively (e.g. via `Kuestenlogik.Surgewave.Runtime`) get the new id automatically on next restore; the old `surgewave-broker` versions 0.1.0–0.1.9 remain published but unlisted.

## [0.1.9] - 2026-05-30

### Changed
- **Release workflow excludes `Connect.Eos.Tests`** — the suite passes locally on Windows (16 s) but hangs the Linux GitHub-Actions runner after the 0.1.7 native-header wire change. Filtered alongside `IntegrationTests` in `release.yml`; the full set still runs on every push via `ci.yml`. Investigation tracked separately.

## [0.1.8] - 2026-05-30

### Fixed
- **`TelemetryTopicSink` uses the native header block layout** — `BuildHeaders` previously hand-rolled Kafka-style zigzag-varint headers and stuffed them straight into `Message.Headers`. After 0.1.7 the broker treats that field as the native block (int32 count + int32-prefixed pairs) and re-encodes it on the way into Kafka, so the old layout was misread as a giant header count and crashed `SerializeMessagesPooled`. Builder now reuses `NativeMessageHeaderCodec.Encode`.

## [0.1.7] - 2026-05-30 (failed release — workflow aborted on the Telemetry test above)

### Added
- **Per-message headers on the native wire protocol** — `SendBuilder.WithHeader(...)` / `SendBatchAsync` accept a header dictionary that now travels end-to-end through the native pipe. The broker carries the block verbatim onto the Kafka RecordBatch headers section, and `ReceiveAsync` returns them on `ReceivedMessage.Headers`. Previously the entire produce path silently dropped headers, which made plugins that rely on them (e.g. Akka.Persistence's seq-nr / writer-uuid / manifest envelope) unusable over the native protocol. Wire format is a single self-contained block (`int32 count` + `int32`-prefixed key/value pairs) appended after the value; new `NativeMessageHeaderCodec` shared between sender and receiver. Breaking change for native-wire consumers.

### Fixed
- **`SurgewaveMessagingOperations.ReceiveAsync` tolerates empty/null values** — the broker encodes `value.Length == 0` as `-1` on the wire (Kafka null-tombstone convention); the client now reads negative lengths as `Array.Empty<byte>()` instead of crashing in `ReadRaw(-1)` with `ArgumentOutOfRangeException`. Symptom: every consumer that saw an Akka-snapshot-style tombstone record failed with `LoadSnapshotFailed`.

## [0.1.6] - 2026-05-30

### Changed
- **gRPC / REST endpoint binds HTTPS by default** — broker `appsettings.json` now ships `Surgewave:GrpcUseTls=true` and `https://*:9093` in the Kestrel Grpc section. Run `dotnet dev-certs https --trust` once to trust the ASP.NET Core dev cert, or configure `Surgewave:GrpcCertificatePath` for a production cert. `Surgewave.Control` + the `ConfigValidation` page now default to `https://localhost:9093`; `GrpcProducerExample` / `GrpcConsumerExample` snippets updated; 19 documentation files swept. Operators who need cleartext for a transitional period can set `Surgewave:GrpcUseTls=false`. Tests are unaffected (they start Surgewave in-process via `SurgewaveRuntime`, not the broker's `appsettings.json`).

### Added
- **S3-only ("zero-disk") broker mode** — `Kuestenlogik.Surgewave.Storage.Engine.S3` ships `S3StorageEngine` that writes batches directly to S3 via the AWS SDK (batched flush, in-memory read cache, JSON index per segment). `S3LogSegmentFactory` plugs into `ILogSegmentFactory`; `SurgewaveRuntimeBuilder.WithS3Storage("bucket")` and `WithS3StorageLocalStack(endpoint, "bucket")` make the toggle a one-liner. Operators get the WarpStream/AutoMQ-style cloud-economics shape: no local disk, infinite cheap retention, multi-AZ without inter-AZ disk replication. The mode was already implemented in code but undocumented; this surfaces it in the gap analysis (G5) as Done. 3 new `S3StorageExtensionsTests`.
- **KIP-848 reconciliation + assignor selection** — `ConsumerGroupV2Coordinator` now respects `request.ServerAssignor` (resolved through `PartitionAssignorFactory`: `range`, `roundrobin`, `sticky`, `cooperative-sticky`; unknown names safely fall back to `range`). A new `ConsumerGroupV2Reconciler` advertises only the subset of a member's target that no other member still reports owning, so a partition is never handed to its new owner before the previous one revokes. Members carry an explicit `OwnedTopicPartitions` list, sourced from the heartbeat request. `ConsumerGroupDescribe` reports `Stable`/`Reconciling`/`Empty` based on whether every member's owned set matches its target. 7 new `ConsumerGroupV2CoordinatorTests`.
- **KIP-932 Renew semantics** — new `IQueueView.ExtendVisibility(messageId, extension)` extends a message's lease in place without bumping `DeliveryCount`; `ShareGroupCoordinator` routes `AcknowledgeType=4` (Renew) to it instead of aliasing to Nack+requeue. The two `ProcessAcknowledgementBatches` overloads now share a single generic implementation via the internal `AckBatch` shape. 3 new `QueueViewTests.ExtendVisibility*`.
- **TLS toggle for the gRPC / REST endpoint** — `Surgewave:GrpcUseTls` in `appsettings.json` flips the broker's `:9093` endpoint between HTTP and HTTPS without editing the Kestrel section. Optional `Surgewave:GrpcCertificatePath` + `Surgewave:GrpcCertificatePassword` wire up production certs; without them, the ASP.NET Core dev cert is used. `BrokerConfig.Validate` rejects a cert path without TLS enabled and a missing cert file. The Kestrel log-level suppression for cleartext HTTP/2-without-ALPN is lifted automatically when TLS is on. Walk-through: [docs/deployment/tls.md](docs/deployment/tls.md).
- **In-house linearizability checker** — `Kuestenlogik.Surgewave.Testing.Chaos.Linearizability` ships a thread-safe `History` recorder (Produce/Consume invoke/ok/fail events) and a `LinearizabilityChecker` that verifies per-partition Kafka invariants: offset-collision (two different values acknowledged at the same offset), divergent reads (consumer disagrees with acknowledged produce), inconsistent reads (two consumers disagree at same offset), offset gaps inside acknowledged ranges, and potentially-lost writes that reads went past. 12 synthetic unit tests + 3 `ChaosCluster`-backed integration tests (clean run, latency-injected run, tampered history regression guard). Complements the planned real-Jepsen integration — chaos tests now fail fast on correctness anomalies without waiting for the full Jepsen/Clojure toolchain.
- **Broker observability hot-path gate** — `ISurgewaveBrokerObservability.HasSubscribers` is a lock-free property backed by a volatile subscriber-count in `SurgewaveBrokerObservability`; broker pipeline code checks it before constructing `SurgewaveBrokerEvent` so unused observability paths pay zero allocation cost. Config-driven via `Surgewave:Observability:Enabled` (default true) — when false, `AddSurgewaveBrokerObservability` skips registration entirely. `SubscriberCapacity` is also configurable. 6 new tests in `ObservabilityHasSubscribersTests` on top of the existing 5.
- **CycloneDX SBOM in .swpkg packages** — every packed `.swpkg` now ships `sbom.json` at the archive root listing the plugin's own assemblies and transitive dep DLLs with SHA-256 hashes and purl identifiers. The marketplace stores the SBOM on upload and serves it at `GET /api/v1/packages/{id}/{version}/sbom` with `application/vnd.cyclonedx+json`; `PackageMetadata.HasSbom` reflects presence. Signing + SBOM together give operators both "who signed" and "what's inside" per install. See the SBOM section of [docs/security/plugin-signing.md](docs/security/plugin-signing.md).
- **Plugin signing end-to-end** — `.swpkg` packages can be signed with ECDSA P-256 (built-in, zero-dep) or CMS/PKCS#7 with X.509 + RFC-3161 timestamps (Sealbolt provider in Surgewave.Licensing). CLI: `surgewave plugins keygen/sign/verify/trust`. MSBuild: `-p:SurgewaveSigningKey=...`. Broker install verifies against `Surgewave:Plugins:Signer` config. Marketplace upload verifies against `Surgewave:Marketplace:Signing` config and stores the sidecar. Surgewave.Control shows a "Verified" badge on signed packages. Pluggable via `ISppSignerProvider`, runtime-discovered via `PluginPackageSignerRegistry` with isolated `AssemblyLoadContext`. Shared contract tests at `Kuestenlogik.Surgewave.Plugins.Packaging.Testing`. See [docs/security/plugin-signing.md](docs/security/plugin-signing.md) for the full trust-chain walkthrough.
- **Surgewave-over-QUIC end-to-end** — raw QUIC transport for client↔broker (`Kuestenlogik.Surgewave.Transport.Quic`, `SurgewaveTransportType.Quic`), broker QUIC adapter (`Kuestenlogik.Surgewave.Protocol.Quic`, ALPN `surgewave/1`), gRPC over HTTP/3 (`https://*:9094`). Auto-detect Surgewave-native vs Kafka via first 4 bytes on each QUIC stream.
- **QUIC inter-broker transport** — `IPeerTransport` / `IPeerConnection` / `IPeerListener` abstraction in `Kuestenlogik.Surgewave.Transport`, with TCP and QUIC implementations. All clustering call sites migrated: Raft RPC, ReplicationServer, ReplicaFetcher, ConnectionPool, ClusterLink (geo-replication). Switch via `Surgewave:InterBrokerTransport=quic`.
- **Inter-broker mTLS over QUIC** — shared-CA model: `Surgewave:InterBrokerCertificatePath` + `Surgewave:InterBrokerCaCertificatePath` enable mutual certificate validation. `TrustAllCertificates` stays as a dev fallback with a prominent startup warning.
- **Multi-stream QUIC peer connections** — `IPeerStreamLease` abstraction with `AcquireStreamAsync` (client) and `AcceptInboundStreamAsync` (server). QUIC opens true per-RPC streams; TCP serialises via lock. RaftTransport migrated. ReplicationServer now does per-stream fan-out for concurrent RPC handling.
- **Peer-transport Prometheus metrics** — `PeerTransportMetrics` (`surgewave.transport.peer.*`): connections opened/closed/active, streams opened/closed, bytes sent/received, errors. Tagged by transport type (tcp/quic).
- **Surgewave.Edge QUIC support** — `EdgeBrokerBuilder.WithCloudTransport(SurgewaveTransportType.Quic)` + `EdgeSyncConfig.CloudTransport`. Edge quickstart doc at `docs/guides/edge-quic-quickstart.md`.
- **Control UI peer-transport visibility** — BrokerList.razor shows per-broker TCP/QUIC chip; proto `BrokerInfo.peer_transport` field (tag 5).
- **Transport A/B benchmark** — `Kuestenlogik.Surgewave.Benchmarks.QuicVsTcp` with configurable packet-loss proxy (direct + proxied paths). Results: TCP 361k msg/s baseline, QUIC 341k msg/s direct (−5%), QUIC matches TCP throughput at 5% loss + 10ms RTT.
- **Network impairment testing** — `Kuestenlogik.Surgewave.Testing.Network` (LossyUdpProxy, LossyTcpProxy), `NetworkLossScenario` in `Kuestenlogik.Surgewave.Testing.Chaos`.
- **msquic fallback** — `PeerTransportFactory.CreateWithFallback` detects missing msquic and downgrades to TCP with clear warning.
- 12 serialization format handlers (Hyperion, MessagePack, CBOR, Bond, Thrift, MemoryPack, Cap'n Proto, Orleans)
- Content-type detection + deserialization in Message Browser
- `surgewave message get` CLI command with pipe support
- Message download buttons (Raw + JSON) in Control UI
- Plugin Repository Sources (NuGet, HTTP/Marketplace, GitHub Releases)
- Plugin Sources management page in Control UI
- `surgewave` CLI as dotnet tool (`dotnet tool install -g Kuestenlogik.Surgewave.Tool`)
- Kuestenlogik.Surgewave.Sdk MSBuild package (auto .swpkg on publish)
- Plugin Development Guide (docs/features/plugin-development.md)
- Serializer benchmarks (MemoryPack 3.5x faster than JSON)
- publish.ps1 — builds the solution, packs NuGets and publishes self-contained executables (Broker, Gateway, Control, Marketplace, Connector, Cli) to artifacts/pub/. Companion scripts start.ps1 / stop.ps1 launch and gracefully shut down the published Broker/Gateway/Control/Marketplace set in separate console windows. `start.ps1` additionally adds the `surgewave` CLI to `$env:PATH` for the current PowerShell session when dot-sourced.
- GitHub Actions CI for all 17 repos
- GitHub Packages NuGet feed for cross-repo dependencies
- **Layered assembly architecture** — consistent `.Abstractions` / `.Hosting` / `.Packaging` extraction pattern. New assemblies: `Kuestenlogik.Surgewave.Connect.Abstractions`, `Kuestenlogik.Surgewave.Connect.Hosting`, `Kuestenlogik.Surgewave.Streams.InteractiveQueries`, `Kuestenlogik.Surgewave.Streams.InteractiveQueries.Hosting`, `Kuestenlogik.Surgewave.Schema.Registry.Hosting`, `Kuestenlogik.Surgewave.Cdc.Hosting`, `Kuestenlogik.Surgewave.Wasm.Hosting`, `Kuestenlogik.Surgewave.Plugins.Packaging`, `Kuestenlogik.Surgewave.Build`. Core libraries no longer depend on `Microsoft.AspNetCore.App`.
- **`IPeerQueryProvider` abstraction** in `Kuestenlogik.Surgewave.Streams` — opt-in peer query infrastructure via `app.WithInteractiveQueries(hostInfo)`. Default implementation (TCP `RemoteQueryServer` / `RemoteQueryClient` / `StreamsMetadataState`) ships in `Kuestenlogik.Surgewave.Streams.InteractiveQueries`. Swappable, mockable, no implicit activation through config properties.
- **`IValidatableConfig` + `ConfigValidator`** in `Kuestenlogik.Surgewave.Core.Configuration` — unified configuration validation contract. 43 config classes across 25 projects now carry DataAnnotations attributes (`[Required]`, `[Range]`, `[RegularExpression]`, `[Url]`) plus cross-property `Validate()` implementations (port uniqueness, EOS ↔ idempotence coupling, heartbeat timeout ordering, min/max invariants, ...). `ConfigValidator.ThrowIfInvalid` for fail-fast startup checks, `ConfigValidationException` aggregates all errors.
- **`Kuestenlogik.Surgewave.Build` MSBuild tasks** — `PackPluginTask` and `InstallPluginTask` with `TaskHostFactory` isolation, shared logic with the `surgewave plugins pack/install` CLI via `PluginPackageManager`. `Kuestenlogik.Surgewave.Build.targets` wires `dotnet publish -p:SurgewavePackPlugin=true` to pack a `.swpkg` and optionally install it into a broker's `plugins/` directory. Layout-aware `SurgewaveStagingDir` / `SurgewaveSppOutputDir` defaults (`artifacts/tmp/<Project>/` + `artifacts/pub/packages/` for the artifacts layout, `tmp/` + `pluginPackage/` next to the .csproj otherwise).
- Manifest-driven plugin discovery in `BrokerPluginActivator` — scans `plugins/*/plugin.json` instead of filename-prefix matching. `plugin.json` manifests added for all five community protocol plugins.

### Changed
- Simplified .swpkg format: flat lib/, assemblies[] replaces targets/connectors[]
- Plugin manifest: removed connectors[], added icon support
- ReadyToRun/SelfContained for all 6 publishable services
- Gateway container publishing added
- xUnit 2.9.3 migrated to xUnit v3 (3.2.2) across all repos
- Scripts reduced from 12 to 5
- **Renamed `Kuestenlogik.Surgewave.Connect.Repository` → `Kuestenlogik.Surgewave.Plugins.Repository`** — the project manages plugin packages, not Connect specifics. Broken `ProjectReference` to `Kuestenlogik.Surgewave.Connect` removed (Repository only used types from `Kuestenlogik.Surgewave.Plugins`).
- **Extracted `Kuestenlogik.Surgewave.Plugins.Packaging` from `Kuestenlogik.Surgewave.Plugins`** — was a namespace inside the Plugins assembly while `Kuestenlogik.Surgewave.Plugins.Repository` was a separate sibling. Now all three are sibling assemblies (Plugins ↔ Plugins.Packaging ↔ Plugins.Repository), matching the Microsoft.Extensions.* convention. `Kuestenlogik.Surgewave.Build` now references only Packaging, not the full plugin runtime loader.
- **Container output layout** — containers always produce portable `.tar` archives under `artifacts/pub/containers/`; when Docker is running the script additionally loads them via `docker load -i`. CLI ships as `surgewave/surgewave-cli:0.1.0` available in `docker-compose.yml` under the `cli` profile (`docker compose run --rm surgewave-cli ...`).
- **`artifacts/` tree restructured** — `artifacts/pub/` split into `apps/` (self-contained executables), `containers/` (tar archives), `packages/` (installed .swpkg), parallel to `artifacts/pkg/` (NuGet `.nupkg`) and `artifacts/tmp/` (temporary publish staging with auto-cleanup).
- **Scripts** — `publish.ps1`, `start.ps1`, `stop.ps1`, `build.ps1` normalized to UTF-8 console output, lowercase service directories, dot-source-aware CLI PATH integration.

- **IBrokerPlugin architecture** — 6 features promoted from hardcoded if-blocks in Program.cs to discoverable `IBrokerPlugin` implementations: AutoTuning, CruiseControl, Schema Registry, Connect, Geo-Replication. All activated via `BrokerPluginActivator` with `IsConfigEnabled` + `ConfigureServices` + `ConfigureAsync`. Program.cs reduced from ~1700 to ~1384 lines.
- **`IBrokerPlugin.ConfigureAsync`** — async plugin lifecycle for plugins with network connections or background services (default: wraps sync Configure).
- **DI migration** — 11 broker-internal objects registered as factory singletons (BrokerMetrics, ClusterState, DynamicBrokerConfig, ClusteringConfig, ReplicaManager, ClusterController, ReplicationServer, ReassignmentPlanner, ReassignmentConfig, PartitionReassignmentManager, ReassignmentExecutor).
- **`pluginsettings.json`** — plugin-bundled default configuration with 3-tier layering (plugin defaults < user appsettings < env vars). Custom filenames supported via manifest. `Kuestenlogik.Surgewave.Plugins.Packaging.Hosting` sibling assembly with `AddPluginDefaults` extension.
- **`plugin.json` rename** — `surgewave-plugin.json` → `plugin.json` across 20+ repos for consistency. JSON Schema published at `schemas/plugin-manifest/v1.json`.
- **CLI plugin tooling** — `surgewave config view --explain`, `surgewave config init --plugin`, `surgewave config validate --output json`, `surgewave plugin show/defaults/diff`, `surgewave plugin install --validate-config`, `surgewave cluster status --wait`.
- **`GET /api/config/validate`** — live broker config validation endpoint; Surgewave Control UI page at `/settings/config-validation`.
- **Marketplace defaults endpoint** — `GET /api/v1/packages/{id}/{version}/defaults` for UI-driven upgrade previews.
- **`update-local-cache.ps1`** — replaces cached NuGet copies after build so sibling repos pick up latest bits.
- **Plugin timing telemetry** — discovery + configure elapsed time logged at startup.
- **Surgewave.Connectors MSBuild migration** — switched to `dotnet publish -p:SurgewavePackPlugin=true` via `Kuestenlogik.Surgewave.Build`, dropped `collect-plugins.ps1`.
- **`PluginAssemblyScanner`** — shared assembly type-scanning utility for both `BrokerPluginActivator` and `PluginDiscovery`.
- **`ConfigLoader`** — shared JSON merge/overlay helper for `ConfigValidateCommand` and `ConfigViewCommand`.
- **`docs/guides/build-your-first-plugin.md`** — 10-step end-to-end plugin tutorial.
- **Protocol plugin documentation** — per-plugin pages for MQTT, AMQP, WebSocket, PostgreSQL with config tables.

### Fixed
- **Unsupported Kafka API key handling** — RequestDispatcher now returns a minimal error response for unknown API keys (like Confluent.Kafka 2.14's GetTelemetrySubscriptions/KIP-714) instead of throwing NotSupportedException which killed the connection and caused "Broker transport failure" on subsequent requests.
- **Connection resilience** — ProcessKafkaRequestsAsync catches per-request exceptions and continues instead of tearing down the socket.
- **BrokerFixture protocol readiness** — integration test fixture now verifies full Kafka handshake (GetMetadata) after TCP ready, closing the race where the protocol dispatcher wasn't yet accepting requests.
- **StreamsTracingTests MeterListener flake** — scoped by Meter reference to avoid parallel-test contamination.
- CircuitBreaker + FaultSchedule flaky timing tests
- NATS connector tests aligned to JetStream config keys
- Stale enterprise project references removed (Arrow, DuckDb, etc.)
- `Kuestenlogik.Surgewave.Plugins.Repository` pulled `Kuestenlogik.Surgewave.Connect` transitively for no reason — this dragged AspNetCore into the CLI and any plugin tooling. Fixed by direct reference to `Kuestenlogik.Surgewave.Plugins` only.

### Removed
- connectors[] from plugin manifest (metadata via attributes at runtime)
- TargetSpec/PluginRole (flat lib/ replaces role-based directories)
- collect-all-plugins.ps1, collect-connectors.ps1, collect-ai-nodes.ps1, install-plugins.ps1
- concept/ directory (all concepts implemented)
- models/ directory (ONNX moved to Surgewave.Ai)
- wasm-plugins/ (moved to docs/)
- config/ (plugin repo sources on Roadmap)
- **`StreamsConfig.ApplicationServer` property** — dead coupling after the `IPeerQueryProvider` extraction. The host is now obtained from the registered provider via `app.WithInteractiveQueries(hostInfo)` instead of implicit activation through config.
- **`PipelineRestApi`, `ConnectRestApi`, `PipelineChatApi` from `Kuestenlogik.Surgewave.Connect` core** — moved to `Kuestenlogik.Surgewave.Connect.Hosting`.
- **`SchemaRegistryRestApi`, `SchemaLinkingRestApi` from `Kuestenlogik.Surgewave.Schema.Registry` core** — moved to `Kuestenlogik.Surgewave.Schema.Registry.Hosting`.
- **`CdcRestApi` from `Kuestenlogik.Surgewave.Cdc` core** — moved to `Kuestenlogik.Surgewave.Cdc.Hosting`.
- **`WasmRestApi`, `SurgewaveWasmBrokerPlugin` from `Kuestenlogik.Surgewave.Wasm` core** — moved to `Kuestenlogik.Surgewave.Wasm.Hosting`.
- **`InteractiveQueryRestApi`, `StreamsIQExtensions` from `Kuestenlogik.Surgewave.Streams` core** — moved to `Kuestenlogik.Surgewave.Streams.InteractiveQueries.Hosting`.
- **`RemoteQueryServer`, `RemoteQueryClient`, `StreamsMetadataState`, state store wrappers, query executor, registry from `Kuestenlogik.Surgewave.Streams` core** — moved to `Kuestenlogik.Surgewave.Streams.InteractiveQueries`.
- Public IQ-related API methods on `StreamsApplication` (`AllMetadata`, `MetadataForKey`, `MetadataState`, `RegisterPeerAsync`, `CreateCompositeStore`, `Store<T>`) — now extension methods in `Kuestenlogik.Surgewave.Streams.InteractiveQueries` (same call sites, single `using` required).

### Fixed
- **`SurgewaveConsumer.ConsumeAsync(timeout)` honours its timeout** — the caller's `TimeSpan timeout` is now passed through to the per-partition long-poll (`maxWaitMs = max(1, min(timeout, 5000))`) instead of being ignored. Previously `ConsumeAsync(500ms)` on a drained 4-partition topic still sat in 4 × 5 s long-polls (~20 s) before returning `null`, which blew past Akka.Persistence TCK's 10 s `ExpectMsg` budget in `SurgewaveSnapshotStore.LoadAsync`. Data messages were already returned eagerly; only the end-of-topic detection was slow.

## [0.2.0] - 2025-XX-XX

### Added
- Ephemeral topic mode with ring-buffer storage (`CleanupPolicy.Ephemeral`)
- Topic configuration in CreateTopic wire protocol (configs passed at creation time)
- Exactly-Once Semantics (EOS) transaction support
- Rack-aware partition replication
- MirrorMaker for cross-cluster replication
- Plugin architecture for CLI extensibility
- GitHub Actions CI/CD pipelines
- Apache 2.0 license

### Changed
- Improved transaction coordinator with persistent state
- Enhanced cluster controller with rack awareness
- `CreateTopicRequestPayload` now carries optional `TopicConfigPayload[]` for inline config

### Fixed
- Topic config (cleanup.policy, ephemeral.buffer.bytes) now applied at creation time instead of post-creation AlterConfig
- PartitionLog recovery: correct LogStartOffset when first message starts at offset 0
- IPv4-only mode: broker now advertises `127.0.0.1` in metadata responses when `EnableDualMode=false`, preventing librdkafka IPv6 resolution failures
- Transaction timeout handling edge cases

## [0.1.0] - 2024-XX-XX

### Added
- Initial release
- Kafka protocol compatibility (API versions 0-12)
- Native Surgewave protocol for high performance
- Producer and Consumer client libraries
- Consumer group coordination
- Topic management and partitioning
- Log segment storage with retention policies
- Replication with leader election
- TLS/SSL encryption support
- SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- Docker and Kubernetes deployment support
- Helm chart for Kubernetes
- CLI tool for administration
- Connect framework for data integration
- Prometheus metrics and Grafana dashboards
- Comprehensive documentation

[Unreleased]: https://github.com/YOUR_ORG/surgewave/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/YOUR_ORG/surgewave/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/YOUR_ORG/surgewave/releases/tag/v0.1.0
