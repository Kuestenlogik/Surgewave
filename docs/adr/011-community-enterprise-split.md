# ADR-011: Community/Enterprise Repository Split

## Status

Accepted

## Date

2026-04

## Context

Surgewave started as a single repository. As the project grew to include AI pipelines, governance features, specialized storage engines, geo-replication, serverless functions, and cloud tiered storage, the single-repo approach created problems: long build times, tangled dependency graphs, and the inability to license enterprise features separately from the open-source core.

The business model requires a clear split: a community edition under a permissive source-available license (BSL) that includes the broker, streaming, protocols, connectors, schema registry, and all client libraries; and an enterprise edition under a commercial license (KCL) that includes advanced features sold separately. The technical architecture must enforce this split at compile time --- the community broker must have zero references to enterprise code.

### Alternatives Considered

- **Single repo with feature flags only:** Would keep all code together but makes it impossible to ship enterprise features under a separate license. Build artifacts would always contain enterprise code, even if disabled.
- **Fork-based split (community fork + enterprise fork):** Leads to merge conflicts and diverging codebases over time. Maintaining two parallel branches is unsustainable.
- **Mono-repo with directory-based licensing:** Keeps everything in one repo but complicates CI/CD, access control, and NuGet packaging for features that should not be publicly visible.

## Decision

Split Surgewave into 20 repositories with a plugin-based boundary between community and enterprise. Enterprise features are delivered as `.swpkg` plugin packages with no compile-time coupling to the broker.

### Repository Structure

**Community Repositories (BSL License):**

- **Surgewave** --- core broker, plugins framework, Connect, Streams, Schema Registry (12 format handlers), Protocols (Kafka, Native, MQTT, AMQP, WebSocket, gRPC), CLI, Control UI, Gateway, Wasm, Edge, CDC, Chaos Testing
- **Surgewave.Connectors** --- 117+ source/sink connectors
- **Surgewave.Samples** --- 28 sample applications
- **Surgewave.Bootcamp** --- training curriculum (34 units, 77 lessons)
- **Surgewave.Templates** --- `dotnet new` project templates

**Enterprise Repositories (KCL License):**

- **Surgewave.Ai** --- AI pipeline nodes, document processing, RAG, agents, A2A, ML (ONNX)
- **Surgewave.Governance** --- data catalog, privacy, multi-tenancy
- **Surgewave.Storage.Arrow** --- Apache Arrow storage engine
- **Surgewave.Storage.DuckDb** --- DuckDB storage engine
- **Surgewave.Storage.Parquet** --- Parquet storage engine
- **Surgewave.Storage.NvmeDirect** --- NVMe io_uring direct storage engine
- **Surgewave.Storage.Tiering.Azure** --- Azure Blob tiered storage provider
- **Surgewave.Storage.Tiering.S3** --- AWS S3 tiered storage provider
- **Surgewave.Storage.Tiering.Gcp** --- GCP Cloud Storage tiered storage provider
- **Surgewave.Replication** --- cross-cluster geo-replication
- **Surgewave.Functions** --- serverless function runtime
- **Surgewave.Operator** --- Kubernetes CRD controller
- **Surgewave.Transport** --- shared memory transport

**Domain-Specific Repositories:**

- **Surgewave.Mesh** --- hierarchical mesh networking
- **Surgewave.Tactical** --- military tactical domain (DIS/HLA)

### Licensing Architecture

`SurgewaveFeatures` in `Kuestenlogik.Surgewave.Plugins` defines two sets of constants:

- **Enterprise features** (13): `Surgewave.Replication`, `Surgewave.Storage.Tiering`, `Surgewave.Storage.NvmeDirect`, `Surgewave.Storage.Arrow`, `Surgewave.Storage.DuckDb`, `Surgewave.Storage.Parquet`, `Surgewave.AI`, `Surgewave.MultiTenancy`, `Surgewave.DataMesh`, `Surgewave.Functions`, `Surgewave.Privacy`, `Surgewave.Transport.SharedMemory`, `Surgewave.Operator`.
- **Community features** (11): `Surgewave.Clustering`, `Surgewave.Streams`, `Surgewave.Wasm`, `Surgewave.Api.GraphQL`, `Surgewave.Api.Grpc`, `Surgewave.Gateway`, `Surgewave.Cdc`, `Surgewave.Edge`, `Surgewave.Connect.Enterprise`, `Surgewave.Control`, `Surgewave.Testing.Chaos`.

`ILicenseProvider` is an optional interface. When registered, it exposes `Edition`, `LicensedTo`, `ExpiresAt`, and `IsFeatureEnabled(featureName)`. When not registered (null), the broker runs in community mode --- all community features are available, enterprise features are skipped.

### Enforcement

`BrokerPluginActivator.ActivatePlugins()` checks each discovered `IBrokerPlugin` against `SurgewaveFeatures.IsEnterpriseFeature(plugin.FeatureId)`. If the feature is enterprise and no license provider is present (or the feature is not enabled in the license), the plugin is skipped with a warning log. Protocol plugins (`IProtocolPlugin`) are not license-gated.

This means:
1. The broker binary contains zero enterprise code.
2. Enterprise `.swpkg` packages can be installed but will not activate without a valid license.
3. Community deployments carry no enterprise overhead --- not even inactive code paths.

## Consequences

- **Clean compile-time boundary** --- `dotnet build` of the Surgewave repository succeeds with no enterprise references. Enterprise repos build independently against the published `Kuestenlogik.Surgewave.Plugins` NuGet package.
- **Independent release cycles** --- enterprise storage engines, AI features, and governance modules can ship updates without waiting for a core broker release.
- **Graceful degradation** --- enterprise plugins that are present but unlicensed produce a clear warning at startup rather than crashing.
- **Increased repository count** (20 repos) adds coordination overhead for cross-cutting changes. Mitigated by the stable `IPlugin` interface contract and semantic versioning.
- **Testing enterprise integrations** requires installing `.swpkg` packages in a test environment. CI pipelines for enterprise repos run against published community NuGet packages.
