# ADR-001: Pluggable Storage Engine Abstraction

## Status

Accepted

## Date

2025-12

## Context

Surgewave needs to support multiple storage backends to serve different use cases. In-memory storage is ideal for testing and low-latency ephemeral workloads. File-system-based storage suits single-node production deployments. Embedded databases like RocksDB and SQLite offer durability with different trade-off profiles. Cloud-native deployments may prefer object storage or distributed backends.

Hardcoding a single storage engine would force all deployments into the same trade-off. Kafka's tight coupling to its log-segment format makes it difficult to experiment with alternatives. We wanted Surgewave to avoid that rigidity from the start.

### Alternatives Considered

- **Single storage engine (file-based like Kafka):** Simpler, but locks out use cases like in-memory testing or embedded databases.
- **Storage adapter via configuration file only (no code abstraction):** Would require runtime reflection and lose compile-time safety.
- **External storage service (separate process):** Adds network hop and operational complexity for what should be a local concern.

## Decision

Introduce an `ILogSegment` / `ILogSegmentFactory` abstraction layer. Each storage engine implements these interfaces. The active engine is selected via configuration (`Surgewave:Storage:Engine`), not code changes. The factory is registered in DI at startup based on the configured engine name.

Current implementations: Memory, FileSystem, RocksDB, SQLite, LMDB, LevelDB, FasterKV, TieredMemoryFile, Mmap, WiredTiger.

## Consequences

- **10 storage engines** are supported out of the box, covering a wide range of deployment scenarios.
- Adding a new storage engine requires implementing two interfaces and registering it --- no changes to the broker core.
- There is a slight abstraction overhead on the hot path (virtual dispatch through the interface). Benchmarks show this is negligible compared to actual I/O cost.
- Storage-specific tuning knobs (e.g., RocksDB compaction settings) are exposed through engine-specific configuration sections, keeping the abstraction clean.
- Testing is simplified: unit tests use the Memory engine, integration tests can parameterize across engines.
