# Kafka Conformance

Surgewave aims to be a drop-in replacement for Apache Kafka. This section is the
authoritative, externally-verifiable statement of what's in scope, what's
implemented, and what is intentionally out of scope. Pages are kept in sync
with the wire-protocol code and tracked in CI.

If you're integrating a Kafka client against Surgewave, these pages tell you
whether your client's path is on the wired, advertised-only, or unsupported
side of the matrix — without having to read source.

## Headline numbers

- **Kafka 4.0 wire-protocol RPCs implemented**: 56 / ~55
- **Kafka 4.2 wire-protocol RPCs advertised**: 60 of 93 enum entries
- **Wired with full handler logic**: 72 RPCs (admin surface complete)
- **Advertised but not wired**: 0 — every advertised admin RPC has a handler
- **Major KIPs implemented**: 15 (KIP-98, KIP-516, KIP-595, KIP-714, KIP-848,
  KIP-853 (wire-only), KIP-892, KIP-894 (partial), KIP-903, KIP-932, KIP-936,
  KIP-985, KIP-994, KIP-1071, KIP-895)
- **Confluent Schema Registry**: wire format + REST API audited compatible
  ([details](schema-registry.md))
- **Cross-client tested**: Confluent.Kafka 2.x (.NET), librdkafka 2.14 (CGv2 e2e),
  Confluent.SchemaRegistry contract-pinned

## Sections

| Page | Scope |
|------|-------|
| [Kafka API matrix](kafka-rpcs.md) | Per-RPC table across all 93 ApiKey enum entries — status, handler, version range, notes |
| [KIP coverage](kips.md) | Per-KIP status with evidence pointers, conformance test inventory, cross-client matrix, known gaps |
| [Schema Registry](schema-registry.md) | Confluent Schema Registry wire-format + REST API compatibility |

## Status legend

| Status | Meaning |
|--------|---------|
| **Wired** | A real handler processes the request and returns a meaningful response. Round-trip-tested against at least one Kafka client. |
| **Wired (rejects)** | Handler is registered and returns a precise structured error (e.g., `LogDirNotFound` for JBOD operations on a single-dir broker, `UnsupportedVersion` for unimplemented online voter changes). Better than the dispatcher's generic `UNSUPPORTED_VERSION` because admin tools see the precise reason. |
| **Stub** | Handler exists; accepts the request and returns a well-formed but degenerate response (e.g. empty subscription set). Clients see success and continue. Documented per row. |
| **gRPC-only** | Functionality lives in `Kuestenlogik.Surgewave.Api.Grpc`; no Kafka wire handler is registered. A Kafka-wire client will see `UNSUPPORTED_VERSION (35)`. |
| **Advertised, not wired** | `ApiVersions` lists the key, but no handler is registered. Currently empty — every advertised RPC has a handler. |
| **Not implemented** | No handler, not advertised. Hard `UnsupportedApiKey` response if a client somehow calls it. |

## Source of truth

The canonical document is [`CONFORMANCE.md`](https://github.com/Kuestenlogik/Surgewave/blob/main/CONFORMANCE.md)
at the repository root. The pages here mirror it for navigability inside
the docs site.

The matrix is generated from the same code that runs at runtime:

- **API version ranges advertised to clients**:
  [`src/Kuestenlogik.Surgewave.Protocol.Kafka/Requests/ApiVersionsRequest.cs`](https://github.com/Kuestenlogik/Surgewave/blob/main/src/Kuestenlogik.Surgewave.Protocol.Kafka/Requests/ApiVersionsRequest.cs)
- **Wired handler set**:
  [`src/Kuestenlogik.Surgewave.Broker/Program.cs`](https://github.com/Kuestenlogik/Surgewave/blob/main/src/Kuestenlogik.Surgewave.Broker/Program.cs)
- **Per-handler `SupportedApiKeys`**:
  [`src/Kuestenlogik.Surgewave.Broker/Handlers/`](https://github.com/Kuestenlogik/Surgewave/tree/main/src/Kuestenlogik.Surgewave.Broker/Handlers)

When a new RPC is wired or a version is bumped, the source plus this docs
section update in the same PR.

## Non-goals (intentional)

- **Kafka MirrorMaker 2 control plane.** Surgewave.Replication ships its own
  cluster-link control plane; MirrorMaker is not emulated.
- **ZooKeeper protocol.** Surgewave is KRaft-native; the legacy ZK control plane
  is intentionally absent.
- **Wire-level emulation of internal Kafka controller RPCs** (`AlterPartition`,
  `Envelope`, `ControllerRegistration`, `AssignReplicasToDirs`). Surgewave has its
  own controller; clients never see these.
