# ADR-010: Schema Registry Multi-Format Architecture

## Status

Accepted

## Date

2026-04

## Context

Kafka's Confluent Schema Registry supports three formats: Avro, Protobuf, and JSON Schema. Modern systems use a much wider variety of serialization formats. Surgewave needed to support all major binary and text-based formats while maintaining API compatibility with Confluent's REST endpoints so that existing Kafka tooling (ksqlDB, Kafka Connect, client libraries) can work against Surgewave without modification.

Adding each format directly to the Schema Registry codebase would create a monolith with heavy dependencies (Avro parser, Protobuf compiler, FlatBuffers IDL tools, etc.). Many deployments only need one or two formats, so forcing all format dependencies on every installation is wasteful.

### Alternatives Considered

- **Single-format registry (Avro only, like early Confluent):** Would be simpler but locks out the many teams using Protobuf, FlatBuffers, MessagePack, or other formats.
- **External format validation services:** Each format as a microservice called via HTTP. Adds latency and operational complexity for what should be a fast, synchronous validation call.
- **Schema-as-opaque-bytes (no validation):** Would simplify the registry but eliminates compatibility checking, which is the primary value of a schema registry.

## Decision

Implement a handler-per-format plugin architecture built on the `ISchemaTypeHandler` interface and a `SchemaTypeHandlerRegistry` that collects all registered handlers via DI.

### Core Abstractions

`ISchemaTypeHandler` defines three members:

- `TypeName` --- the schema type identifier (e.g., `"AVRO"`, `"JSON"`, `"PROTOBUF"`), used in the REST API and stored alongside schemas.
- `Validate(string schemaString)` --- checks that a schema definition is well-formed, returning `(bool IsValid, string? Error)`.
- `CheckCompatibility(string newSchemaString, IReadOnlyList<Schema> existingSchemas, CompatibilityMode mode)` --- performs backward, forward, or full compatibility checking against existing schema versions.

`ISchemaTypeHandlerRegistry` provides `GetHandler(typeName)`, `IsSupported(typeName)`, and `GetSupportedTypes()`. The default `SchemaTypeHandlerRegistry` implementation stores handlers in a `FrozenDictionary<string, ISchemaTypeHandler>` for lock-free lookups at runtime.

### Supported Formats (12 Server-Side Handlers)

Each format ships as a separate project (`Kuestenlogik.Surgewave.Schema.Registry.Handlers.<Format>`):

| Format | Handler | Type Name |
|--------|---------|-----------|
| Apache Avro | `AvroSchemaHandler` | AVRO |
| Protocol Buffers | `ProtobufSchemaHandler` | PROTOBUF |
| JSON Schema | `JsonSchemaHandler` | JSON |
| FlatBuffers | `FlatBuffersSchemaHandler` | FLATBUFFERS |
| Hyperion | `HyperionSchemaHandler` | HYPERION |
| MessagePack | `MessagePackSchemaHandler` | MSGPACK |
| CBOR | `CborSchemaHandler` | CBOR |
| Microsoft Bond | `BondSchemaHandler` | BOND |
| Apache Thrift | `ThriftSchemaHandler` | THRIFT |
| MemoryPack | `MemoryPackSchemaHandler` | MEMORYPACK |
| Cap'n Proto | `CapnProtoSchemaHandler` | CAPNPROTO |
| Microsoft Orleans | `OrleansSchemaHandler` | ORLEANS |

### Client Serializer Packages (12 Matching Packages)

Each format has a corresponding `Kuestenlogik.Surgewave.Client.SchemaRegistry.<Format>` package providing `SchemaRegistry<Format>Serializer` and `SchemaRegistry<Format>Deserializer`. These handle the Confluent wire format (magic byte `0x00` + 4-byte schema ID + payload), schema registration on first produce, and schema lookup on consume.

### Confluent API Compatibility

`SchemaRegistryRestApi` maps endpoints under `/subjects/`, `/schemas/`, `/config/`, and `/compatibility/` using the same URL patterns, request/response shapes, and error codes as Confluent Schema Registry v7. The REST API documentation is generated via OpenAPI with Scalar UI.

### Format Auto-Detection

`ContentTypeDetector` inspects raw payload bytes to detect the serialization format when no `content-type` header is present. It recognizes the Confluent wire format prefix (`0x00`), MessagePack markers (`0x80`-`0x9F`, `0xDC`-`0xDF`), JSON (`{` or `[`), and UTF-8 text.

`ContentTypes` defines well-known MIME type constants (e.g., `application/x-protobuf`, `application/x-msgpack`, `application/cbor`) used in message headers.

### Registration Pattern

Each handler project provides `AddSurgewave<Format>SchemaHandler()` extension methods on `IServiceCollection`. The Schema Registry collects all `ISchemaTypeHandler` registrations from DI when building the `SchemaTypeHandlerRegistry`. Custom handlers follow the same pattern.

## Consequences

- **12 serialization formats** are supported out of the box, covering the vast majority of production workloads.
- Adding a new format requires implementing `ISchemaTypeHandler` in a new project and registering it via DI --- no changes to the registry core.
- Each handler project only pulls in the dependencies for its specific format, keeping individual deployment footprints small.
- The `FrozenDictionary` in `SchemaTypeHandlerRegistry` ensures zero-allocation handler lookups on the hot path.
- Confluent API compatibility means existing Kafka ecosystem tools work against Surgewave's Schema Registry without code changes.
- The separate client serializer packages let producers and consumers choose only the formats they need, avoiding unnecessary transitive dependencies.
