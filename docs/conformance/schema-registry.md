# Confluent Schema Registry compatibility

Surgewave ships its own Schema Registry under `src/Kuestenlogik.Surgewave.Schema.Registry`. To
be a drop-in replacement for Confluent Schema Registry, Surgewave must match two
verifiable contracts: the magic-byte wire format embedded in every record,
and the REST API surface that schema-aware clients call. Both have been
audited; this page is the per-contract status statement.

## Magic-byte wire format

Confluent's wire layout in every record value:

```
0x00 (1 byte)        — magic byte
schemaId (4 bytes)   — schema id, big-endian int32
[messageIndex]       — Protobuf only: ZigZag-varint message-index array
payload              — bytes
```

| Aspect | Surgewave status | Source |
|--------|-------------|--------|
| Magic byte 0x00 | ✓ Wired | `src/Kuestenlogik.Surgewave.Client.SchemaRegistry/SchemaRegistrySerializerConfig.cs` (`WriteHeader` / `ReadSchemaId`) |
| SchemaId big-endian int32 | ✓ Wired | same |
| Avro / JSON payload follows immediately | ✓ Wired | `SchemaRegistryAvroSerializer`, `SchemaRegistryJsonSerializer` |
| Protobuf MessageIndex (single-message) | ✓ Wired (writes `0x00`, reads via `SkipVarint`) | `SchemaRegistryProtobufSerializer` / `Deserializer` |
| Protobuf MessageIndex (multi-message, KIP-style nested types) | ✗ Not implemented — index is hard-coded to 0 | gap |

## REST API path coverage

Surgewave hosts the API under `src/Kuestenlogik.Surgewave.Schema.Registry.Hosting/SchemaRegistryRestApi.cs`.

| Confluent path | Method | Surgewave status | Notes |
|----------------|--------|-------------|-------|
| `/subjects` | GET | ✓ | Returns `string[]` |
| `/subjects/{subject}/versions` | GET | ✓ | Returns `int[]` |
| `/subjects/{subject}/versions` | POST | ✓ | Body `RegisterSchemaRequest` → `{"id":int}` |
| `/subjects/{subject}/versions/{version}` | GET | ✓ | Full `SchemaResponse` |
| `/subjects/{subject}/versions/latest` | GET | ✓ | Same shape as above |
| `/subjects/{subject}` | POST | ✓ | Schema lookup by content |
| `/schemas/ids/{id}` | GET | ✓ | `GetSchemaByIdResponse` |
| `/schemas/ids/{id}/versions` | GET | ✓ | `IReadOnlyList<SubjectVersion>` |
| `/schemas/types` | GET | ✓ | Returns `string[]` of supported types |
| `/compatibility/subjects/{subject}/versions/{version}` | POST | ✓ | `CompatibilityCheckResponse` |
| `/config` | GET / PUT | ✓ | `ConfigResponse` |
| `/config/{subject}` | GET / PUT / DELETE | ✓ | per-subject override |
| `/mode` | GET / PUT | Partial | basic mode response, advanced read-only / read-write modes simplified |

Surgewave-only extensions (do not interfere with Confluent clients):
`/schemas/infer/{topic}`, `/schemas/infer/{topic}/register`,
`/api/schema-evolution/*`, `/api/schema-migration/*`.

## Compatibility levels

Surgewave accepts and emits all seven Confluent levels: `NONE`, `BACKWARD`,
`BACKWARD_TRANSITIVE`, `FORWARD`, `FORWARD_TRANSITIVE`, `FULL`,
`FULL_TRANSITIVE` (case-insensitive on input, uppercase on output).

## Schema types

`AVRO`, `JSON`, `PROTOBUF` are accepted on input and emitted on output,
case-insensitive on input. Surgewave additionally accepts `FLATBUFFERS` —
this is a Surgewave extension; standard Confluent clients ignore unknown types.

## JSON shape contract

Pinned by `tests/Kuestenlogik.Surgewave.Schema.Registry.Tests/ConfluentSchemaRegistryContractTests.cs`
(28 tests). The tests guard against silent regressions in the field-naming
convention — a stray `errorCode` (camelCase) instead of `error_code`
(snake_case) would break every Confluent client without showing up as a
type or unit-test failure elsewhere.

| Response | Shape |
|----------|-------|
| `ErrorResponse` | `{"error_code": int, "message": string}` (snake_case — this is intentional and matches Confluent's historical inconsistency) |
| `CompatibilityCheckResponse` | `{"is_compatible": bool, "messages": string[]?}` |
| `ConfigResponse` | `{"compatibilityLevel": string}` (camelCase here — Confluent is inconsistent across endpoints; we follow them exactly) |
| `SchemaResponse` | `{"subject", "id", "version", "schemaType", "schema", "references"}` |
| `RegisterSchemaResponse` | `{"id": int}` only |

## Known gaps

- **Multi-message Protobuf** — Surgewave always writes/reads MessageIndex 0.
  This is the common case; multi-message Protobuf schemas (`message A {} message B {}`
  in one .proto) need the full ZigZag-varint array. Tracked.
- **End-to-end against `Confluent.SchemaRegistry` .NET client** — the
  contract tests cover JSON shapes and the magic-byte wire format
  byte-by-byte. A live round-trip with `CachedSchemaRegistryClient`
  registering / fetching / serializing through Surgewave's REST API is the next
  layer of confidence.
- **Schema References** (Confluent KIP-718) — `references` field is
  serialized in responses, but the resolver path that walks references on
  fetch needs verification against multi-schema imports.
