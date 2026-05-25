# WebSocket

Surgewave exposes produce and consume over WebSocket so browser-side code
can drive the same topics as the native Kafka clients — no sidecar, no
extra service.

## Endpoints

- `ws://broker:5050/ws/produce` — JSON-framed produce
- `ws://broker:5050/ws/consume` — JSON-framed consume with offset
  tracking

Same auth, same ACLs, same topics, same offsets as the Kafka and Native
wires. A WebSocket consumer counts as a normal consumer-group member.

## Message format

Frames are line-delimited JSON, one record per frame:

```json
{ "topic": "events", "key": "user-123", "value": { ... }, "headers": {} }
```

Server-side, Surgewave decodes the JSON value into the topic's registered
schema if one is set, applying the same validation that the Kafka and
Native wires would.

## Use cases

- **In-browser dashboards** — live telemetry without long polling
- **Webhooks-out** — push events to browser-side handlers
- **Quick demos** — test produce/consume from a browser console

For server-side integrations the gRPC streaming API is usually a better
fit; see [gRPC](grpc.md).
