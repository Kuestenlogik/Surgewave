# WebSocket Protocol Plugin

`kuestenlogik.surgewave.protocol.websocket` — exposes Surgewave topics over WebSocket
endpoints, so browser-based clients (or any environment with a WebSocket
library) can produce and consume directly without needing a Surgewave or Kafka
client. The endpoints share the broker's HTTP port — no separate TCP
listener.

## Installation

```bash
surgewave plugin install kuestenlogik.surgewave.protocol.websocket-<version>.swpkg
```

## Configuration

Section: `Surgewave:WebSocket`. Every field has a recommended default in
`pluginsettings.json`.

| Field | Default | Description |
|---|---|---|
| `Enabled` | `false` | Master switch. Set to `true` to register the WebSocket endpoints with the broker's HTTP host. |
| `Path` | `"/ws"` | Base path for WebSocket endpoints. Three sub-routes are mounted: `<path>/produce/{topic}`, `<path>/consume/{topic}`, `<path>/subscribe`. |
| `MaxMessageSizeBytes` | `1048576` (1 MB) | Maximum WebSocket frame size accepted from clients. |
| `PingInterval` | `00:00:30` (30 s) | Interval between server-initiated WebSocket ping frames to keep idle connections alive. |
| `MaxConnections` | `5000` | Maximum concurrent WebSocket connections across all sub-routes. |

### Minimal config

```json
{
  "Surgewave": {
    "WebSocket": { "Enabled": true }
  }
}
```

The WebSocket endpoints register at `/ws/produce/{topic}`,
`/ws/consume/{topic}` and `/ws/subscribe` on the broker's HTTP host.

### Browser-side example

```javascript
// Produce
const producer = new WebSocket("ws://broker:9093/ws/produce/orders");
producer.onopen = () => producer.send(JSON.stringify({ id: 42, total: 99.99 }));

// Consume
const consumer = new WebSocket("ws://broker:9093/ws/consume/orders");
consumer.onmessage = (event) => console.log("got:", event.data);
```

### Custom path

If you proxy through a reverse proxy that already mounts `/ws` for something
else, set a different path:

```json
{
  "Surgewave": {
    "WebSocket": {
      "Enabled": true,
      "Path": "/surgewave/ws",
      "MaxConnections": 50000
    }
  }
}
```

## TLS

WebSocket TLS is handled by Kestrel, the same way as HTTPS for any other
endpoint on the broker. Configure the listening URL in
`Surgewave:Kestrel:Endpoints` to use `https://`.

## Operations

```bash
surgewave plugin show kuestenlogik.surgewave.protocol.websocket
surgewave config view appsettings.json --explain
surgewave config validate appsettings.json
```

## Reference

- Source: `src/Kuestenlogik.Surgewave.Protocol.WebSocket/`
- Config class: `WebSocketConfig.cs`
