# WASM Plugin System

Surgewave supports WebAssembly (WASM) plugins for language-agnostic, sandboxed data processing.
Write plugins in Rust, Go, AssemblyScript, or any language that compiles to WASM, and deploy
them without restarting the broker.

## Overview

WASM plugins run in a sandboxed Wasmtime runtime with configurable memory limits,
execution timeouts, and optional file/network access. They integrate with Surgewave's
pipeline system as Source, Sink, Transform, or Function nodes.

## ABI (Application Binary Interface)

Surgewave exposes host functions to plugins and expects specific exports from the WASM module.

### Plugin Exports (required by all types)

| Function | Signature | Description |
|----------|-----------|-------------|
| `plugin_init` | `() -> i32` | Called once on load. Returns `0` on success. |
| `plugin_info` | `() -> i32` | Returns a pointer to a JSON metadata string in WASM memory. |
| `plugin_close` | `() -> i32` | Called on shutdown. Returns `0` on success. |
| `alloc` | `(size: i32) -> i32` | Allocate memory inside the module for the host to write into. |
| `dealloc` | `(ptr: i32, size: i32)` | Free memory inside the module. |

### Plugin Exports (by type)

**Transform / Function:**
| Function | Signature | Description |
|----------|-----------|-------------|
| `plugin_process` | `(ptr: i32, len: i32) -> i32` | Process a message. Returns result pointer or `0` to drop. |

**Source:**
| Function | Signature | Description |
|----------|-----------|-------------|
| `plugin_poll` | `() -> i32` | Poll for a new record. Returns pointer to output buffer or `0` if none. |

**Sink:**
| Function | Signature | Description |
|----------|-----------|-------------|
| `plugin_push` | `(ptr: i32, len: i32) -> i32` | Consume a record. Returns `0` on success, `-1` on error. |

### Host Functions (provided by Surgewave)

Surgewave exports these functions to the WASM module's `env` namespace:

| Function | Signature | Description |
|----------|-----------|-------------|
| `surgewave_produce` | `(topic_ptr, topic_len, key_ptr, key_len, value_ptr, value_len) -> i32` | Produce a message to a Surgewave topic. |
| `surgewave_log` | `(level: i32, msg_ptr: i32, msg_len: i32)` | Write a log message (0=Trace … 4=Error). |
| `surgewave_get_config` | `(key_ptr: i32, key_len: i32, out_ptr: i32, out_len: i32) -> i32` | Read a config value by key. Returns bytes written or `-1`. |
| `surgewave_state_get` | `(key_ptr: i32, key_len: i32, out_ptr: i32, out_len: i32) -> i32` | Get a value from the per-plugin state store. |
| `surgewave_state_put` | `(key_ptr: i32, key_len: i32, value_ptr: i32, value_len: i32) -> i32` | Put a value into the per-plugin state store. |

## Building Plugins

### Rust

```bash
rustup target add wasm32-wasi
cargo init --lib my-plugin
```

`Cargo.toml`:
```toml
[lib]
crate-type = ["cdylib"]
```

```bash
cargo build --target wasm32-wasi --release
```

Output: `target/wasm32-wasi/release/my_plugin.wasm`

### Go (TinyGo)

```bash
tinygo build -o plugin.wasm -target=wasi main.go
```

```go
package main

//export plugin_init
func pluginInit() int32 { return 0 }

//export plugin_close
func pluginClose() int32 { return 0 }

//export plugin_process
func pluginProcess(ptr, length int32) int32 {
    // process message; return result pointer or 0 to drop
    return ptr
}

//export alloc
func alloc(size int32) int32 { /* ... */ return 0 }

//export dealloc
func dealloc(ptr, size int32) {}

func main() {}
```

### AssemblyScript

```bash
npm init -y
npm install --save-dev assemblyscript
npx asinit .
```

```typescript
export function plugin_init(): i32 { return 0; }
export function plugin_close(): i32 { return 0; }

export function plugin_process(ptr: i32, len: i32): i32 {
    // process message; return result pointer or 0 to drop
    return ptr;
}

export function alloc(size: i32): i32 { /* ... */ return 0; }
export function dealloc(ptr: i32, size: i32): void {}
```

```bash
npx asc assembly/index.ts --target release --outFile plugin.wasm
```

## Manifest Format

Each plugin directory must contain a `wasm-plugin.json` manifest:

```json
{
  "id": "my-company.my-transform",
  "name": "My Transform",
  "version": "1.0.0",
  "type": "Transform",
  "description": "Transforms messages using custom logic",
  "author": "My Company",
  "inputTopic": "input-topic",
  "outputTopic": "output-topic",
  "config": {
    "key1": "value1",
    "key2": "value2"
  }
}
```

### Fields

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique plugin identifier |
| `name` | Yes | Human-readable name |
| `version` | Yes | Semantic version |
| `type` | Yes | `Source`, `Sink`, `Transform`, or `Function` |
| `description` | No | Description for UI and API |
| `author` | No | Author or organization |
| `inputTopic` | No | Input topic (Sink/Transform/Function) |
| `outputTopic` | No | Output topic (Source/Transform/Function) |
| `config` | No | Key-value pairs passed to the WASM module |

## Deployment

### Drop-in Directory

Place plugin directories under the configured WASM directory (default: `wasm-plugins/`):

```
wasm-plugins/
  my-transform/
    plugin.wasm
    wasm-plugin.json
  my-source/
    plugin.wasm
    wasm-plugin.json
```

### Hot-Deploy

When `Surgewave:Wasm:EnableHotDeploy=true` (default), Surgewave watches the plugins directory
and automatically loads new or updated plugins. A debounce interval (default: 2s) prevents
multiple reloads during file copy operations.

### REST API

```bash
# Discover plugins in directory
curl http://localhost:9092/api/wasm/plugins/discover

# Load a plugin
curl -X POST http://localhost:9092/api/wasm/plugins/load \
  -H "Content-Type: application/json" \
  -d '{"pluginId": "my-company.my-transform"}'

# List loaded plugins with status
curl http://localhost:9092/api/wasm/plugins

# Reload a plugin (hot-reload)
curl -X POST http://localhost:9092/api/wasm/plugins/my-company.my-transform/reload

# Stop a plugin
curl -X POST http://localhost:9092/api/wasm/plugins/my-company.my-transform/stop
```

## Configuration

```json
{
  "Surgewave": {
    "Wasm": {
      "Enabled": true,
      "WasmDirectory": "wasm-plugins",
      "MaxMemoryBytes": 67108864,
      "ExecutionTimeout": "00:00:30",
      "AllowFileAccess": false,
      "AllowNetworkAccess": false,
      "EnableHotDeploy": true,
      "HotDeployDebounce": "00:00:02"
    }
  }
}
```

| Setting | Default | Description |
|---------|---------|-------------|
| `Enabled` | `false` | Enable WASM plugin subsystem |
| `WasmDirectory` | `wasm-plugins` | Directory to scan for plugins |
| `MaxMemoryBytes` | `67108864` (64MB) | Max linear memory per module |
| `ExecutionTimeout` | `00:00:30` | Max wall-clock time per function call |
| `AllowFileAccess` | `false` | Allow WASI file system access |
| `AllowNetworkAccess` | `false` | Allow outbound network calls |
| `EnableHotDeploy` | `true` | Watch directory for changes |
| `HotDeployDebounce` | `00:00:02` | Debounce interval for file watcher |

## Examples

See the `wasm-plugins/examples/` directory for complete examples:

- **transform-uppercase** -- Convert string values to uppercase
- **filter-json** -- Filter messages by JSON field value
- **sensor-source** -- Simulated IoT sensor data generator
- **webhook-sink** -- Forward messages to HTTP webhook
