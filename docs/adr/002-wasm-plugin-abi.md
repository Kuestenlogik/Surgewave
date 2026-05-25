# ADR-002: WebAssembly Plugin ABI Design

## Status

Accepted

## Date

2026-03

## Context

Surgewave needs a plugin system for user-defined sources, sinks, transforms, and functions. Native .NET plugins require authors to target the same runtime version, and a misbehaving plugin can crash the entire broker process. We needed a language-agnostic, sandboxed plugin model that allows hot deployment without broker restarts.

### Alternatives Considered

- **Native .NET plugins (Assembly.LoadContext):** Fast, but couples plugins to the broker runtime. A null-reference or stack overflow in plugin code takes down the broker.
- **gRPC sidecar plugins (like HashiCorp's go-plugin):** Language-agnostic but adds network overhead and operational complexity for every plugin instance.
- **Lua/JavaScript embedded scripting:** Sandboxed, but limited ecosystem and poor performance for data-intensive transforms.

## Decision

Use WebAssembly with the Wasmtime runtime. Define a stable ABI contract between the broker (host) and plugins (guests):

**Plugin exports (guest -> host):**
`init`, `process`, `poll`, `push`, `close`, `alloc`, `dealloc`

**Host exports (host -> guest):**
`surgewave_produce`, `surgewave_log`, `surgewave_get_config`, `surgewave_state_get`, `surgewave_state_put`

Four plugin types are supported: Source, Sink, Transform, and Function. Each type uses a subset of the exports (e.g., Source uses `poll`, Sink uses `push`, Transform uses `process`).

Plugins are deployed as `.wasm` files to the plugins directory. The broker watches for changes and loads new plugins without restart.

## Consequences

- **Language-agnostic:** Plugins can be written in Rust, Go, C, or any language that compiles to WASM.
- **Sandboxed execution:** A plugin cannot crash the broker, access the file system, or make network calls unless explicitly granted by the host.
- **Hot-deployable:** New plugins are picked up automatically without broker restart.
- **Performance trade-off:** WASM execution is slower than native .NET for compute-heavy tasks. Data must be serialized across the host-guest boundary via linear memory.
- **No GPU access:** WASM plugins cannot use hardware acceleration, which limits their use for ML inference workloads.
- **ABI versioning:** The ABI must be versioned carefully. Breaking changes require a new ABI version and a migration path.
