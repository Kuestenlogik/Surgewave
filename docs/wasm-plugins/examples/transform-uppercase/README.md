# Uppercase Transform WASM Plugin

Converts all string values in JSON messages to uppercase.

## Building from Rust

```bash
# Install Rust wasm32-wasi target
rustup target add wasm32-wasi

# Create project
cargo init --lib uppercase-transform
cd uppercase-transform
```

Add to `Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]
```

Implement in `src/lib.rs`:

```rust
use std::slice;

/// Called by Surgewave host to process a message.
/// Returns pointer to output buffer, or 0 to drop the message.
#[no_mangle]
pub extern "C" fn surgewave_process(ptr: *const u8, len: u32) -> u64 {
    let input = unsafe { slice::from_raw_parts(ptr, len as usize) };
    let text = String::from_utf8_lossy(input);
    let upper = text.to_uppercase();
    let bytes = upper.into_bytes();
    let len = bytes.len() as u32;
    let ptr = bytes.as_ptr() as u32;
    std::mem::forget(bytes);
    ((ptr as u64) << 32) | (len as u64)
}

/// Called once when the plugin is loaded.
#[no_mangle]
pub extern "C" fn surgewave_init() {}

/// Called when the plugin is stopped.
#[no_mangle]
pub extern "C" fn surgewave_stop() {}
```

Build:

```bash
cargo build --target wasm32-wasi --release
cp target/wasm32-wasi/release/uppercase_transform.wasm ../plugin.wasm
```

## Deployment

1. Copy `plugin.wasm` and `wasm-plugin.json` to `wasm-plugins/transform-uppercase/`
2. Enable WASM plugins: `Surgewave:Wasm:Enabled=true`
3. The plugin is auto-discovered and can be loaded via REST API or Control UI

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `fields` | `*` | Comma-separated field names to uppercase, or `*` for all |
| `recursive` | `true` | Process nested objects recursively |
