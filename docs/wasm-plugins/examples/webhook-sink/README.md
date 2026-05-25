# HTTP Webhook Sink WASM Plugin

Forwards each consumed Surgewave message as an HTTP POST request to a configurable webhook endpoint.

## Building from Rust

```bash
rustup target add wasm32-wasi
cargo init --lib webhook-sink
cd webhook-sink
```

Add to `Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
# Note: WASI networking requires the host to provide socket access
```

Implement in `src/lib.rs`:

```rust
use std::slice;

// In a real implementation, the host provides HTTP functions via imports.
// Surgewave exposes `surgewave_http_post(url_ptr, url_len, body_ptr, body_len) -> i32`
// as a host function for WASM plugins with network access.

extern "C" {
    fn surgewave_http_post(url_ptr: *const u8, url_len: u32, body_ptr: *const u8, body_len: u32) -> i32;
}

static mut URL: &[u8] = b"https://hooks.example.com/surgewave";

/// Called by Surgewave host for each consumed record.
/// The plugin sends the record body to the configured webhook URL.
#[no_mangle]
pub extern "C" fn surgewave_put(ptr: *const u8, len: u32) -> i32 {
    let url = unsafe { URL };
    let status = unsafe {
        surgewave_http_post(url.as_ptr(), url.len() as u32, ptr, len)
    };

    if status >= 200 && status < 300 {
        0 // success
    } else {
        -1 // error — Surgewave will retry based on config
    }
}

#[no_mangle]
pub extern "C" fn surgewave_init() {}

#[no_mangle]
pub extern "C" fn surgewave_stop() {}
```

Build:

```bash
cargo build --target wasm32-wasi --release
cp target/wasm32-wasi/release/webhook_sink.wasm ../plugin.wasm
```

## Important Notes

- Network access requires `Surgewave:Wasm:AllowNetworkAccess=true` in broker configuration
- The `surgewave_http_post` host function is provided by the Surgewave WASM runtime
- Retry logic is handled by Surgewave based on the plugin's return code and config

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `url` | (required) | Webhook endpoint URL |
| `method` | `POST` | HTTP method (POST or PUT) |
| `content.type` | `application/json` | Content-Type header value |
| `headers` | (none) | Additional headers as `key:value` pairs, comma-separated |
| `retry.max` | `3` | Maximum retry attempts on failure |
| `retry.delay.ms` | `1000` | Delay between retries in milliseconds |
