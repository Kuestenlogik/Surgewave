# JSON Field Filter WASM Plugin

Filters JSON messages by evaluating a condition on a specified field.
Messages that do not match are dropped from the stream.

## Building from Rust

```bash
rustup target add wasm32-wasi
cargo init --lib filter-json
cd filter-json
```

Add to `Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
serde_json = "1"
```

Implement in `src/lib.rs`:

```rust
use serde_json::Value;
use std::slice;

static mut FIELD: &str = "severity";
static mut OPERATOR: &str = "gte";
static mut THRESHOLD: f64 = 3.0;

#[no_mangle]
pub extern "C" fn surgewave_process(ptr: *const u8, len: u32) -> u64 {
    let input = unsafe { slice::from_raw_parts(ptr, len as usize) };
    let Ok(json) = serde_json::from_slice::<Value>(input) else {
        return 0; // drop malformed JSON
    };

    let field_val = unsafe { &json[FIELD] };
    let num = field_val.as_f64().unwrap_or(0.0);
    let threshold = unsafe { THRESHOLD };

    let pass = unsafe {
        match OPERATOR {
            "eq" => (num - threshold).abs() < f64::EPSILON,
            "gt" => num > threshold,
            "gte" => num >= threshold,
            "lt" => num < threshold,
            "lte" => num <= threshold,
            _ => true,
        }
    };

    if pass {
        // Return the original message unchanged
        ((ptr as u64) << 32) | (len as u64)
    } else {
        0 // drop
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
cp target/wasm32-wasi/release/filter_json.wasm ../plugin.wasm
```

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `field` | `severity` | JSON field name to evaluate |
| `operator` | `gte` | Comparison operator: `eq`, `gt`, `gte`, `lt`, `lte` |
| `value` | `3` | Threshold value for comparison |
