# Simulated Sensor Source WASM Plugin

Generates simulated IoT sensor data (temperature, humidity, pressure) and publishes JSON messages to a Surgewave topic.

## Building from Rust

```bash
rustup target add wasm32-wasi
cargo init --lib sensor-source
cd sensor-source
```

Add to `Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]
```

Implement in `src/lib.rs`:

```rust
use std::time::{SystemTime, UNIX_EPOCH};

static mut COUNTER: u64 = 0;

/// Called by Surgewave host to poll for new records.
/// Returns pointer+length of a JSON message, or 0 when no data is available.
#[no_mangle]
pub extern "C" fn surgewave_poll() -> u64 {
    unsafe { COUNTER += 1 };
    let counter = unsafe { COUNTER };

    // Simple pseudo-random based on counter
    let temp = 18.0 + (counter % 100) as f64 * 0.1;
    let humidity = 30.0 + (counter % 500) as f64 * 0.1;
    let pressure = 1013.25 + (counter % 50) as f64 * 0.1;

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let json = format!(
        r#"{{"sensor_id":"sensor-001","temperature":{temp:.1},"humidity":{humidity:.1},"pressure":{pressure:.2},"timestamp":{ts}}}"#
    );

    let bytes = json.into_bytes();
    let len = bytes.len() as u32;
    let ptr = bytes.as_ptr() as u32;
    std::mem::forget(bytes);
    ((ptr as u64) << 32) | (len as u64)
}

#[no_mangle]
pub extern "C" fn surgewave_init() {}

#[no_mangle]
pub extern "C" fn surgewave_stop() {}
```

Build:

```bash
cargo build --target wasm32-wasi --release
cp target/wasm32-wasi/release/sensor_source.wasm ../plugin.wasm
```

## Output Format

```json
{
  "sensor_id": "sensor-001",
  "temperature": 22.5,
  "humidity": 55.3,
  "pressure": 1015.75,
  "timestamp": 1711929600000
}
```

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `interval.ms` | `1000` | Polling interval in milliseconds |
| `sensor.id` | `sensor-001` | Sensor identifier in output messages |
| `temp.min` | `18.0` | Minimum temperature value |
| `temp.max` | `28.0` | Maximum temperature value |
| `humidity.min` | `30.0` | Minimum humidity percentage |
| `humidity.max` | `80.0` | Maximum humidity percentage |
