# Generator Connector

The Generator connector produces test messages with configurable templates. It's useful for testing, development, load generation, and benchmarking scenarios.

## Overview

- **Source-only Connector**: Generates messages based on templates
- No external dependencies
- Supports reproducible output with seed configuration
- Multiple placeholder types for dynamic content

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `generator.topic` | string | (required) | Target topic for generated messages |
| `generator.message.count` | long | 0 | Total messages to generate (0 = unlimited) |
| `generator.interval.ms` | long | 1000 | Interval between batches in milliseconds |
| `generator.batch.size` | int | 1 | Messages to generate per batch |
| `generator.key.template` | string | `${sequence}` | Template for message key |
| `generator.value.template` | string | (see below) | Template for message value |
| `generator.message.format` | string | json | Format: `json`, `string`, or `bytes` |
| `generator.sequence.start` | long | 1 | Starting value for sequence |
| `generator.sequence.step` | long | 1 | Increment for sequence |
| `generator.random.seed` | long | 0 | Random seed (0 = random) |
| `generator.random.string.length` | int | 10 | Length of random strings |
| `generator.random.int.min` | int | 0 | Minimum for random integers |
| `generator.random.int.max` | int | 1000 | Maximum for random integers |
| `generator.random.double.min` | double | 0.0 | Minimum for random doubles |
| `generator.random.double.max` | double | 1.0 | Maximum for random doubles |

### Default Value Template

```
{"id":${sequence},"timestamp":"${timestamp}","uuid":"${uuid}","value":${random_int}}
```

## Template Placeholders

| Placeholder | Description | Example Output |
|-------------|-------------|----------------|
| `${sequence}` | Incrementing sequence number | `1`, `2`, `3` |
| `${timestamp}` | ISO 8601 timestamp | `2024-01-15T10:30:00.0000000Z` |
| `${timestamp_ms}` | Unix timestamp in milliseconds | `1705315800000` |
| `${uuid}` | Random UUID | `550e8400-e29b-41d4-a716-446655440000` |
| `${random_int}` | Random integer in configured range | `42` |
| `${random_double}` | Random double in configured range | `0.123456` |
| `${random_string}` | Random alphanumeric string | `aB3cD4eF5g` |
| `${random_bool}` | Random boolean | `true` or `false` |
| `${topic}` | Target topic name | `my-topic` |
| `${partition}` | Partition number | `0` |

## Example Configurations

### Basic Test Data

```json
{
  "name": "test-generator",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Generator.GeneratorSourceConnector",
  "generator.topic": "test-data",
  "generator.message.count": "1000",
  "generator.batch.size": "100",
  "generator.interval.ms": "100"
}
```

### Load Testing

```json
{
  "name": "load-generator",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Generator.GeneratorSourceConnector",
  "generator.topic": "load-test",
  "generator.message.count": "0",
  "generator.batch.size": "1000",
  "generator.interval.ms": "0"
}
```

### Reproducible Test Data

```json
{
  "name": "reproducible-generator",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Generator.GeneratorSourceConnector",
  "generator.topic": "reproducible-data",
  "generator.random.seed": "42",
  "generator.message.count": "100"
}
```

### Custom JSON Structure

```json
{
  "name": "custom-generator",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Generator.GeneratorSourceConnector",
  "generator.topic": "custom-data",
  "generator.key.template": "user-${sequence}",
  "generator.value.template": "{\"userId\":${sequence},\"email\":\"user${sequence}@test.com\",\"score\":${random_int},\"active\":${random_bool}}"
}
```

### Sensor Data Simulation

```json
{
  "name": "sensor-generator",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Generator.GeneratorSourceConnector",
  "generator.topic": "sensor-readings",
  "generator.key.template": "sensor-${random_int}",
  "generator.value.template": "{\"sensorId\":\"sensor-${random_int}\",\"temperature\":${random_double},\"timestamp\":\"${timestamp}\"}",
  "generator.random.int.min": "1",
  "generator.random.int.max": "10",
  "generator.random.double.min": "15.0",
  "generator.random.double.max": "35.0",
  "generator.interval.ms": "500"
}
```

## Headers

Generated messages include the following headers:
- `generator.sequence`: Current sequence number
- `generator.count`: Total messages generated so far

## Offset Management

The connector tracks:
- Current sequence number
- Total messages generated

This enables resuming from the last position after a restart.

## Use Cases

### Testing
- Unit tests with predictable data
- Integration tests with controlled message flow
- End-to-end testing pipelines

### Development
- Prototyping consumers without real data
- Testing message formats
- Debugging message processing

### Load Testing
- Sustained throughput testing
- Consumer performance benchmarks
- Cluster capacity planning

### Demo Environments
- Showcasing functionality
- Training and documentation
- Proof of concept implementations

## Tips

1. **Unlimited Generation**: Set `generator.message.count` to 0 for continuous generation
2. **Maximum Throughput**: Set `generator.interval.ms` to 0 and increase `generator.batch.size`
3. **Reproducible Results**: Use `generator.random.seed` for deterministic output
4. **Custom Sequences**: Adjust `generator.sequence.start` and `generator.sequence.step` for non-standard sequences
