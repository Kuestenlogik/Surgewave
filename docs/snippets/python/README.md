# Python gRPC Examples

These examples demonstrate using Surgewave from Python via gRPC.

## Setup

1. Generate Python gRPC code from the protobuf:

```bash
# Install dependencies
pip install grpcio grpcio-tools

# Generate Python code from proto file
python -m grpc_tools.protoc \
    -I../../src/Kuestenlogik.Surgewave.Grpc/Protos \
    --python_out=. \
    --grpc_python_out=. \
    ../../src/Kuestenlogik.Surgewave.Grpc/Protos/streaming.proto
```

2. Run the examples:

```bash
# Producer
python producer.py

# Consumer (in another terminal)
python consumer.py
```

## Requirements

```
grpcio==1.60.0
grpcio-tools==1.60.0
```

## Features Demonstrated

- **Producer**: Send messages to topics
- **Consumer**: Stream messages from topics
- **Error Handling**: Handle gRPC errors gracefully
- **Language Independence**: Same broker, different language!
