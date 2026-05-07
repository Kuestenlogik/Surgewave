# Surgewave Source Code

Core source code for the Surgewave message broker.

## Project Structure

### Core Components

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Core` | Core models, storage interfaces, utilities |
| `Kuestenlogik.Surgewave.Broker` | Main broker implementation |
| `Kuestenlogik.Surgewave.Cli` | Command-line interface (`surgewave` command) |

### Protocol Layer

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Protocol` | Base protocol abstractions |
| `Kuestenlogik.Surgewave.Protocol.Kafka` | Kafka wire protocol (full compatibility) |
| `Kuestenlogik.Surgewave.Protocol.Native` | High-performance native protocol |

### Client Libraries

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Client` | Primary client library |
| `Kuestenlogik.Surgewave.Runtime` | In-process embedded SurgewaveRuntime |

### Storage Engines

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Storage` | Log storage abstraction |
| `Kuestenlogik.Surgewave.Storage.Engine` | Storage engine interface |
| `Kuestenlogik.Surgewave.Storage.Engine.FileSystem` | Default file-based storage |
| `Kuestenlogik.Surgewave.Storage.Engine.Memory` | In-memory storage (testing) |
| `Kuestenlogik.Surgewave.Storage.Engine.Arrow` | Apache Arrow columnar storage |

### Tiered Storage

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Storage.Tiering` | Tiered storage abstraction |
| `Kuestenlogik.Surgewave.Storage.Tiering.S3` | Amazon S3 backend |
| `Kuestenlogik.Surgewave.Storage.Tiering.Azure` | Azure Blob Storage backend |
| `Kuestenlogik.Surgewave.Storage.Tiering.Gcp` | Google Cloud Storage backend |

### Clustering

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Clustering` | Raft consensus, replication |

### APIs

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Api.Grpc` | gRPC API definitions |
| `Kuestenlogik.Surgewave.Api.Grpc.Server` | gRPC server implementation |

### Schema Registry

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Schema.Registry` | Schema Registry core |
| `Kuestenlogik.Surgewave.Schema.Registry.Handlers.Avro` | Avro schema support |
| `Kuestenlogik.Surgewave.Schema.Registry.Handlers.Json` | JSON Schema support |
| `Kuestenlogik.Surgewave.Schema.Registry.Handlers.Protobuf` | Protobuf schema support |
| `Kuestenlogik.Surgewave.Schema.Registry.Handlers.FlatBuffers` | FlatBuffers support |

### Connect Framework

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Connect` | Kafka Connect compatible framework |
| `Kuestenlogik.Surgewave.Connect.Http` | HTTP source/sink connectors |
| `Kuestenlogik.Surgewave.Connect.Mirror` | MirrorMaker for cross-cluster replication |

### Stream Processing

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Streams` | Stream processing DSL |

### Transport Layer

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Transport` | Transport abstraction |
| `Kuestenlogik.Surgewave.Transport.Tcp` | TCP transport |
| `Kuestenlogik.Surgewave.Transport.SharedMemory` | Shared memory (IPC) |

## Building

```bash
# Build all
dotnet build

# Build specific project
dotnet build src/Kuestenlogik.Surgewave.Broker

# Build in release mode
dotnet build -c Release
```

## Running

```bash
# Start broker
dotnet run --project src/Kuestenlogik.Surgewave.Broker

# Use CLI
dotnet run --project src/Kuestenlogik.Surgewave.Cli -- <command>
```

## Architecture

See [ARCHITECTURE.md](../ARCHITECTURE.md) for detailed architecture documentation.
