# gRPC API

Language-independent gRPC API for cross-platform clients.

## Overview

Surgewave exposes a gRPC API:
- **11 services** with **75 RPC methods**
- Bidirectional streaming
- HTTP/2 transport
- Language-agnostic (Python, Java, Go, etc.)

## Services

| Service | Methods | Description |
|---------|---------|-------------|
| ProducerService | 3 | Produce, ProduceBatch, ProduceStream |
| ConsumerService | 4 | Consume, Fetch, Commit, ListOffsets |
| ConsumerGroupService | 4 | JoinGroup, SyncGroup, LeaveGroup, Heartbeat |
| TopicService | 4 | CreateTopic, DeleteTopic, ListTopics, DescribeTopic |
| ClusterService | 3 | Ping, GetMetadata, GetClusterInfo |
| TransactionService | 2 | InitProducerId, EndTxn |
| AdminService | 2 | ElectLeader, DescribeBrokerConfig |
| QuotaService | 2 | GetQuotaConfig, SetQuotaConfig |
| SecurityService | 3 | DescribeAcls, CreateAcls, DeleteAcls |
| SchemaRegistryService | 11 | Schema management |
| ConnectService | 13 | Connector management |

## Configuration

```json
{
  "Surgewave": {
    "GrpcPort": 9093
  }
}
```

## .NET Client

```csharp
using Grpc.Net.Client;
using Kuestenlogik.Surgewave.Grpc;

var channel = GrpcChannel.ForAddress("https://localhost:9093");
var client = new ProducerService.ProducerServiceClient(channel);

// Produce
await client.ProduceAsync(new ProduceRequest
{
    Topic = "my-topic",
    Key = ByteString.CopyFromUtf8("key"),
    Value = ByteString.CopyFromUtf8("value")
});

// Streaming produce
using var call = client.ProduceStream();
await call.RequestStream.WriteAsync(new ProduceRequest { Topic = "topic", Value = data1 });
await call.RequestStream.WriteAsync(new ProduceRequest { Topic = "topic", Value = data2 });
await call.RequestStream.CompleteAsync();
```

## Python Client

### Generate Stubs

```bash
pip install grpcio grpcio-tools
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. surgewave.proto
```

### Producer Example

```python
import grpc
import surgewave_pb2
import surgewave_pb2_grpc

channel = grpc.insecure_channel('localhost:9093')
stub = surgewave_pb2_grpc.ProducerServiceStub(channel)

response = stub.Produce(surgewave_pb2.ProduceRequest(
    topic='my-topic',
    key=b'key',
    value=b'value'
))
print(f'Offset: {response.offset}')
```

### Consumer Example

```python
stub = surgewave_pb2_grpc.ConsumerServiceStub(channel)

for message in stub.Consume(surgewave_pb2.ConsumeRequest(
    topic='my-topic',
    partition=0,
    offset=0
)):
    print(f'Key: {message.key}, Value: {message.value}')
```

## Go Client

```go
import (
    "google.golang.org/grpc"
    pb "surgewave/proto"
)

conn, _ := grpc.Dial("localhost:9093", grpc.WithInsecure())
client := pb.NewProducerServiceClient(conn)

resp, _ := client.Produce(context.Background(), &pb.ProduceRequest{
    Topic: "my-topic",
    Key:   []byte("key"),
    Value: []byte("value"),
})
```

## Java Client

```java
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("localhost", 9093)
    .usePlaintext()
    .build();

ProducerServiceGrpc.ProducerServiceBlockingStub stub =
    ProducerServiceGrpc.newBlockingStub(channel);

ProduceResponse response = stub.produce(ProduceRequest.newBuilder()
    .setTopic("my-topic")
    .setKey(ByteString.copyFromUtf8("key"))
    .setValue(ByteString.copyFromUtf8("value"))
    .build());
```

## Streaming APIs

### Bidirectional Streaming

```csharp
using var call = client.ProduceStream();

// Producer task
var producerTask = Task.Run(async () =>
{
    for (int i = 0; i < 1000; i++)
    {
        await call.RequestStream.WriteAsync(new ProduceRequest
        {
            Topic = "events",
            Value = ByteString.CopyFromUtf8($"event-{i}")
        });
    }
    await call.RequestStream.CompleteAsync();
});

// Consumer task
var consumerTask = Task.Run(async () =>
{
    await foreach (var response in call.ResponseStream.ReadAllAsync())
    {
        Console.WriteLine($"Offset: {response.Offset}");
    }
});

await Task.WhenAll(producerTask, consumerTask);
```

## Flow Control

gRPC streaming includes flow control:

```csharp
var options = new GrpcChannelOptions
{
    MaxReceiveMessageSize = 16 * 1024 * 1024,  // 16 MB
    MaxSendMessageSize = 16 * 1024 * 1024
};
var channel = GrpcChannel.ForAddress("https://localhost:9093", options);
```

## Proto Definitions

Proto files are available at:
```
src/Kuestenlogik.Surgewave.Grpc/Protos/
├── surgewave.proto
├── producer.proto
├── consumer.proto
├── admin.proto
└── ...
```

## Performance

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Unary Produce | ~1 ms | 100K msg/s |
| Streaming Produce | ~0.5 ms | 500K msg/s |
| Unary Consume | ~1 ms | 150K msg/s |
| Streaming Consume | ~0.3 ms | 600K msg/s |

## Next Steps

- [Shared Memory](shared-memory.md) - Ultra-low latency
- [Samples](../samples/index.md) - Complete examples
