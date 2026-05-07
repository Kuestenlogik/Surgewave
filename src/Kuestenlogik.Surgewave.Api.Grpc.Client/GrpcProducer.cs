using Grpc.Net.Client;
using Google.Protobuf;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Client;

/// <summary>
/// gRPC-based producer client - language independent
/// </summary>
public sealed class GrpcProducer : IAsyncDisposable
{
    private readonly GrpcChannel _channel;
    private readonly ProducerService.ProducerServiceClient _client;

    public GrpcProducer(string address)
    {
        _channel = GrpcChannel.ForAddress(address);
        _client = new ProducerService.ProducerServiceClient(_channel);
    }

    /// <summary>
    /// Send a single message
    /// </summary>
    public async Task<ProduceResponse> SendAsync(
        string topic,
        byte[] value,
        byte[]? key = null,
        int partition = -1,
        Dictionary<string, byte[]>? headers = null,
        CancellationToken cancellationToken = default)
    {
        var record = new Record
        {
            Value = ByteString.CopyFrom(value),
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        if (key != null)
        {
            record.Key = ByteString.CopyFrom(key);
        }

        if (headers != null)
        {
            foreach (var (k, v) in headers)
            {
                record.Headers[k] = ByteString.CopyFrom(v);
            }
        }

        var request = new ProduceRequest
        {
            Topic = topic,
            Partition = partition,
            Record = record,
            AcksRequired = true
        };

        return await _client.ProduceAsync(request, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Send multiple messages in a batch
    /// </summary>
    public async Task<ProduceBatchResponse> SendBatchAsync(
        IEnumerable<(string topic, byte[] value, byte[]? key, int partition)> messages,
        CancellationToken cancellationToken = default)
    {
        using var call = _client.ProduceBatch(cancellationToken: cancellationToken);

        foreach (var (topic, value, key, partition) in messages)
        {
            var record = new Record
            {
                Value = ByteString.CopyFrom(value),
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            if (key != null)
            {
                record.Key = ByteString.CopyFrom(key);
            }

            var request = new ProduceRequest
            {
                Topic = topic,
                Partition = partition,
                Record = record,
                AcksRequired = true
            };

            await call.RequestStream.WriteAsync(request);
        }

        await call.RequestStream.CompleteAsync();

        return await call.ResponseAsync;
    }

    /// <summary>
    /// Bidirectional streaming producer - send messages and receive responses in real-time.
    /// Returns a stream handle that allows sending messages and receiving responses concurrently.
    /// </summary>
    public ProduceStreamHandle OpenProduceStream(CancellationToken cancellationToken = default)
    {
        var call = _client.ProduceStream(cancellationToken: cancellationToken);
        return new ProduceStreamHandle(call);
    }

    public async ValueTask DisposeAsync()
    {
        await _channel.ShutdownAsync();
        _channel.Dispose();
    }
}
