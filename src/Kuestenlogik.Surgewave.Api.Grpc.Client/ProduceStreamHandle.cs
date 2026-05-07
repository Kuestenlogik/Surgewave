using Grpc.Core;
using Google.Protobuf;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Client;

/// <summary>
/// Handle for bidirectional streaming produce operations.
/// Allows sending messages and receiving responses concurrently.
/// </summary>
public sealed class ProduceStreamHandle : IAsyncDisposable
{
    private readonly AsyncDuplexStreamingCall<ProduceRequest, ProduceResponse> _call;
    private bool _completed;

    internal ProduceStreamHandle(AsyncDuplexStreamingCall<ProduceRequest, ProduceResponse> call)
    {
        _call = call;
    }

    /// <summary>
    /// Send a message through the stream.
    /// </summary>
    public async Task SendAsync(
        string topic,
        byte[] value,
        byte[]? key = null,
        int partition = -1,
        Dictionary<string, byte[]>? headers = null,
        CancellationToken cancellationToken = default)
    {
        if (_completed)
            throw new InvalidOperationException("Stream has been completed");

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

        await _call.RequestStream.WriteAsync(request, cancellationToken);
    }

    /// <summary>
    /// Send a pre-built produce request through the stream.
    /// </summary>
    public async Task SendAsync(ProduceRequest request, CancellationToken cancellationToken = default)
    {
        if (_completed)
            throw new InvalidOperationException("Stream has been completed");

        await _call.RequestStream.WriteAsync(request, cancellationToken);
    }

    /// <summary>
    /// Read responses from the stream as they arrive.
    /// </summary>
    public async IAsyncEnumerable<ProduceResponse> ReadResponsesAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var response in _call.ResponseStream.ReadAllAsync(cancellationToken))
        {
            yield return response;
        }
    }

    /// <summary>
    /// Complete the request stream, signaling no more messages will be sent.
    /// </summary>
    public async Task CompleteAsync()
    {
        if (_completed)
            return;

        _completed = true;
        await _call.RequestStream.CompleteAsync();
    }

    /// <summary>
    /// Dispose the stream handle.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!_completed)
        {
            try
            {
                await _call.RequestStream.CompleteAsync();
            }
            catch
            {
                // Ignore errors during disposal
            }
        }

        _call.Dispose();
    }
}
