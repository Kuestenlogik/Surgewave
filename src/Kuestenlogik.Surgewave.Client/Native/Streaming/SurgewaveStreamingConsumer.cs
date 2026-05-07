using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Streaming;

namespace Kuestenlogik.Surgewave.Client.Native.Streaming;

/// <summary>
/// Server-push streaming consumer for Surgewave native protocol.
///
/// Opens a persistent subscription on one or more topic partitions. The broker
/// pushes record batches to the client as new data arrives, eliminating the
/// polling overhead of the pull-based <see cref="ReceiveBuilder"/> path.
///
/// Usage:
/// <code>
/// await using var consumer = await client.Messaging.SubscribeAsync(
///     "orders", partitions: [0, 1, 2], startOffset: -1);
///
/// await foreach (var msg in consumer.Records)
/// {
///     Console.WriteLine(msg.ValueString);
/// }
/// </code>
/// </summary>
public sealed class SurgewaveStreamingConsumer : IAsyncDisposable
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _subscriptionId;
    private readonly string _topic;
    private readonly int[] _partitions;
    private readonly Channel<ReceivedMessage> _channel;
    private readonly CancellationTokenSource _cts;
    private readonly Task _ackTask;

    // Total bytes received; used for flow-control acknowledgments
    private long _receivedBytes;

    private const int ChannelCapacity = 4096;
    private static readonly TimeSpan AckInterval = TimeSpan.FromSeconds(5);

    private SurgewaveStreamingConsumer(
        SurgewaveNativeClient client,
        string subscriptionId,
        string topic,
        int[] partitions)
    {
        _client = client;
        _subscriptionId = subscriptionId;
        _topic = topic;
        _partitions = partitions;
        _cts = new CancellationTokenSource();

        _channel = Channel.CreateBounded<ReceivedMessage>(new BoundedChannelOptions(ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false,
            AllowSynchronousContinuations = false
        });

        // Register push handler so the transport routes broker-pushed FetchResponse
        // frames (RequestId == 0) to our handler
        _client.RegisterPushHandler(SurgewaveOpCode.FetchResponse, HandlePushAsync);

        // Start periodic ack background task
        _ackTask = Task.Run(() => AckLoopAsync(_cts.Token));
    }

    /// <summary>
    /// Subscribe to a topic and return a streaming consumer.
    /// </summary>
    /// <param name="client">Connected Surgewave native client.</param>
    /// <param name="topic">Topic to subscribe to.</param>
    /// <param name="partitions">Partitions to subscribe to. Pass empty array for all partitions.</param>
    /// <param name="startOffset">Starting offset. Use -1 for latest, -2 for earliest.</param>
    /// <param name="maxBytesPerPush">Maximum bytes the broker may push per batch.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task<SurgewaveStreamingConsumer> SubscribeAsync(
        SurgewaveNativeClient client,
        string topic,
        int[] partitions,
        long startOffset = -1,
        int maxBytesPerPush = 1024 * 1024,
        CancellationToken cancellationToken = default)
    {
        var subscriptionId = Guid.NewGuid().ToString("N");

        // Build per-partition start offsets (all partitions use the same startOffset)
        var partitionOffsets = partitions.Length == 0
            ? Array.Empty<PartitionOffset>()
            : Array.ConvertAll(partitions, p => new PartitionOffset(p, startOffset));

        var payload = new SubscribePayload
        {
            SubscriptionId = subscriptionId,
            Topic = topic,
            Partitions = partitionOffsets,
            MaxBytesPerPush = maxBytesPerPush
        };

        var buffer = ArrayPool<byte>.Shared.Rent(payload.EstimateSize() + 32);
        try
        {
            var writer = new SurgewavePayloadWriter(buffer);
            payload.Write(ref writer);

            var (header, _) = await client.SendRequestAsync(
                SurgewaveOpCode.Subscribe,
                buffer.AsMemory(0, writer.Position),
                cancellationToken);

            if (header.ErrorCode != SurgewaveErrorCode.None)
                throw new ProtocolException(SurgewaveOpCode.Subscribe, header.ErrorCode);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        return new SurgewaveStreamingConsumer(client, subscriptionId, topic, partitions);
    }

    /// <summary>
    /// Stream of records pushed by the broker. Enumerate this with <c>await foreach</c>.
    /// The enumeration ends when the consumer is disposed.
    /// </summary>
    public IAsyncEnumerable<ReceivedMessage> Records => ReadRecordsAsync(_cts.Token);

    private async IAsyncEnumerable<ReceivedMessage> ReadRecordsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var msg in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return msg;
        }
    }

    /// <summary>
    /// Handle a server-pushed FetchResponse frame (RequestId == 0).
    /// Parses the StreamRecordPayload and writes each message to the internal channel.
    /// </summary>
    private async Task HandlePushAsync(SurgewaveResponseHeader header, ReadOnlyMemory<byte> rawPayload)
    {
        try
        {
            var reader = new SurgewavePayloadReader(rawPayload.Span);
            var batch = StreamRecordPayload.Read(ref reader);

            // Ignore pushes for other subscriptions
            if (batch.SubscriptionId != _subscriptionId)
                return;

            foreach (var msg in batch.Messages)
            {
                var received = new ReceivedMessage(
                    msg.Offset,
                    msg.Timestamp,
                    msg.Key,
                    msg.Value);

                await _channel.Writer.WriteAsync(received, _cts.Token);
                Interlocked.Add(ref _receivedBytes, rawPayload.Length);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown in progress
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
        {
            // Log but don't crash the reader loop on a malformed push frame
            System.Diagnostics.Debug.WriteLine($"StreamingConsumer: malformed push frame: {ex.Message}");
        }
    }

    /// <summary>
    /// Periodically send StreamAck to allow the broker to advance its flow-control window.
    /// </summary>
    private async Task AckLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(AckInterval, cancellationToken);

                var acked = Interlocked.Exchange(ref _receivedBytes, 0);
                if (acked <= 0)
                    continue;

                var ackPayload = new StreamAckPayload
                {
                    SubscriptionId = _subscriptionId,
                    AcknowledgedBytes = acked
                };

                var buffer = ArrayPool<byte>.Shared.Rent(ackPayload.EstimateSize() + 16);
                try
                {
                    var writer = new SurgewavePayloadWriter(buffer);
                    ackPayload.Write(ref writer);

                    await _client.SendRequestAsync(
                        SurgewaveOpCode.StreamAck,
                        buffer.AsMemory(0, writer.Position),
                        cancellationToken);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown
        }
        catch
        {
            // Don't crash the consumer on ack failure
        }
    }

    /// <summary>
    /// Send Unsubscribe and release resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // Stop the ack loop and channel producer
        await _cts.CancelAsync();
        _channel.Writer.TryComplete();

        try { await _ackTask; } catch (OperationCanceledException) { }

        // Deregister the push handler
        _client.UnregisterPushHandler(SurgewaveOpCode.FetchResponse);

        // Best-effort Unsubscribe
        try
        {
            var unsubPayload = new UnsubscribePayload { SubscriptionId = _subscriptionId };
            var buffer = ArrayPool<byte>.Shared.Rent(unsubPayload.EstimateSize() + 16);
            try
            {
                var writer = new SurgewavePayloadWriter(buffer);
                unsubPayload.Write(ref writer);

                await _client.SendRequestAsync(
                    SurgewaveOpCode.Unsubscribe,
                    buffer.AsMemory(0, writer.Position),
                    CancellationToken.None);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        catch
        {
            // Ignore errors on cleanup
        }

        _cts.Dispose();
    }
}
