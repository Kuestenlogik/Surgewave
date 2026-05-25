using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Broker.Native.Streaming;
using Kuestenlogik.Surgewave.Protocol.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol push-streaming operations: Subscribe, Unsubscribe, StreamAck.
///
/// Subscribe (0x0202): Client requests push delivery for a topic/partitions.
/// Unsubscribe (0x0203): Client cancels an active push subscription.
/// StreamAck (0x0206): Client returns credit bytes for flow control.
///
/// Push frame wire format (sent server → client via push delegate):
///   subscriptionId string (2-byte length prefix + UTF-8)
///   partition      int32
///   highWatermark  int64
///   messageCount   int32
///   payload        raw message bytes (same layout as FetchResponse)
/// </summary>
public sealed class NativeStreamingHandler : INativeRequestHandler
{
    private readonly ILogger<NativeStreamingHandler> _logger;

    public IEnumerable<SurgewaveOpCode> SupportedOpCodes =>
    [
        SurgewaveOpCode.Subscribe,
        SurgewaveOpCode.Unsubscribe,
        SurgewaveOpCode.StreamAck
    ];

    public NativeStreamingHandler(ILogger<NativeStreamingHandler> logger)
    {
        _logger = logger;
    }

    public Task HandleAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        return context.Header.OpCode switch
        {
            SurgewaveOpCode.Subscribe => HandleSubscribeAsync(context, payload, cancellationToken),
            SurgewaveOpCode.Unsubscribe => HandleUnsubscribeAsync(context, payload, cancellationToken),
            SurgewaveOpCode.StreamAck => HandleStreamAckAsync(context, payload, cancellationToken),
            _ => Task.CompletedTask
        };
    }

    /// <summary>
    /// Subscribe payload wire format:
    ///   subscriptionId  string (int16 length + UTF-8)
    ///   topic           string (int16 length + UTF-8)
    ///   partitionCount  int32
    ///   for each partition:
    ///     partition     int32
    ///     startOffset   int64
    ///   maxBytesPerPush int32
    /// </summary>
    private async Task HandleSubscribeAsync(
        NativeRequestContext context,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        var manager = context.SubscriptionManager;
        if (manager == null)
        {
            await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.InvalidRequest, "Streaming not available for this connection", cancellationToken);
            return;
        }

        var reader = new SurgewavePayloadReader(payload.Span);

        var subscriptionId = reader.ReadString();
        var topic = reader.ReadString();
        var partitionCount = reader.ReadInt32();

        if (string.IsNullOrEmpty(subscriptionId) || string.IsNullOrEmpty(topic) || partitionCount <= 0)
        {
            await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.InvalidRequest, "Subscribe: subscriptionId, topic, and partitions are required", cancellationToken);
            return;
        }

        var partitions = new int[partitionCount];
        var offsets = new long[partitionCount];

        for (var i = 0; i < partitionCount; i++)
        {
            partitions[i] = reader.ReadInt32();
            offsets[i] = reader.ReadInt64();
        }

        var maxBytesPerPush = reader.Remaining >= 4 ? reader.ReadInt32() : 1024 * 1024;

        // Build a send delegate that sends push frames via SendResponseAsync.
        // The subscriptionId, partition, hwm, and count are embedded in the payload itself
        // so the client can demultiplex push frames.
        var sendResponseAsync = context.SendResponseAsync;
        var clientSupportsCompression = context.ClientSupportsCompression;
        var requestId = context.Header.RequestId;

        async Task PushDelegate(string subId, int partition, long hwm, int msgCount, ReadOnlyMemory<byte> data, CancellationToken ct)
        {
            // Build push frame payload:
            //   subIdLen(2) + subIdBytes + partition(4) + hwm(8) + msgCount(4) + data
            var subIdBytes = Encoding.UTF8.GetBytes(subId);
            var frameSize = 2 + subIdBytes.Length + 4 + 8 + 4 + data.Length;
            var frame = new byte[frameSize];
            var pos = 0;

            BinaryPrimitives.WriteInt16BigEndian(frame.AsSpan(pos, 2), (short)subIdBytes.Length);
            pos += 2;
            subIdBytes.CopyTo(frame, pos);
            pos += subIdBytes.Length;

            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(pos, 4), partition);
            pos += 4;

            BinaryPrimitives.WriteInt64BigEndian(frame.AsSpan(pos, 8), hwm);
            pos += 8;

            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(pos, 4), msgCount);
            pos += 4;

            data.Span.CopyTo(frame.AsSpan(pos));

            // Use requestId 0 for push frames (unsolicited server-push)
            await sendResponseAsync(0u, SurgewaveOpCode.FetchResponse, SurgewaveErrorCode.None, frame, ct);
        }

        var started = manager.Subscribe(
            subscriptionId!,
            topic!,
            partitions,
            offsets,
            maxBytesPerPush,
            PushDelegate);

        if (!started)
        {
            await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.InvalidRequest,
                $"Subscribe failed: duplicate subscriptionId or max subscriptions reached",
                cancellationToken);
            return;
        }

        // Send SubscribeAck: echo back the subscriptionId
        var ackBytes = Encoding.UTF8.GetBytes(subscriptionId!);
        var ackPayload = new byte[2 + ackBytes.Length];
        BinaryPrimitives.WriteInt16BigEndian(ackPayload.AsSpan(0, 2), (short)ackBytes.Length);
        ackBytes.CopyTo(ackPayload, 2);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.Subscribe,
            SurgewaveErrorCode.None, ackPayload, cancellationToken);

        _logger.LogDebug("Client subscribed: {SubscriptionId} topic={Topic} partitions={PartitionCount}",
            subscriptionId, topic, partitionCount);
    }

    /// <summary>
    /// Unsubscribe payload wire format:
    ///   subscriptionId  string (int16 length + UTF-8)
    /// </summary>
    private async Task HandleUnsubscribeAsync(
        NativeRequestContext context,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        var manager = context.SubscriptionManager;
        if (manager == null)
        {
            await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.InvalidRequest, "Streaming not available for this connection", cancellationToken);
            return;
        }

        var reader = new SurgewavePayloadReader(payload.Span);
        var subscriptionId = reader.ReadString();

        if (string.IsNullOrEmpty(subscriptionId))
        {
            await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.InvalidRequest, "Unsubscribe: subscriptionId is required", cancellationToken);
            return;
        }

        var removed = await manager.UnsubscribeAsync(subscriptionId!);

        var ackBytes = Encoding.UTF8.GetBytes(subscriptionId!);
        var ackPayload = new byte[2 + ackBytes.Length + 1];
        BinaryPrimitives.WriteInt16BigEndian(ackPayload.AsSpan(0, 2), (short)ackBytes.Length);
        ackBytes.CopyTo(ackPayload, 2);
        ackPayload[2 + ackBytes.Length] = removed ? (byte)1 : (byte)0;

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.Unsubscribe,
            SurgewaveErrorCode.None, ackPayload, cancellationToken);

        _logger.LogDebug("Client unsubscribed: {SubscriptionId} (found={Found})", subscriptionId, removed);
    }

    /// <summary>
    /// StreamAck payload wire format (flow control credit):
    ///   subscriptionId  string (int16 length + UTF-8)
    ///   creditBytes     int64  (number of bytes the client is ready to receive)
    /// </summary>
    private Task HandleStreamAckAsync(
        NativeRequestContext context,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        var manager = context.SubscriptionManager;
        if (manager == null)
            return Task.CompletedTask;

        var reader = new SurgewavePayloadReader(payload.Span);
        var subscriptionId = reader.ReadString();

        if (string.IsNullOrEmpty(subscriptionId) || reader.Remaining < 8)
            return Task.CompletedTask;

        var creditBytes = reader.ReadInt64();
        manager.AddCredit(subscriptionId!, creditBytes);

        return Task.CompletedTask;
    }
}
