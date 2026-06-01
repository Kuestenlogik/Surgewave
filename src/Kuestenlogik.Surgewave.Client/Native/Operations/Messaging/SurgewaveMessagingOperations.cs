using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Client.Native.Streaming;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Send and receive operations for Surgewave native client.
/// </summary>
public sealed class SurgewaveMessagingOperations
{
    private readonly SurgewaveNativeClient _client;

    internal SurgewaveMessagingOperations(SurgewaveNativeClient client) => _client = client;

    /// <summary>
    /// Send a single message. For high-throughput, use SendBatchAsync or SurgewaveBatchingProducer.
    /// </summary>
    public Task<long> SendAsync(string topic, int partition, byte[]? key, byte[] value, CancellationToken cancellationToken = default)
        => SendAsync(topic, partition, key, value, headers: null, cancellationToken);

    /// <summary>
    /// Send a single message with optional headers.
    /// </summary>
    public async Task<long> SendAsync(string topic, int partition, byte[]? key, byte[] value, IReadOnlyDictionary<string, byte[]>? headers, CancellationToken cancellationToken = default)
    {
        var headerSize = NativeMessageHeaderCodec.EncodedSize(headers);
        var bufferSize = 256 + (key?.Length ?? 0) + value.Length + headerSize;
        var payloadBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            var writer = new SurgewavePayloadWriter(payloadBuffer);
            writer.WriteString(topic);
            writer.WriteInt32(partition);
            writer.WriteInt32(1);

            if (key != null && key.Length > 0)
            {
                writer.WriteInt32(key.Length);
                writer.WriteRaw(key);
            }
            else
            {
                writer.WriteInt32(-1);
            }
            writer.WriteBytes(value);
            var headerWritten = NativeMessageHeaderCodec.Encode(headers, payloadBuffer.AsSpan(writer.Position));
            writer.Advance(headerWritten);

            var (header, responsePayload) = await _client.SendRequestAsync(
                SurgewaveOpCode.Produce,
                payloadBuffer.AsMemory(0, writer.Position),
                cancellationToken);

            if (header.ErrorCode != SurgewaveErrorCode.None)
                throw new ProtocolException(SurgewaveOpCode.Produce, header.ErrorCode);

            return BinaryPrimitives.ReadInt64BigEndian(responsePayload.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payloadBuffer);
        }
    }

    /// <summary>
    /// Send a single message with string key/value.
    /// Zero-allocation path that encodes strings directly to pooled buffer.
    /// </summary>
    public async Task<long> SendAsync(string topic, int partition, string? key, string value, CancellationToken cancellationToken = default)
    {
        // Calculate sizes upfront to avoid intermediate allocations
        var keyByteCount = key != null ? Encoding.UTF8.GetByteCount(key) : 0;
        var valueByteCount = Encoding.UTF8.GetByteCount(value);
        var bufferSize = 256 + keyByteCount + valueByteCount;

        var payloadBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            var writer = new SurgewavePayloadWriter(payloadBuffer);
            writer.WriteString(topic);
            writer.WriteInt32(partition);
            writer.WriteInt32(1);

            if (key != null && keyByteCount > 0)
            {
                writer.WriteInt32(keyByteCount);
                // Encode directly to buffer - zero allocation
                Encoding.UTF8.GetBytes(key.AsSpan(), payloadBuffer.AsSpan(writer.Position, keyByteCount));
                writer.Advance(keyByteCount);
            }
            else
            {
                writer.WriteInt32(-1);
            }

            // Write value length and encode directly
            writer.WriteInt32(valueByteCount);
            Encoding.UTF8.GetBytes(value.AsSpan(), payloadBuffer.AsSpan(writer.Position, valueByteCount));
            writer.Advance(valueByteCount);

            // Leerer Native-Header-Block (Pflicht seit f609a7e — sonst
            // liest der Server position-shifted Garbage als Header-Count
            // und der Producer haengt im Ack-Warten).
            writer.WriteInt32(0);

            var (header, responsePayload) = await _client.SendRequestAsync(
                SurgewaveOpCode.Produce,
                payloadBuffer.AsMemory(0, writer.Position),
                cancellationToken);

            if (header.ErrorCode != SurgewaveErrorCode.None)
                throw new ProtocolException(SurgewaveOpCode.Produce, header.ErrorCode);

            return BinaryPrimitives.ReadInt64BigEndian(responsePayload.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payloadBuffer);
        }
    }

    /// <summary>
    /// Start building a message to send with fluent API.
    /// </summary>
    public SendBuilder Send(string topic) => new(_client, topic);

    /// <summary>
    /// Start building a typed message to send with serialization support.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> Send<TKey, TValue>(string topic) => new(_client, topic);

    /// <summary>
    /// Send a batch of messages for high throughput.
    /// </summary>
    public Task<long> SendBatchAsync(string topic, int partition, IReadOnlyList<(byte[]? Key, byte[] Value)> messages, CancellationToken cancellationToken = default)
    {
        var withHeaders = new List<(byte[]? Key, byte[] Value, IReadOnlyDictionary<string, byte[]>? Headers)>(messages.Count);
        foreach (var (k, v) in messages) withHeaders.Add((k, v, null));
        return SendBatchAsync(topic, partition, withHeaders, cancellationToken);
    }

    /// <summary>
    /// Send a batch of messages with per-message headers.
    /// </summary>
    public async Task<long> SendBatchAsync(string topic, int partition, IReadOnlyList<(byte[]? Key, byte[] Value, IReadOnlyDictionary<string, byte[]>? Headers)> messages, CancellationToken cancellationToken = default)
    {
        if (messages.Count == 0) return -1;

        var totalSize = 256;
        foreach (var (key, value, headers) in messages)
            totalSize += 4 + (key?.Length ?? 0)
                       + 4 + value.Length
                       + NativeMessageHeaderCodec.EncodedSize(headers);

        var payloadBuffer = ArrayPool<byte>.Shared.Rent(totalSize);
        try
        {
            var writer = new SurgewavePayloadWriter(payloadBuffer);
            writer.WriteString(topic);
            writer.WriteInt32(partition);
            writer.WriteInt32(messages.Count);

            foreach (var (key, value, headers) in messages)
            {
                if (key != null && key.Length > 0)
                {
                    writer.WriteInt32(key.Length);
                    writer.WriteRaw(key);
                }
                else
                {
                    writer.WriteInt32(-1);
                }
                writer.WriteBytes(value);
                var headerBytes = NativeMessageHeaderCodec.Encode(headers, payloadBuffer.AsSpan(writer.Position));
                writer.Advance(headerBytes);
            }

            var (header, responsePayload) = await _client.SendRequestAsync(
                SurgewaveOpCode.Produce,
                payloadBuffer.AsMemory(0, writer.Position),
                cancellationToken);

            if (header.ErrorCode != SurgewaveErrorCode.None)
                throw new ProtocolException(SurgewaveOpCode.Produce, header.ErrorCode);

            return BinaryPrimitives.ReadInt64BigEndian(responsePayload.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payloadBuffer);
        }
    }

    /// <summary>
    /// Send a batch of string messages.
    /// </summary>
    public Task<long> SendBatchAsync(string topic, int partition, IReadOnlyList<(string? Key, string Value)> messages, CancellationToken cancellationToken = default)
    {
        var byteMessages = new List<(byte[]? Key, byte[] Value)>(messages.Count);
        foreach (var (key, value) in messages)
        {
            byteMessages.Add((
                key != null ? Encoding.UTF8.GetBytes(key) : null,
                Encoding.UTF8.GetBytes(value)));
        }
        return SendBatchAsync(topic, partition, byteMessages, cancellationToken);
    }

    /// <summary>
    /// Receive messages from a topic partition.
    /// </summary>
    /// <param name="topic">The topic name</param>
    /// <param name="partition">The partition number</param>
    /// <param name="offset">The starting offset</param>
    /// <param name="maxBytes">Maximum bytes to fetch</param>
    /// <param name="maxWaitMs">Maximum time to wait for new data (long-polling). Use 0 for no wait.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task<ReceiveResult> ReceiveAsync(string topic, int partition, long offset, int maxBytes = 1024 * 1024, int maxWaitMs = 5000, CancellationToken cancellationToken = default)
    {
        var payloadBuffer = ArrayPool<byte>.Shared.Rent(256);
        try
        {
            var writer = new SurgewavePayloadWriter(payloadBuffer);
            writer.WriteString(topic);
            writer.WriteInt32(partition);
            writer.WriteInt64(offset);
            writer.WriteInt32(maxBytes);
            writer.WriteInt32(maxWaitMs);  // Long-polling wait time

            var (header, responsePayload) = await _client.SendRequestAsync(
                SurgewaveOpCode.Fetch,
                payloadBuffer.AsMemory(0, writer.Position),
                cancellationToken);

            if (header.ErrorCode != SurgewaveErrorCode.None)
                throw new ProtocolException(SurgewaveOpCode.Fetch, header.ErrorCode);

            var reader = new SurgewavePayloadReader(responsePayload.Span);
            var highWatermark = reader.ReadInt64();
            var messageCount = reader.ReadInt32();

            var messages = new List<ReceivedMessage>(messageCount);
            for (int i = 0; i < messageCount; i++)
            {
                var msgOffset = reader.ReadInt64();
                var timestamp = reader.ReadInt64();

                var keyLength = reader.ReadInt32();
                byte[]? msgKey = keyLength >= 0 ? reader.ReadRaw(keyLength).ToArray() : null;

                var valueLength = reader.ReadInt32();
                // Storage encodes empty/null values as length=-1 (matches the
                // key path above). Treat anything below zero as an empty body
                // — `ReadRaw(-1)` would otherwise crash with
                // ArgumentOutOfRangeException, which broke every consumer that
                // saw a tombstone-style record (e.g. Akka snapshot deletes).
                var valueBytes = valueLength > 0
                    ? reader.ReadRaw(valueLength).ToArray()
                    : Array.Empty<byte>();

                // Per-message native header block.
                var headers = NativeMessageHeaderCodec.Decode(responsePayload.Span[reader.Position..], out var headerBytes);
                reader.Skip(headerBytes);

                messages.Add(new ReceivedMessage(msgOffset, timestamp, msgKey, valueBytes, headers));
            }

            return new ReceiveResult(highWatermark, messages);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payloadBuffer);
        }
    }

    /// <summary>
    /// Start building a receive request with fluent API.
    /// Use Receive().Stream() for continuous consumption.
    /// </summary>
    public ReceiveBuilder Receive(string topic) => new(_client, topic);

    /// <summary>
    /// Create a bidirectional channel for both sending and receiving.
    /// </summary>
    public TopicChannel Channel(string topic, int partition = 0) => new(this, topic, partition);

    /// <summary>
    /// Get the latest offset for a partition.
    /// </summary>
    public Task<long> GetLatestOffsetAsync(string topic, int partition, CancellationToken cancellationToken = default)
        => GetOffsetAsync(topic, partition, -1, cancellationToken);

    /// <summary>
    /// Get the earliest offset for a partition.
    /// </summary>
    public Task<long> GetEarliestOffsetAsync(string topic, int partition, CancellationToken cancellationToken = default)
        => GetOffsetAsync(topic, partition, -2, cancellationToken);

    /// <summary>
    /// Get offset for a specific timestamp.
    /// </summary>
    public Task<long> GetOffsetForTimestampAsync(string topic, int partition, DateTimeOffset timestamp, CancellationToken cancellationToken = default)
        => GetOffsetAsync(topic, partition, timestamp.ToUnixTimeMilliseconds(), cancellationToken);

    private async Task<long> GetOffsetAsync(string topic, int partition, long timestamp, CancellationToken cancellationToken)
    {
        var payloadBuffer = ArrayPool<byte>.Shared.Rent(256);
        try
        {
            var writer = new SurgewavePayloadWriter(payloadBuffer);
            writer.WriteString(topic);
            writer.WriteInt32(partition);
            writer.WriteInt64(timestamp);

            var (header, responsePayload) = await _client.SendRequestAsync(
                SurgewaveOpCode.ListOffsets,
                payloadBuffer.AsMemory(0, writer.Position),
                cancellationToken);

            if (header.ErrorCode != SurgewaveErrorCode.None)
                throw new ProtocolException(SurgewaveOpCode.ListOffsets, header.ErrorCode);

            return BinaryPrimitives.ReadInt64BigEndian(responsePayload.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payloadBuffer);
        }
    }

    /// <summary>
    /// Open a server-push streaming subscription on a topic.
    /// The broker will push record batches to the client as new data arrives.
    /// Dispose the returned consumer to close the subscription.
    /// </summary>
    /// <param name="topic">Topic to subscribe to.</param>
    /// <param name="partitions">Partitions to subscribe to. Empty means all partitions.</param>
    /// <param name="startOffset">Starting offset. Use -1 for latest, -2 for earliest.</param>
    /// <param name="maxBytesPerPush">Maximum bytes the broker may push per batch.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public Task<SurgewaveStreamingConsumer> SubscribeAsync(
        string topic,
        int[]? partitions = null,
        long startOffset = -1,
        int maxBytesPerPush = 1024 * 1024,
        CancellationToken cancellationToken = default)
        => SurgewaveStreamingConsumer.SubscribeAsync(
            _client,
            topic,
            partitions ?? [],
            startOffset,
            maxBytesPerPush,
            cancellationToken);

    /// <summary>
    /// Ping the broker.
    /// </summary>
    public async Task<long> PingAsync(CancellationToken cancellationToken = default)
    {
        var (_, responsePayload) = await _client.SendRequestAsync(
            SurgewaveOpCode.Ping,
            ReadOnlyMemory<byte>.Empty,
            cancellationToken);

        return BinaryPrimitives.ReadInt64BigEndian(responsePayload.Span);
    }

    /// <summary>
    /// Nack a message, indicating it could not be processed.
    /// The broker will schedule re-delivery with backoff or route to DLQ after max retries.
    /// </summary>
    /// <returns>A result indicating whether the message was routed to DLQ and the current retry count.</returns>
    public async Task<(bool RoutedToDlq, int RetryCount)> NackAsync(
        string topic,
        int partition,
        long offset,
        CancellationToken cancellationToken = default)
    {
        var payloadBuffer = ArrayPool<byte>.Shared.Rent(256);
        try
        {
            var writer = new SurgewavePayloadWriter(payloadBuffer);
            writer.WriteString(topic);
            writer.WriteInt32(partition);
            writer.WriteInt64(offset);

            var (header, responsePayload) = await _client.SendRequestAsync(
                SurgewaveOpCode.Nack,
                payloadBuffer.AsMemory(0, writer.Position),
                cancellationToken);

            if (header.ErrorCode != SurgewaveErrorCode.None)
                throw new ProtocolException(SurgewaveOpCode.Nack, header.ErrorCode);

            var responseSpan = responsePayload.Span;
            var routedToDlq = responseSpan[0] != 0;
            var retryCount = BinaryPrimitives.ReadInt32BigEndian(responseSpan.Slice(1, 4));

            return (routedToDlq, retryCount);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payloadBuffer);
        }
    }
}
