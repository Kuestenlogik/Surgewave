namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Streaming;

/// <summary>
/// Wire format for a server-pushed stream record batch (FetchResponse with RequestId=0).
/// The broker sends this unsolicited to deliver messages to an active subscriber.
///
/// Wire layout (matches NativeStreamingHandler.PushDelegate):
///   subscriptionId  string (int16 length prefix + UTF-8)
///   partition       int32
///   highWatermark   int64
///   messageCount    int32
///   for each message (same layout as FetchResponse):
///     offset        int64
///     timestamp     int64
///     keyLength     int32   (−1 = null key)
///     key           bytes   (keyLength bytes, only if keyLength >= 0)
///     valueLength   int32
///     value         bytes   (valueLength bytes)
/// </summary>
public readonly record struct StreamRecordPayload
{
    /// <summary>
    /// Subscription identifier this push belongs to.
    /// </summary>
    public string SubscriptionId { get; init; }

    /// <summary>
    /// Partition of the pushed messages.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// Latest committed offset on the partition (high-water mark).
    /// </summary>
    public long HighWatermark { get; init; }

    /// <summary>
    /// Messages pushed in this batch.
    /// </summary>
    public StreamMessage[] Messages { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static StreamRecordPayload Read(ref SurgewavePayloadReader reader)
    {
        var subscriptionId = reader.ReadString() ?? "";
        var partition = reader.ReadInt32();
        var highWatermark = reader.ReadInt64();

        var messageCount = reader.ReadInt32();
        var messages = new StreamMessage[messageCount];
        for (var i = 0; i < messageCount; i++)
        {
            var offset = reader.ReadInt64();
            var timestamp = reader.ReadInt64();

            var keyLength = reader.ReadInt32();
            byte[]? key = keyLength >= 0 ? reader.ReadRaw(keyLength).ToArray() : null;

            var valueLength = reader.ReadInt32();
            var value = reader.ReadRaw(valueLength).ToArray();

            messages[i] = new StreamMessage(offset, timestamp, key, value);
        }

        return new StreamRecordPayload
        {
            SubscriptionId = subscriptionId,
            Partition = partition,
            HighWatermark = highWatermark,
            Messages = messages
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(SubscriptionId);
        writer.WriteInt32(Partition);
        writer.WriteInt64(HighWatermark);
        writer.WriteInt32(Messages.Length);
        foreach (var msg in Messages)
        {
            writer.WriteInt64(msg.Offset);
            writer.WriteInt64(msg.Timestamp);
            if (msg.Key != null)
            {
                writer.WriteInt32(msg.Key.Length);
                writer.WriteRaw(msg.Key);
            }
            else
            {
                writer.WriteInt32(-1);
            }
            writer.WriteInt32(msg.Value.Length);
            writer.WriteRaw(msg.Value);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(SubscriptionId);
        writer.WriteInt32(Partition);
        writer.WriteInt64(HighWatermark);
        writer.WriteInt32(Messages.Length);
        foreach (var msg in Messages)
        {
            writer.WriteInt64(msg.Offset);
            writer.WriteInt64(msg.Timestamp);
            if (msg.Key != null)
            {
                writer.WriteInt32(msg.Key.Length);
                writer.WriteBytes(msg.Key);
            }
            else
            {
                writer.WriteInt32(-1);
            }
            writer.WriteInt32(msg.Value.Length);
            writer.WriteBytes(msg.Value);
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size =
            2 + System.Text.Encoding.UTF8.GetByteCount(SubscriptionId ?? "") +
            4 + // Partition
            8 + // HighWatermark
            4;  // Message count

        foreach (var msg in Messages)
        {
            size +=
                8 + // Offset
                8 + // Timestamp
                4 + (msg.Key?.Length ?? 0) + // Key length prefix + key bytes
                4 + msg.Value.Length;         // Value length prefix + value bytes
        }

        return size;
    }
}

/// <summary>
/// A single message within a streamed record batch.
/// </summary>
public readonly record struct StreamMessage(long Offset, long Timestamp, byte[]? Key, byte[] Value);
