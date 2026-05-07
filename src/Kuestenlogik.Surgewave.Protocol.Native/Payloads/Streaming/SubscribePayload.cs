using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Streaming;

/// <summary>
/// Wire format for Subscribe request.
/// Sent by clients to open a server-push subscription on one or more partitions.
///
/// Wire layout (matches NativeStreamingHandler.HandleSubscribeAsync):
///   subscriptionId  string (int16 length prefix + UTF-8)
///   topic           string (int16 length prefix + UTF-8)
///   partitionCount  int32
///   for each partition:
///     partition     int32
///     startOffset   int64   (−1=latest, −2=earliest)
///   maxBytesPerPush int32
/// </summary>
public readonly record struct SubscribePayload
{
    /// <summary>
    /// Client-generated subscription identifier (UUID string).
    /// </summary>
    public string SubscriptionId { get; init; }

    /// <summary>
    /// Topic to subscribe to.
    /// </summary>
    public string Topic { get; init; }

    /// <summary>
    /// Partitions and their starting offsets.
    /// </summary>
    public PartitionOffset[] Partitions { get; init; }

    /// <summary>
    /// Maximum bytes the broker may push per message batch.
    /// </summary>
    public int MaxBytesPerPush { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static SubscribePayload Read(ref SurgewavePayloadReader reader)
    {
        var subscriptionId = reader.ReadString() ?? "";
        var topic = reader.ReadString() ?? "";

        var partitionCount = reader.ReadInt32();
        var partitions = new PartitionOffset[partitionCount];
        for (var i = 0; i < partitionCount; i++)
        {
            var partition = reader.ReadInt32();
            var startOffset = reader.ReadInt64();
            partitions[i] = new PartitionOffset(partition, startOffset);
        }

        var maxBytesPerPush = reader.Remaining >= 4 ? reader.ReadInt32() : 1024 * 1024;

        return new SubscribePayload
        {
            SubscriptionId = subscriptionId,
            Topic = topic,
            Partitions = partitions,
            MaxBytesPerPush = maxBytesPerPush
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(SubscriptionId);
        writer.WriteString(Topic);
        writer.WriteInt32(Partitions.Length);
        foreach (var p in Partitions)
        {
            writer.WriteInt32(p.Partition);
            writer.WriteInt64(p.StartOffset);
        }
        writer.WriteInt32(MaxBytesPerPush);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(SubscriptionId);
        writer.WriteString(Topic);
        writer.WriteInt32(Partitions.Length);
        foreach (var p in Partitions)
        {
            writer.WriteInt32(p.Partition);
            writer.WriteInt64(p.StartOffset);
        }
        writer.WriteInt32(MaxBytesPerPush);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + Encoding.UTF8.GetByteCount(SubscriptionId ?? "") +
        2 + Encoding.UTF8.GetByteCount(Topic ?? "") +
        4 + // Partition count
        Partitions.Length * (4 + 8) + // Per partition: partition(4) + startOffset(8)
        4; // MaxBytesPerPush
}

/// <summary>
/// A partition number paired with a starting offset for a Subscribe request.
/// </summary>
public readonly record struct PartitionOffset(int Partition, long StartOffset);
