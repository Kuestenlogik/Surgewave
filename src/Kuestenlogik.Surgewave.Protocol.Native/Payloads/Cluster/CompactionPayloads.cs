namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

/// <summary>
/// Wire format for TriggerLogCompaction response.
/// </summary>
public readonly record struct CompactionResultPayload
{
    public bool Success { get; init; }
    public long RecordsRemoved { get; init; }
    public long BytesRemoved { get; init; }
    public int SegmentsCompacted { get; init; }

    public static CompactionResultPayload Read(ref SurgewavePayloadReader reader)
    {
        return new CompactionResultPayload
        {
            Success = reader.ReadUInt8() != 0,
            RecordsRemoved = reader.ReadInt64(),
            BytesRemoved = reader.ReadInt64(),
            SegmentsCompacted = reader.ReadInt32()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt8(Success ? (byte)1 : (byte)0);
        writer.WriteInt64(RecordsRemoved);
        writer.WriteInt64(BytesRemoved);
        writer.WriteInt32(SegmentsCompacted);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt8(Success ? (byte)1 : (byte)0);
        writer.WriteInt64(RecordsRemoved);
        writer.WriteInt64(BytesRemoved);
        writer.WriteInt32(SegmentsCompacted);
    }

    public int EstimateSize() => 1 + 8 + 8 + 4; // Success + RecordsRemoved + BytesRemoved + SegmentsCompacted
}

/// <summary>
/// Wire format for topic compaction status in GetCompactionStatus response.
/// </summary>
public readonly record struct TopicCompactionStatusPayload
{
    public string Topic { get; init; }
    public int PartitionCount { get; init; }
    public string CleanupPolicy { get; init; }
    public int SegmentCount { get; init; }
    public long TotalBytes { get; init; }

    public static TopicCompactionStatusPayload Read(ref SurgewavePayloadReader reader)
    {
        return new TopicCompactionStatusPayload
        {
            Topic = reader.ReadString() ?? string.Empty,
            PartitionCount = reader.ReadInt32(),
            CleanupPolicy = reader.ReadString() ?? string.Empty,
            SegmentCount = reader.ReadInt32(),
            TotalBytes = reader.ReadInt64()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(PartitionCount);
        writer.WriteString(CleanupPolicy);
        writer.WriteInt32(SegmentCount);
        writer.WriteInt64(TotalBytes);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(PartitionCount);
        writer.WriteString(CleanupPolicy);
        writer.WriteInt32(SegmentCount);
        writer.WriteInt64(TotalBytes);
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Topic ?? "") +         // Topic
        4 +                                                                // PartitionCount
        2 + System.Text.Encoding.UTF8.GetByteCount(CleanupPolicy ?? "") + // CleanupPolicy
        4 +                                                                // SegmentCount
        8;                                                                 // TotalBytes
}

/// <summary>
/// Wire format for GetCompactionStatus response.
/// </summary>
public readonly record struct CompactionStatusPayload
{
    public IReadOnlyList<TopicCompactionStatusPayload> Topics { get; init; }

    public static CompactionStatusPayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var topics = new TopicCompactionStatusPayload[count];

        for (int i = 0; i < count; i++)
        {
            topics[i] = TopicCompactionStatusPayload.Read(ref reader);
        }

        return new CompactionStatusPayload { Topics = topics };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Topics.Count);
        foreach (var topic in Topics)
        {
            topic.Write(ref writer);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Topics.Count);
        foreach (var topic in Topics)
        {
            topic.WriteTo(writer);
        }
    }

    public int EstimateSize() =>
        4 + Topics.Sum(t => t.EstimateSize()); // Count + all topics
}
