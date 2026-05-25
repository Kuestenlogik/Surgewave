namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for GetGroupLag response.
/// </summary>
public readonly record struct GetGroupLagResponsePayload
{
    public ushort ErrorCode { get; init; }
    public string GroupId { get; init; }
    public string State { get; init; }
    public long TotalLag { get; init; }
    public int PartitionCount { get; init; }
    public int MemberCount { get; init; }
    public TopicLagPayload[] Topics { get; init; }

    public static GetGroupLagResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var errorCode = reader.ReadUInt16();
        var groupId = reader.ReadString() ?? "";
        var state = reader.ReadString() ?? "";
        var totalLag = reader.ReadInt64();
        var partitionCount = reader.ReadInt32();
        var memberCount = reader.ReadInt32();
        var topicCount = reader.ReadInt16();

        var topics = new TopicLagPayload[topicCount];
        for (int i = 0; i < topicCount; i++)
        {
            var topic = reader.ReadString() ?? "";
            var topicLag = reader.ReadInt64();
            var partCount = reader.ReadInt16();

            var partitions = new PartitionLagPayload[partCount];
            for (int j = 0; j < partCount; j++)
            {
                partitions[j] = new PartitionLagPayload
                {
                    Partition = reader.ReadInt32(),
                    CommittedOffset = reader.ReadInt64(),
                    HighWatermark = reader.ReadInt64(),
                    Lag = reader.ReadInt64(),
                    LogStartOffset = reader.ReadInt64()
                };
            }

            topics[i] = new TopicLagPayload
            {
                Topic = topic,
                TotalLag = topicLag,
                Partitions = partitions
            };
        }

        return new GetGroupLagResponsePayload
        {
            ErrorCode = errorCode,
            GroupId = groupId,
            State = state,
            TotalLag = totalLag,
            PartitionCount = partitionCount,
            MemberCount = memberCount,
            Topics = topics
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteString(GroupId);
        writer.WriteString(State);
        writer.WriteInt64(TotalLag);
        writer.WriteInt32(PartitionCount);
        writer.WriteInt32(MemberCount);
        writer.WriteInt16((short)(Topics?.Length ?? 0));

        if (Topics != null)
        {
            foreach (var topic in Topics)
            {
                writer.WriteString(topic.Topic);
                writer.WriteInt64(topic.TotalLag);
                writer.WriteInt16((short)(topic.Partitions?.Length ?? 0));

                if (topic.Partitions != null)
                {
                    foreach (var partition in topic.Partitions)
                    {
                        writer.WriteInt32(partition.Partition);
                        writer.WriteInt64(partition.CommittedOffset);
                        writer.WriteInt64(partition.HighWatermark);
                        writer.WriteInt64(partition.Lag);
                        writer.WriteInt64(partition.LogStartOffset);
                    }
                }
            }
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteString(GroupId);
        writer.WriteString(State);
        writer.WriteInt64(TotalLag);
        writer.WriteInt32(PartitionCount);
        writer.WriteInt32(MemberCount);
        writer.WriteInt16((short)(Topics?.Length ?? 0));

        if (Topics != null)
        {
            foreach (var topic in Topics)
            {
                writer.WriteString(topic.Topic);
                writer.WriteInt64(topic.TotalLag);
                writer.WriteInt16((short)(topic.Partitions?.Length ?? 0));

                if (topic.Partitions != null)
                {
                    foreach (var partition in topic.Partitions)
                    {
                        writer.WriteInt32(partition.Partition);
                        writer.WriteInt64(partition.CommittedOffset);
                        writer.WriteInt64(partition.HighWatermark);
                        writer.WriteInt64(partition.Lag);
                        writer.WriteInt64(partition.LogStartOffset);
                    }
                }
            }
        }
    }

    public int EstimateSize()
    {
        var size = 2 + // ErrorCode
            2 + System.Text.Encoding.UTF8.GetByteCount(GroupId ?? "") +
            2 + System.Text.Encoding.UTF8.GetByteCount(State ?? "") +
            8 + // TotalLag
            4 + // PartitionCount
            4 + // MemberCount
            2;  // Topic count

        if (Topics != null)
        {
            foreach (var topic in Topics)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(topic.Topic ?? "");
                size += 8; // TotalLag
                size += 2; // Partition count
                size += (topic.Partitions?.Length ?? 0) * (4 + 8 + 8 + 8 + 8); // Per partition
            }
        }

        return size;
    }
}

/// <summary>
/// Topic lag info in GetGroupLag response.
/// </summary>
public readonly record struct TopicLagPayload
{
    public string Topic { get; init; }
    public long TotalLag { get; init; }
    public PartitionLagPayload[] Partitions { get; init; }
}

/// <summary>
/// Partition lag info in GetGroupLag response.
/// </summary>
public readonly record struct PartitionLagPayload
{
    public int Partition { get; init; }
    public long CommittedOffset { get; init; }
    public long HighWatermark { get; init; }
    public long Lag { get; init; }
    public long LogStartOffset { get; init; }
}
