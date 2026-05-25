using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ShareGroups;

// ──────────────────────────────────────────────────────────────────────
// Describe Share Group Offsets
// ──────────────────────────────────────────────────────────────────────

/// <summary>
/// Wire format for DescribeShareGroupOffsets request.
///
/// Wire layout:
///   groupId          string (int16 length prefix + UTF-8)
///   topicCount       int32
///   for each topic:
///     topicName      string (int16 length prefix + UTF-8)
///     partitionCount int32
///     for each partition:
///       partitionIndex int32
/// </summary>
public readonly record struct DescribeShareGroupOffsetsRequestPayload
{
    public string GroupId { get; init; }
    public ShareGroupOffsetsTopic[] Topics { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static DescribeShareGroupOffsetsRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var groupId = reader.ReadString() ?? "";
        var topicCount = reader.ReadInt32();
        var topics = new ShareGroupOffsetsTopic[topicCount];

        for (var t = 0; t < topicCount; t++)
        {
            var topicName = reader.ReadString() ?? "";
            var partitionCount = reader.ReadInt32();
            var partitions = new int[partitionCount];
            for (var p = 0; p < partitionCount; p++)
                partitions[p] = reader.ReadInt32();
            topics[t] = new ShareGroupOffsetsTopic(topicName, partitions);
        }

        return new DescribeShareGroupOffsetsRequestPayload
        {
            GroupId = groupId,
            Topics = topics
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteInt32(Topics.Length);

        foreach (var topic in Topics)
        {
            writer.WriteString(topic.TopicName);
            writer.WriteInt32(topic.Partitions.Length);
            foreach (var p in topic.Partitions)
                writer.WriteInt32(p);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteInt32(Topics.Length);

        foreach (var topic in Topics)
        {
            writer.WriteString(topic.TopicName);
            writer.WriteInt32(topic.Partitions.Length);
            foreach (var p in topic.Partitions)
                writer.WriteInt32(p);
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 2 + Encoding.UTF8.GetByteCount(GroupId ?? "") + 4;

        foreach (var topic in Topics)
        {
            size += 2 + Encoding.UTF8.GetByteCount(topic.TopicName ?? "");
            size += 4 + topic.Partitions.Length * 4;
        }

        return size;
    }
}

/// <summary>
/// Wire format for DescribeShareGroupOffsets response.
///
/// Wire layout:
///   groupCount             int32
///   for each group:
///     groupId              string (int16 length prefix + UTF-8)
///     topicCount           int32
///     for each topic:
///       topicName          string (int16 length prefix + UTF-8)
///       partitionCount     int32
///       for each partition:
///         partitionIndex   int32
///         startOffset      int64
///         leaderEpoch      int32
///         errorCode        int16
/// </summary>
public readonly record struct DescribeShareGroupOffsetsResponsePayload
{
    public DescribeShareGroupOffsetsGroup[] Groups { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static DescribeShareGroupOffsetsResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var groupCount = reader.ReadInt32();
        var groups = new DescribeShareGroupOffsetsGroup[groupCount];

        for (var g = 0; g < groupCount; g++)
        {
            var groupId = reader.ReadString() ?? "";
            var topicCount = reader.ReadInt32();
            var topics = new DescribeShareGroupOffsetsTopicResponse[topicCount];

            for (var t = 0; t < topicCount; t++)
            {
                var topicName = reader.ReadString() ?? "";
                var partitionCount = reader.ReadInt32();
                var partitions = new ShareGroupOffsetPartitionResponse[partitionCount];

                for (var p = 0; p < partitionCount; p++)
                {
                    var partitionIndex = reader.ReadInt32();
                    var startOffset = reader.ReadInt64();
                    var leaderEpoch = reader.ReadInt32();
                    var errorCode = reader.ReadInt16();
                    partitions[p] = new ShareGroupOffsetPartitionResponse
                    {
                        PartitionIndex = partitionIndex,
                        StartOffset = startOffset,
                        LeaderEpoch = leaderEpoch,
                        ErrorCode = errorCode
                    };
                }

                topics[t] = new DescribeShareGroupOffsetsTopicResponse(topicName, partitions);
            }

            groups[g] = new DescribeShareGroupOffsetsGroup(groupId, topics);
        }

        return new DescribeShareGroupOffsetsResponsePayload { Groups = groups };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Groups.Length);

        foreach (var group in Groups)
        {
            writer.WriteString(group.GroupId);
            writer.WriteInt32(group.Topics.Length);

            foreach (var topic in group.Topics)
            {
                writer.WriteString(topic.TopicName);
                writer.WriteInt32(topic.Partitions.Length);

                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt64(partition.StartOffset);
                    writer.WriteInt32(partition.LeaderEpoch);
                    writer.WriteInt16(partition.ErrorCode);
                }
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Groups.Length);

        foreach (var group in Groups)
        {
            writer.WriteString(group.GroupId);
            writer.WriteInt32(group.Topics.Length);

            foreach (var topic in group.Topics)
            {
                writer.WriteString(topic.TopicName);
                writer.WriteInt32(topic.Partitions.Length);

                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt64(partition.StartOffset);
                    writer.WriteInt32(partition.LeaderEpoch);
                    writer.WriteInt16(partition.ErrorCode);
                }
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 4; // Groups count

        foreach (var group in Groups)
        {
            size += 2 + Encoding.UTF8.GetByteCount(group.GroupId ?? "") + 4;

            foreach (var topic in group.Topics)
            {
                size += 2 + Encoding.UTF8.GetByteCount(topic.TopicName ?? "") + 4;
                size += topic.Partitions.Length * (4 + 8 + 4 + 2);
            }
        }

        return size;
    }
}

// ──────────────────────────────────────────────────────────────────────
// Alter Share Group Offsets
// ──────────────────────────────────────────────────────────────────────

/// <summary>
/// Wire format for AlterShareGroupOffsets request.
///
/// Wire layout:
///   groupId          string (int16 length prefix + UTF-8)
///   topicCount       int32
///   for each topic:
///     topicName      string (int16 length prefix + UTF-8)
///     partitionCount int32
///     for each partition:
///       partitionIndex int32
///       startOffset    int64
/// </summary>
public readonly record struct AlterShareGroupOffsetsRequestPayload
{
    public string GroupId { get; init; }
    public AlterShareGroupOffsetsTopic[] Topics { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static AlterShareGroupOffsetsRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var groupId = reader.ReadString() ?? "";
        var topicCount = reader.ReadInt32();
        var topics = new AlterShareGroupOffsetsTopic[topicCount];

        for (var t = 0; t < topicCount; t++)
        {
            var topicName = reader.ReadString() ?? "";
            var partitionCount = reader.ReadInt32();
            var partitions = new AlterShareGroupOffsetsPartition[partitionCount];

            for (var p = 0; p < partitionCount; p++)
            {
                var partitionIndex = reader.ReadInt32();
                var startOffset = reader.ReadInt64();
                partitions[p] = new AlterShareGroupOffsetsPartition(partitionIndex, startOffset);
            }

            topics[t] = new AlterShareGroupOffsetsTopic(topicName, partitions);
        }

        return new AlterShareGroupOffsetsRequestPayload
        {
            GroupId = groupId,
            Topics = topics
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteInt32(Topics.Length);

        foreach (var topic in Topics)
        {
            writer.WriteString(topic.TopicName);
            writer.WriteInt32(topic.Partitions.Length);

            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt64(partition.StartOffset);
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteInt32(Topics.Length);

        foreach (var topic in Topics)
        {
            writer.WriteString(topic.TopicName);
            writer.WriteInt32(topic.Partitions.Length);

            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt64(partition.StartOffset);
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 2 + Encoding.UTF8.GetByteCount(GroupId ?? "") + 4;

        foreach (var topic in Topics)
        {
            size += 2 + Encoding.UTF8.GetByteCount(topic.TopicName ?? "");
            size += 4 + topic.Partitions.Length * (4 + 8);
        }

        return size;
    }
}

/// <summary>
/// Wire format for AlterShareGroupOffsets response.
/// Same structure as DescribeShareGroupOffsetsResponse.
/// </summary>
public readonly record struct AlterShareGroupOffsetsResponsePayload
{
    public DescribeShareGroupOffsetsGroup[] Groups { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static AlterShareGroupOffsetsResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var inner = DescribeShareGroupOffsetsResponsePayload.Read(ref reader);
        return new AlterShareGroupOffsetsResponsePayload { Groups = inner.Groups };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Groups.Length);

        foreach (var group in Groups)
        {
            writer.WriteString(group.GroupId);
            writer.WriteInt32(group.Topics.Length);

            foreach (var topic in group.Topics)
            {
                writer.WriteString(topic.TopicName);
                writer.WriteInt32(topic.Partitions.Length);

                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt64(partition.StartOffset);
                    writer.WriteInt32(partition.LeaderEpoch);
                    writer.WriteInt16(partition.ErrorCode);
                }
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Groups.Length);

        foreach (var group in Groups)
        {
            writer.WriteString(group.GroupId);
            writer.WriteInt32(group.Topics.Length);

            foreach (var topic in group.Topics)
            {
                writer.WriteString(topic.TopicName);
                writer.WriteInt32(topic.Partitions.Length);

                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt64(partition.StartOffset);
                    writer.WriteInt32(partition.LeaderEpoch);
                    writer.WriteInt16(partition.ErrorCode);
                }
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 4;

        foreach (var group in Groups)
        {
            size += 2 + Encoding.UTF8.GetByteCount(group.GroupId ?? "") + 4;

            foreach (var topic in group.Topics)
            {
                size += 2 + Encoding.UTF8.GetByteCount(topic.TopicName ?? "") + 4;
                size += topic.Partitions.Length * (4 + 8 + 4 + 2);
            }
        }

        return size;
    }
}

// ──────────────────────────────────────────────────────────────────────
// Delete Share Group Offsets
// ──────────────────────────────────────────────────────────────────────

/// <summary>
/// Wire format for DeleteShareGroupOffsets request.
///
/// Wire layout:
///   groupId          string (int16 length prefix + UTF-8)
///   topicCount       int32
///   for each topic:
///     topicName      string (int16 length prefix + UTF-8)
/// </summary>
public readonly record struct DeleteShareGroupOffsetsRequestPayload
{
    public string GroupId { get; init; }
    public string[] Topics { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static DeleteShareGroupOffsetsRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var groupId = reader.ReadString() ?? "";
        var topicCount = reader.ReadInt32();
        var topics = new string[topicCount];

        for (var t = 0; t < topicCount; t++)
            topics[t] = reader.ReadString() ?? "";

        return new DeleteShareGroupOffsetsRequestPayload
        {
            GroupId = groupId,
            Topics = topics
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteInt32(Topics.Length);

        foreach (var topic in Topics)
            writer.WriteString(topic);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteInt32(Topics.Length);

        foreach (var topic in Topics)
            writer.WriteString(topic);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 2 + Encoding.UTF8.GetByteCount(GroupId ?? "") + 4;

        foreach (var topic in Topics)
            size += 2 + Encoding.UTF8.GetByteCount(topic ?? "");

        return size;
    }
}

/// <summary>
/// Wire format for DeleteShareGroupOffsets response.
///
/// Wire layout:
///   resultCount            int32
///   for each result:
///     topicName            string (int16 length prefix + UTF-8)
///     topicErrorCode       int16
///     partitionCount       int32
///     for each partition:
///       partitionIndex     int32
///       errorCode          int16
/// </summary>
public readonly record struct DeleteShareGroupOffsetsResponsePayload
{
    public DeleteShareGroupOffsetsResult[] Results { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static DeleteShareGroupOffsetsResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var resultCount = reader.ReadInt32();
        var results = new DeleteShareGroupOffsetsResult[resultCount];

        for (var r = 0; r < resultCount; r++)
        {
            var topicName = reader.ReadString() ?? "";
            var topicErrorCode = reader.ReadInt16();
            var partitionCount = reader.ReadInt32();
            var partitions = new DeleteShareGroupOffsetsPartitionResult[partitionCount];

            for (var p = 0; p < partitionCount; p++)
            {
                var partitionIndex = reader.ReadInt32();
                var errorCode = reader.ReadInt16();
                partitions[p] = new DeleteShareGroupOffsetsPartitionResult(partitionIndex, errorCode);
            }

            results[r] = new DeleteShareGroupOffsetsResult(topicName, topicErrorCode, partitions);
        }

        return new DeleteShareGroupOffsetsResponsePayload { Results = results };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Results.Length);

        foreach (var result in Results)
        {
            writer.WriteString(result.TopicName);
            writer.WriteInt16(result.ErrorCode);
            writer.WriteInt32(result.Partitions.Length);

            foreach (var partition in result.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16(partition.ErrorCode);
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Results.Length);

        foreach (var result in Results)
        {
            writer.WriteString(result.TopicName);
            writer.WriteInt16(result.ErrorCode);
            writer.WriteInt32(result.Partitions.Length);

            foreach (var partition in result.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16(partition.ErrorCode);
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 4; // Results count

        foreach (var result in Results)
        {
            size += 2 + Encoding.UTF8.GetByteCount(result.TopicName ?? "");
            size += 2 + 4; // TopicErrorCode + Partitions count
            size += result.Partitions.Length * (4 + 2);
        }

        return size;
    }
}

// ──────────────────────────────────────────────────────────────────────
// Shared records for ShareGroupOffsets payloads
// ──────────────────────────────────────────────────────────────────────

/// <summary>
/// A topic with partition indices for describe share group offsets request.
/// </summary>
public readonly record struct ShareGroupOffsetsTopic(string TopicName, int[] Partitions);

/// <summary>
/// A group with topics in the describe/alter share group offsets response.
/// </summary>
public readonly record struct DescribeShareGroupOffsetsGroup(string GroupId, DescribeShareGroupOffsetsTopicResponse[] Topics);

/// <summary>
/// A topic with partition responses in the describe/alter share group offsets response.
/// </summary>
public readonly record struct DescribeShareGroupOffsetsTopicResponse(string TopicName, ShareGroupOffsetPartitionResponse[] Partitions);

/// <summary>
/// A partition response within describe/alter share group offsets.
/// </summary>
public readonly record struct ShareGroupOffsetPartitionResponse
{
    public int PartitionIndex { get; init; }
    public long StartOffset { get; init; }
    public int LeaderEpoch { get; init; }
    public short ErrorCode { get; init; }
}

/// <summary>
/// A topic with partitions for alter share group offsets request.
/// </summary>
public readonly record struct AlterShareGroupOffsetsTopic(string TopicName, AlterShareGroupOffsetsPartition[] Partitions);

/// <summary>
/// A partition within an alter share group offsets request.
/// </summary>
public readonly record struct AlterShareGroupOffsetsPartition(int PartitionIndex, long StartOffset);

/// <summary>
/// A result for a topic in the delete share group offsets response.
/// </summary>
public readonly record struct DeleteShareGroupOffsetsResult(string TopicName, short ErrorCode, DeleteShareGroupOffsetsPartitionResult[] Partitions);

/// <summary>
/// A partition result within a delete share group offsets response.
/// </summary>
public readonly record struct DeleteShareGroupOffsetsPartitionResult(int PartitionIndex, short ErrorCode);
