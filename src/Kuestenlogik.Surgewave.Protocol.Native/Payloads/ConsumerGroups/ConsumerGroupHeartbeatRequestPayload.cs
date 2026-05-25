using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for Consumer Group Heartbeat request (KIP-848).
/// The new consumer group protocol replaces JoinGroup/SyncGroup with a single heartbeat RPC.
///
/// Wire layout:
///   GroupId                  string (int16 length prefix + UTF-8)
///   MemberId                 string (int16 length prefix + UTF-8)
///   MemberEpoch              int32
///   InstanceId               nullable string (bool prefix + string)
///   RackId                   nullable string (bool prefix + string)
///   RebalanceTimeoutMs       int32
///   SubscribedTopicNames     nullable string array (bool prefix + int32 count + strings)
///   ServerAssignor           nullable string (bool prefix + string)
///   TopicPartitions          TopicPartitionAssignment[] (int32 count + entries)
/// </summary>
public readonly record struct ConsumerGroupHeartbeatRequestPayload
{
    public string GroupId { get; init; }
    public string MemberId { get; init; }
    public int MemberEpoch { get; init; }
    public string? InstanceId { get; init; }
    public string? RackId { get; init; }
    public int RebalanceTimeoutMs { get; init; }
    public string[]? SubscribedTopicNames { get; init; }
    public string? ServerAssignor { get; init; }
    public TopicPartitionAssignment[] TopicPartitions { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static ConsumerGroupHeartbeatRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var groupId = reader.ReadString() ?? "";
        var memberId = reader.ReadString() ?? "";
        var memberEpoch = reader.ReadInt32();
        var instanceId = reader.ReadNullableString();
        var rackId = reader.ReadNullableString();
        var rebalanceTimeoutMs = reader.ReadInt32();

        string[]? subscribedTopicNames = null;
        if (reader.ReadBoolean())
        {
            var topicCount = reader.ReadInt32();
            subscribedTopicNames = new string[topicCount];
            for (var i = 0; i < topicCount; i++)
                subscribedTopicNames[i] = reader.ReadString() ?? "";
        }

        var serverAssignor = reader.ReadNullableString();

        var partitionCount = reader.ReadInt32();
        var topicPartitions = new TopicPartitionAssignment[partitionCount];
        for (var i = 0; i < partitionCount; i++)
        {
            var topicId = GuidHelper.ReadGuid(ref reader);
            var pCount = reader.ReadInt32();
            var partitions = new int[pCount];
            for (var j = 0; j < pCount; j++)
                partitions[j] = reader.ReadInt32();
            topicPartitions[i] = new TopicPartitionAssignment(topicId, partitions);
        }

        return new ConsumerGroupHeartbeatRequestPayload
        {
            GroupId = groupId,
            MemberId = memberId,
            MemberEpoch = memberEpoch,
            InstanceId = instanceId,
            RackId = rackId,
            RebalanceTimeoutMs = rebalanceTimeoutMs,
            SubscribedTopicNames = subscribedTopicNames,
            ServerAssignor = serverAssignor,
            TopicPartitions = topicPartitions
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteInt32(MemberEpoch);
        writer.WriteNullableString(InstanceId);
        writer.WriteNullableString(RackId);
        writer.WriteInt32(RebalanceTimeoutMs);

        if (SubscribedTopicNames is not null)
        {
            writer.WriteBoolean(true);
            writer.WriteInt32(SubscribedTopicNames.Length);
            foreach (var topic in SubscribedTopicNames)
                writer.WriteString(topic);
        }
        else
        {
            writer.WriteBoolean(false);
        }

        writer.WriteNullableString(ServerAssignor);

        var partitions = TopicPartitions ?? [];
        writer.WriteInt32(partitions.Length);
        foreach (var tp in partitions)
        {
            GuidHelper.WriteGuid(ref writer, tp.TopicId);
            writer.WriteInt32(tp.Partitions.Length);
            foreach (var p in tp.Partitions)
                writer.WriteInt32(p);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteInt32(MemberEpoch);
        writer.WriteNullableString(InstanceId);
        writer.WriteNullableString(RackId);
        writer.WriteInt32(RebalanceTimeoutMs);

        if (SubscribedTopicNames is not null)
        {
            writer.WriteBoolean(true);
            writer.WriteInt32(SubscribedTopicNames.Length);
            foreach (var topic in SubscribedTopicNames)
                writer.WriteString(topic);
        }
        else
        {
            writer.WriteBoolean(false);
        }

        writer.WriteNullableString(ServerAssignor);

        var partitions = TopicPartitions ?? [];
        writer.WriteInt32(partitions.Length);
        foreach (var tp in partitions)
        {
            GuidHelper.WriteGuid(writer, tp.TopicId);
            writer.WriteInt32(tp.Partitions.Length);
            foreach (var p in tp.Partitions)
                writer.WriteInt32(p);
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size =
            2 + Encoding.UTF8.GetByteCount(GroupId ?? "") +
            2 + Encoding.UTF8.GetByteCount(MemberId ?? "") +
            4 + // MemberEpoch
            1 + (InstanceId != null ? 2 + Encoding.UTF8.GetByteCount(InstanceId) : 0) +
            1 + (RackId != null ? 2 + Encoding.UTF8.GetByteCount(RackId) : 0) +
            4 + // RebalanceTimeoutMs
            1;  // SubscribedTopicNames null marker

        if (SubscribedTopicNames is not null)
        {
            size += 4; // array count
            foreach (var topic in SubscribedTopicNames)
                size += 2 + Encoding.UTF8.GetByteCount(topic ?? "");
        }

        size += 1 + (ServerAssignor != null ? 2 + Encoding.UTF8.GetByteCount(ServerAssignor) : 0);

        var partitions = TopicPartitions ?? [];
        size += 4; // partition array count
        foreach (var tp in partitions)
            size += 16 + 4 + tp.Partitions.Length * 4; // Guid(16) + count(4) + partitions

        return size;
    }
}

/// <summary>
/// Topic partition assignment: a topic ID paired with partition indices.
/// Used by KIP-848 consumer group heartbeat and describe responses.
/// </summary>
public readonly record struct TopicPartitionAssignment(Guid TopicId, int[] Partitions);
