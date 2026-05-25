using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ShareGroups;

/// <summary>
/// Wire format for ShareGroupHeartbeat request.
///
/// Wire layout:
///   groupId              string (int16 length prefix + UTF-8)
///   memberId             string (int16 length prefix + UTF-8)
///   memberEpoch          int32
///   rackId               nullable string (1 byte null marker + int16 length prefix + UTF-8)
///   subscribedTopicCount int32 (-1 = null)
///   for each topic:
///     topicName          string (int16 length prefix + UTF-8)
/// </summary>
public readonly record struct ShareGroupHeartbeatRequestPayload
{
    public string GroupId { get; init; }
    public string MemberId { get; init; }
    public int MemberEpoch { get; init; }
    public string? RackId { get; init; }
    public string[]? SubscribedTopicNames { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static ShareGroupHeartbeatRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var groupId = reader.ReadString() ?? "";
        var memberId = reader.ReadString() ?? "";
        var memberEpoch = reader.ReadInt32();
        var rackId = reader.ReadNullableString();

        var topicCount = reader.ReadInt32();
        string[]? subscribedTopicNames = null;
        if (topicCount >= 0)
        {
            subscribedTopicNames = new string[topicCount];
            for (var i = 0; i < topicCount; i++)
                subscribedTopicNames[i] = reader.ReadString() ?? "";
        }

        return new ShareGroupHeartbeatRequestPayload
        {
            GroupId = groupId,
            MemberId = memberId,
            MemberEpoch = memberEpoch,
            RackId = rackId,
            SubscribedTopicNames = subscribedTopicNames
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteInt32(MemberEpoch);
        writer.WriteNullableString(RackId);

        if (SubscribedTopicNames is null)
        {
            writer.WriteInt32(-1);
        }
        else
        {
            writer.WriteInt32(SubscribedTopicNames.Length);
            foreach (var topic in SubscribedTopicNames)
                writer.WriteString(topic);
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
        writer.WriteNullableString(RackId);

        if (SubscribedTopicNames is null)
        {
            writer.WriteInt32(-1);
        }
        else
        {
            writer.WriteInt32(SubscribedTopicNames.Length);
            foreach (var topic in SubscribedTopicNames)
                writer.WriteString(topic);
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
            1 + (RackId != null ? 2 + Encoding.UTF8.GetByteCount(RackId) : 0) +
            4; // SubscribedTopicNames count

        if (SubscribedTopicNames is not null)
        {
            foreach (var topic in SubscribedTopicNames)
                size += 2 + Encoding.UTF8.GetByteCount(topic ?? "");
        }

        return size;
    }
}

/// <summary>
/// Wire format for ShareGroupHeartbeat response.
///
/// Wire layout:
///   throttleTimeMs       int32
///   errorCode            int16
///   memberId             string (int16 length prefix + UTF-8)
///   memberEpoch          int32
///   heartbeatIntervalMs  int32
///   assignmentCount      int32
///   for each assignment:
///     topicId            16 bytes (big-endian UUID)
///     partitionCount     int32
///     for each partition:
///       partitionIndex   int32
/// </summary>
public readonly record struct ShareGroupHeartbeatResponsePayload
{
    public int ThrottleTimeMs { get; init; }
    public short ErrorCode { get; init; }
    public string MemberId { get; init; }
    public int MemberEpoch { get; init; }
    public int HeartbeatIntervalMs { get; init; }
    public HeartbeatTopicPartition[] Assignment { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static ShareGroupHeartbeatResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var throttleTimeMs = reader.ReadInt32();
        var errorCode = reader.ReadInt16();
        var memberId = reader.ReadString() ?? "";
        var memberEpoch = reader.ReadInt32();
        var heartbeatIntervalMs = reader.ReadInt32();

        var assignmentCount = reader.ReadInt32();
        var assignment = new HeartbeatTopicPartition[assignmentCount];
        for (var i = 0; i < assignmentCount; i++)
        {
            var topicId = GuidHelper.ReadGuid(ref reader);
            var partitionCount = reader.ReadInt32();
            var partitions = new int[partitionCount];
            for (var j = 0; j < partitionCount; j++)
                partitions[j] = reader.ReadInt32();
            assignment[i] = new HeartbeatTopicPartition(topicId, partitions);
        }

        return new ShareGroupHeartbeatResponsePayload
        {
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            MemberId = memberId,
            MemberEpoch = memberEpoch,
            HeartbeatIntervalMs = heartbeatIntervalMs,
            Assignment = assignment
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16(ErrorCode);
        writer.WriteString(MemberId);
        writer.WriteInt32(MemberEpoch);
        writer.WriteInt32(HeartbeatIntervalMs);
        writer.WriteInt32(Assignment.Length);

        foreach (var tp in Assignment)
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
        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16(ErrorCode);
        writer.WriteString(MemberId);
        writer.WriteInt32(MemberEpoch);
        writer.WriteInt32(HeartbeatIntervalMs);
        writer.WriteInt32(Assignment.Length);

        foreach (var tp in Assignment)
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
            4 + // ThrottleTimeMs
            2 + // ErrorCode
            2 + Encoding.UTF8.GetByteCount(MemberId ?? "") +
            4 + // MemberEpoch
            4 + // HeartbeatIntervalMs
            4;  // Assignment count

        foreach (var tp in Assignment)
            size += 16 + 4 + tp.Partitions.Length * 4; // TopicId(16) + count(4) + partitions

        return size;
    }

}

/// <summary>
/// A topic ID (UUID) paired with an array of partition indices for heartbeat assignment.
/// </summary>
public readonly record struct HeartbeatTopicPartition(Guid TopicId, int[] Partitions);
