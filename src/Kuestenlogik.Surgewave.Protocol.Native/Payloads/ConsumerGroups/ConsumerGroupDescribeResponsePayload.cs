using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for Consumer Group Describe response (KIP-848).
///
/// Wire layout:
///   ThrottleTimeMs  int32
///   GroupCount      int32
///   for each group:
///     ErrorCode           int16
///     GroupId             string
///     GroupState          string
///     GroupEpoch          int32
///     AssignmentEpoch     int32
///     AssignorName        string
///     MemberCount         int32
///     for each member:
///       MemberId                string
///       InstanceId              nullable string
///       RackId                  nullable string
///       ClientId                string
///       ClientHost              string
///       SubscribedTopicNames    string[] (int32 count + strings)
///       SubscribedTopicRegex    nullable string
///       Assignment              TopicPartitionAssignment[] (int32 count + entries)
///       TargetAssignment        TopicPartitionAssignment[] (int32 count + entries)
/// </summary>
public readonly record struct ConsumerGroupDescribeResponsePayload
{
    public int ThrottleTimeMs { get; init; }
    public DescribedConsumerGroup[] Groups { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static ConsumerGroupDescribeResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var throttleTimeMs = reader.ReadInt32();
        var groupCount = reader.ReadInt32();
        var groups = new DescribedConsumerGroup[groupCount];

        for (var g = 0; g < groupCount; g++)
        {
            var errorCode = reader.ReadInt16();
            var groupId = reader.ReadString() ?? "";
            var groupState = reader.ReadString() ?? "";
            var groupEpoch = reader.ReadInt32();
            var assignmentEpoch = reader.ReadInt32();
            var assignorName = reader.ReadString() ?? "";

            var memberCount = reader.ReadInt32();
            var members = new ConsumerGroupMember[memberCount];
            for (var m = 0; m < memberCount; m++)
            {
                var memberId = reader.ReadString() ?? "";
                var instanceId = reader.ReadNullableString();
                var rackId = reader.ReadNullableString();
                var clientId = reader.ReadString() ?? "";
                var clientHost = reader.ReadString() ?? "";

                var topicNameCount = reader.ReadInt32();
                var subscribedTopicNames = new string[topicNameCount];
                for (var t = 0; t < topicNameCount; t++)
                    subscribedTopicNames[t] = reader.ReadString() ?? "";

                var subscribedTopicRegex = reader.ReadNullableString();

                var assignment = ReadTopicPartitionAssignments(ref reader);
                var targetAssignment = ReadTopicPartitionAssignments(ref reader);

                members[m] = new ConsumerGroupMember
                {
                    MemberId = memberId,
                    InstanceId = instanceId,
                    RackId = rackId,
                    ClientId = clientId,
                    ClientHost = clientHost,
                    SubscribedTopicNames = subscribedTopicNames,
                    SubscribedTopicRegex = subscribedTopicRegex,
                    Assignment = assignment,
                    TargetAssignment = targetAssignment
                };
            }

            groups[g] = new DescribedConsumerGroup
            {
                ErrorCode = errorCode,
                GroupId = groupId,
                GroupState = groupState,
                GroupEpoch = groupEpoch,
                AssignmentEpoch = assignmentEpoch,
                AssignorName = assignorName,
                Members = members
            };
        }

        return new ConsumerGroupDescribeResponsePayload
        {
            ThrottleTimeMs = throttleTimeMs,
            Groups = groups
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(ThrottleTimeMs);
        var groups = Groups ?? [];
        writer.WriteInt32(groups.Length);

        foreach (var group in groups)
        {
            writer.WriteInt16(group.ErrorCode);
            writer.WriteString(group.GroupId);
            writer.WriteString(group.GroupState);
            writer.WriteInt32(group.GroupEpoch);
            writer.WriteInt32(group.AssignmentEpoch);
            writer.WriteString(group.AssignorName);

            var members = group.Members ?? [];
            writer.WriteInt32(members.Length);

            foreach (var member in members)
            {
                writer.WriteString(member.MemberId);
                writer.WriteNullableString(member.InstanceId);
                writer.WriteNullableString(member.RackId);
                writer.WriteString(member.ClientId);
                writer.WriteString(member.ClientHost);

                var topicNames = member.SubscribedTopicNames ?? [];
                writer.WriteInt32(topicNames.Length);
                foreach (var name in topicNames)
                    writer.WriteString(name);

                writer.WriteNullableString(member.SubscribedTopicRegex);

                WriteTopicPartitionAssignments(ref writer, member.Assignment);
                WriteTopicPartitionAssignments(ref writer, member.TargetAssignment);
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(ThrottleTimeMs);
        var groups = Groups ?? [];
        writer.WriteInt32(groups.Length);

        foreach (var group in groups)
        {
            writer.WriteInt16(group.ErrorCode);
            writer.WriteString(group.GroupId);
            writer.WriteString(group.GroupState);
            writer.WriteInt32(group.GroupEpoch);
            writer.WriteInt32(group.AssignmentEpoch);
            writer.WriteString(group.AssignorName);

            var members = group.Members ?? [];
            writer.WriteInt32(members.Length);

            foreach (var member in members)
            {
                writer.WriteString(member.MemberId);
                writer.WriteNullableString(member.InstanceId);
                writer.WriteNullableString(member.RackId);
                writer.WriteString(member.ClientId);
                writer.WriteString(member.ClientHost);

                var topicNames = member.SubscribedTopicNames ?? [];
                writer.WriteInt32(topicNames.Length);
                foreach (var name in topicNames)
                    writer.WriteString(name);

                writer.WriteNullableString(member.SubscribedTopicRegex);

                WriteTopicPartitionAssignments(writer, member.Assignment);
                WriteTopicPartitionAssignments(writer, member.TargetAssignment);
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 4 + 4; // ThrottleTimeMs + group count

        foreach (var group in Groups ?? [])
        {
            size +=
                2 + // ErrorCode
                2 + Encoding.UTF8.GetByteCount(group.GroupId ?? "") +
                2 + Encoding.UTF8.GetByteCount(group.GroupState ?? "") +
                4 + // GroupEpoch
                4 + // AssignmentEpoch
                2 + Encoding.UTF8.GetByteCount(group.AssignorName ?? "") +
                4;  // member count

            foreach (var member in group.Members ?? [])
            {
                size +=
                    2 + Encoding.UTF8.GetByteCount(member.MemberId ?? "") +
                    1 + (member.InstanceId != null ? 2 + Encoding.UTF8.GetByteCount(member.InstanceId) : 0) +
                    1 + (member.RackId != null ? 2 + Encoding.UTF8.GetByteCount(member.RackId) : 0) +
                    2 + Encoding.UTF8.GetByteCount(member.ClientId ?? "") +
                    2 + Encoding.UTF8.GetByteCount(member.ClientHost ?? "") +
                    4; // subscribed topic names count

                foreach (var name in member.SubscribedTopicNames ?? [])
                    size += 2 + Encoding.UTF8.GetByteCount(name ?? "");

                size += 1 + (member.SubscribedTopicRegex != null
                    ? 2 + Encoding.UTF8.GetByteCount(member.SubscribedTopicRegex) : 0);

                size += EstimateTopicPartitionAssignmentsSize(member.Assignment);
                size += EstimateTopicPartitionAssignmentsSize(member.TargetAssignment);
            }
        }

        return size;
    }

    private static TopicPartitionAssignment[] ReadTopicPartitionAssignments(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var assignments = new TopicPartitionAssignment[count];
        for (var i = 0; i < count; i++)
        {
            var topicId = GuidHelper.ReadGuid(ref reader);
            var pCount = reader.ReadInt32();
            var partitions = new int[pCount];
            for (var j = 0; j < pCount; j++)
                partitions[j] = reader.ReadInt32();
            assignments[i] = new TopicPartitionAssignment(topicId, partitions);
        }
        return assignments;
    }

    private static void WriteTopicPartitionAssignments(ref SurgewavePayloadWriter writer, TopicPartitionAssignment[] assignments)
    {
        var arr = assignments ?? [];
        writer.WriteInt32(arr.Length);
        foreach (var tp in arr)
        {
            GuidHelper.WriteGuid(ref writer, tp.TopicId);
            writer.WriteInt32(tp.Partitions.Length);
            foreach (var p in tp.Partitions)
                writer.WriteInt32(p);
        }
    }

    private static void WriteTopicPartitionAssignments(IPayloadWriter writer, TopicPartitionAssignment[] assignments)
    {
        var arr = assignments ?? [];
        writer.WriteInt32(arr.Length);
        foreach (var tp in arr)
        {
            GuidHelper.WriteGuid(writer, tp.TopicId);
            writer.WriteInt32(tp.Partitions.Length);
            foreach (var p in tp.Partitions)
                writer.WriteInt32(p);
        }
    }

    private static int EstimateTopicPartitionAssignmentsSize(TopicPartitionAssignment[] assignments)
    {
        var arr = assignments ?? [];
        var size = 4; // array count
        foreach (var tp in arr)
            size += 16 + 4 + tp.Partitions.Length * 4;
        return size;
    }
}

/// <summary>
/// A described consumer group in KIP-848 describe response.
/// </summary>
public readonly record struct DescribedConsumerGroup
{
    public short ErrorCode { get; init; }
    public string GroupId { get; init; }
    public string GroupState { get; init; }
    public int GroupEpoch { get; init; }
    public int AssignmentEpoch { get; init; }
    public string AssignorName { get; init; }
    public ConsumerGroupMember[] Members { get; init; }
}

/// <summary>
/// A member in a KIP-848 consumer group describe response.
/// </summary>
public readonly record struct ConsumerGroupMember
{
    public string MemberId { get; init; }
    public string? InstanceId { get; init; }
    public string? RackId { get; init; }
    public string ClientId { get; init; }
    public string ClientHost { get; init; }
    public string[] SubscribedTopicNames { get; init; }
    public string? SubscribedTopicRegex { get; init; }
    public TopicPartitionAssignment[] Assignment { get; init; }
    public TopicPartitionAssignment[] TargetAssignment { get; init; }
}
