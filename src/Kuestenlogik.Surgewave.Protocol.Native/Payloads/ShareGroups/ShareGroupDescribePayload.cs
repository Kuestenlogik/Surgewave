using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ShareGroups;

/// <summary>
/// Wire format for ShareGroupDescribe request.
///
/// Wire layout:
///   groupIdCount  int32
///   for each group:
///     groupId     string (int16 length prefix + UTF-8)
/// </summary>
public readonly record struct ShareGroupDescribeRequestPayload
{
    public string[] GroupIds { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static ShareGroupDescribeRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var groupIds = new string[count];
        for (var i = 0; i < count; i++)
            groupIds[i] = reader.ReadString() ?? "";

        return new ShareGroupDescribeRequestPayload { GroupIds = groupIds };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(GroupIds.Length);
        foreach (var groupId in GroupIds)
            writer.WriteString(groupId);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(GroupIds.Length);
        foreach (var groupId in GroupIds)
            writer.WriteString(groupId);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 4; // GroupIds count
        foreach (var groupId in GroupIds)
            size += 2 + Encoding.UTF8.GetByteCount(groupId ?? "");
        return size;
    }
}

/// <summary>
/// Wire format for ShareGroupDescribe response.
///
/// Wire layout:
///   groupCount               int32
///   for each group:
///     groupId                string (int16 length prefix + UTF-8)
///     state                  string (int16 length prefix + UTF-8)
///     assignorName           string (int16 length prefix + UTF-8)
///     memberCount            int32
///     for each member:
///       memberId             string (int16 length prefix + UTF-8)
///       rackId               string (int16 length prefix + UTF-8, -1 = null)
///       clientId             string (int16 length prefix + UTF-8)
///       clientHost           string (int16 length prefix + UTF-8)
///       subscribedTopicCount int32
///       for each subscribed topic:
///         topicName          string (int16 length prefix + UTF-8)
///       assignmentCount      int32
///       for each assignment:
///         topicId            16 bytes (big-endian UUID)
///         partitionCount     int32
///         for each partition:
///           partitionIndex   int32
/// </summary>
public readonly record struct ShareGroupDescribeResponsePayload
{
    public DescribedShareGroup[] Groups { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static ShareGroupDescribeResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var groupCount = reader.ReadInt32();
        var groups = new DescribedShareGroup[groupCount];

        for (var g = 0; g < groupCount; g++)
        {
            var groupId = reader.ReadString() ?? "";
            var state = reader.ReadString() ?? "";
            var assignorName = reader.ReadString() ?? "";

            var memberCount = reader.ReadInt32();
            var members = new DescribedShareGroupMember[memberCount];

            for (var m = 0; m < memberCount; m++)
            {
                var memberId = reader.ReadString() ?? "";
                var rackId = reader.ReadString();
                var clientId = reader.ReadString() ?? "";
                var clientHost = reader.ReadString() ?? "";

                var topicCount = reader.ReadInt32();
                var subscribedTopicNames = new string[topicCount];
                for (var t = 0; t < topicCount; t++)
                    subscribedTopicNames[t] = reader.ReadString() ?? "";

                var assignmentCount = reader.ReadInt32();
                var assignment = new DescribedTopicPartitions[assignmentCount];
                for (var a = 0; a < assignmentCount; a++)
                {
                    var topicId = GuidHelper.ReadGuid(ref reader);
                    var partitionCount = reader.ReadInt32();
                    var partitions = new int[partitionCount];
                    for (var p = 0; p < partitionCount; p++)
                        partitions[p] = reader.ReadInt32();
                    assignment[a] = new DescribedTopicPartitions(topicId, partitions);
                }

                members[m] = new DescribedShareGroupMember
                {
                    MemberId = memberId,
                    RackId = rackId,
                    ClientId = clientId,
                    ClientHost = clientHost,
                    SubscribedTopicNames = subscribedTopicNames,
                    Assignment = assignment
                };
            }

            groups[g] = new DescribedShareGroup
            {
                GroupId = groupId,
                State = state,
                AssignorName = assignorName,
                Members = members
            };
        }

        return new ShareGroupDescribeResponsePayload { Groups = groups };
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
            writer.WriteString(group.State);
            writer.WriteString(group.AssignorName);
            writer.WriteInt32(group.Members.Length);

            foreach (var member in group.Members)
            {
                writer.WriteString(member.MemberId);
                writer.WriteString(member.RackId);
                writer.WriteString(member.ClientId);
                writer.WriteString(member.ClientHost);

                writer.WriteInt32(member.SubscribedTopicNames.Length);
                foreach (var topic in member.SubscribedTopicNames)
                    writer.WriteString(topic);

                writer.WriteInt32(member.Assignment.Length);
                foreach (var tp in member.Assignment)
                {
                    GuidHelper.WriteGuid(ref writer, tp.TopicId);
                    writer.WriteInt32(tp.Partitions.Length);
                    foreach (var p in tp.Partitions)
                        writer.WriteInt32(p);
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
            writer.WriteString(group.State);
            writer.WriteString(group.AssignorName);
            writer.WriteInt32(group.Members.Length);

            foreach (var member in group.Members)
            {
                writer.WriteString(member.MemberId);
                writer.WriteString(member.RackId);
                writer.WriteString(member.ClientId);
                writer.WriteString(member.ClientHost);

                writer.WriteInt32(member.SubscribedTopicNames.Length);
                foreach (var topic in member.SubscribedTopicNames)
                    writer.WriteString(topic);

                writer.WriteInt32(member.Assignment.Length);
                foreach (var tp in member.Assignment)
                {
                    GuidHelper.WriteGuid(writer, tp.TopicId);
                    writer.WriteInt32(tp.Partitions.Length);
                    foreach (var p in tp.Partitions)
                        writer.WriteInt32(p);
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
            size += 2 + Encoding.UTF8.GetByteCount(group.GroupId ?? "");
            size += 2 + Encoding.UTF8.GetByteCount(group.State ?? "");
            size += 2 + Encoding.UTF8.GetByteCount(group.AssignorName ?? "");
            size += 4; // Members count

            foreach (var member in group.Members)
            {
                size += 2 + Encoding.UTF8.GetByteCount(member.MemberId ?? "");
                size += 2 + (member.RackId != null ? Encoding.UTF8.GetByteCount(member.RackId) : 0);
                size += 2 + Encoding.UTF8.GetByteCount(member.ClientId ?? "");
                size += 2 + Encoding.UTF8.GetByteCount(member.ClientHost ?? "");

                size += 4; // SubscribedTopicNames count
                foreach (var topic in member.SubscribedTopicNames)
                    size += 2 + Encoding.UTF8.GetByteCount(topic ?? "");

                size += 4; // Assignment count
                foreach (var tp in member.Assignment)
                    size += 16 + 4 + tp.Partitions.Length * 4;
            }
        }

        return size;
    }
}

/// <summary>
/// A described share group with its state, assignor, and members.
/// </summary>
public readonly record struct DescribedShareGroup
{
    public string GroupId { get; init; }
    public string State { get; init; }
    public string AssignorName { get; init; }
    public DescribedShareGroupMember[] Members { get; init; }
}

/// <summary>
/// A member within a described share group.
/// </summary>
public readonly record struct DescribedShareGroupMember
{
    public string MemberId { get; init; }
    public string? RackId { get; init; }
    public string ClientId { get; init; }
    public string ClientHost { get; init; }
    public string[] SubscribedTopicNames { get; init; }
    public DescribedTopicPartitions[] Assignment { get; init; }
}

/// <summary>
/// A topic ID (UUID) paired with an array of partition indices within a described group.
/// </summary>
public readonly record struct DescribedTopicPartitions(Guid TopicId, int[] Partitions);
