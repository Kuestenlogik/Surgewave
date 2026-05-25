using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.StreamsGroups;

/// <summary>
/// Wire format for Streams Group Describe response (KIP-1071).
///
/// Wire layout:
///   ThrottleTimeMs  int32
///   GroupCount      int32
///   for each group:
///     ErrorCode         int16
///     GroupId           string
///     GroupState        string
///     GroupEpoch        int32
///     TopologyEpoch     int32
///     AssignmentEpoch   int32
///     MemberCount       int32
///     for each member:
///       MemberId        string
///       InstanceId      nullable string
///       RackId          nullable string
///       ClientId        string
///       ClientHost      string
///       ProcessId       Guid (16 bytes, big-endian UUID)
///       TopologyEpoch   int32
///       ActiveTasks     TaskId[] (int32 count + entries)
///       StandbyTasks    TaskId[] (int32 count + entries)
///       WarmupTasks     TaskId[] (int32 count + entries)
///       IsClassic       bool (1 byte)
/// </summary>
public readonly record struct StreamsGroupDescribeResponsePayload
{
    public int ThrottleTimeMs { get; init; }
    public DescribedStreamsGroup[] Groups { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static StreamsGroupDescribeResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var throttleTimeMs = reader.ReadInt32();
        var groupCount = reader.ReadInt32();
        var groups = new DescribedStreamsGroup[groupCount];

        for (var g = 0; g < groupCount; g++)
        {
            var errorCode = reader.ReadInt16();
            var groupId = reader.ReadString() ?? "";
            var groupState = reader.ReadString() ?? "";
            var groupEpoch = reader.ReadInt32();
            var topologyEpoch = reader.ReadInt32();
            var assignmentEpoch = reader.ReadInt32();

            var memberCount = reader.ReadInt32();
            var members = new StreamsGroupMember[memberCount];
            for (var m = 0; m < memberCount; m++)
            {
                var memberId = reader.ReadString() ?? "";
                var instanceId = reader.ReadNullableString();
                var rackId = reader.ReadNullableString();
                var clientId = reader.ReadString() ?? "";
                var clientHost = reader.ReadString() ?? "";
                var processId = GuidHelper.ReadGuid(ref reader);
                var memberTopologyEpoch = reader.ReadInt32();

                var activeTasks = ReadTaskIds(ref reader);
                var standbyTasks = ReadTaskIds(ref reader);
                var warmupTasks = ReadTaskIds(ref reader);
                var isClassic = reader.ReadBoolean();

                members[m] = new StreamsGroupMember
                {
                    MemberId = memberId,
                    InstanceId = instanceId,
                    RackId = rackId,
                    ClientId = clientId,
                    ClientHost = clientHost,
                    ProcessId = processId,
                    TopologyEpoch = memberTopologyEpoch,
                    Assignment = new StreamsTaskAssignment(activeTasks, standbyTasks, warmupTasks),
                    IsClassic = isClassic
                };
            }

            groups[g] = new DescribedStreamsGroup
            {
                ErrorCode = errorCode,
                GroupId = groupId,
                GroupState = groupState,
                GroupEpoch = groupEpoch,
                TopologyEpoch = topologyEpoch,
                AssignmentEpoch = assignmentEpoch,
                Members = members
            };
        }

        return new StreamsGroupDescribeResponsePayload
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
            writer.WriteInt32(group.TopologyEpoch);
            writer.WriteInt32(group.AssignmentEpoch);

            var members = group.Members ?? [];
            writer.WriteInt32(members.Length);

            foreach (var member in members)
            {
                writer.WriteString(member.MemberId);
                writer.WriteNullableString(member.InstanceId);
                writer.WriteNullableString(member.RackId);
                writer.WriteString(member.ClientId);
                writer.WriteString(member.ClientHost);
                GuidHelper.WriteGuid(ref writer, member.ProcessId);
                writer.WriteInt32(member.TopologyEpoch);

                WriteTaskIds(ref writer, member.Assignment.ActiveTasks);
                WriteTaskIds(ref writer, member.Assignment.StandbyTasks);
                WriteTaskIds(ref writer, member.Assignment.WarmupTasks);
                writer.WriteBoolean(member.IsClassic);
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
            writer.WriteInt32(group.TopologyEpoch);
            writer.WriteInt32(group.AssignmentEpoch);

            var members = group.Members ?? [];
            writer.WriteInt32(members.Length);

            foreach (var member in members)
            {
                writer.WriteString(member.MemberId);
                writer.WriteNullableString(member.InstanceId);
                writer.WriteNullableString(member.RackId);
                writer.WriteString(member.ClientId);
                writer.WriteString(member.ClientHost);
                GuidHelper.WriteGuid(writer, member.ProcessId);
                writer.WriteInt32(member.TopologyEpoch);

                WriteTaskIds(writer, member.Assignment.ActiveTasks);
                WriteTaskIds(writer, member.Assignment.StandbyTasks);
                WriteTaskIds(writer, member.Assignment.WarmupTasks);
                writer.WriteBoolean(member.IsClassic);
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
                4 + // TopologyEpoch
                4 + // AssignmentEpoch
                4;  // member count

            foreach (var member in group.Members ?? [])
            {
                size +=
                    2 + Encoding.UTF8.GetByteCount(member.MemberId ?? "") +
                    1 + (member.InstanceId != null ? 2 + Encoding.UTF8.GetByteCount(member.InstanceId) : 0) +
                    1 + (member.RackId != null ? 2 + Encoding.UTF8.GetByteCount(member.RackId) : 0) +
                    2 + Encoding.UTF8.GetByteCount(member.ClientId ?? "") +
                    2 + Encoding.UTF8.GetByteCount(member.ClientHost ?? "") +
                    16 + // ProcessId
                    4;   // TopologyEpoch

                size += EstimateTaskIdsSize(member.Assignment.ActiveTasks);
                size += EstimateTaskIdsSize(member.Assignment.StandbyTasks);
                size += EstimateTaskIdsSize(member.Assignment.WarmupTasks);
                size += 1; // IsClassic
            }
        }

        return size;
    }

    private static StreamsTaskId[] ReadTaskIds(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var tasks = new StreamsTaskId[count];
        for (var i = 0; i < count; i++)
        {
            var subtopologyId = reader.ReadString() ?? "";
            var partitionId = reader.ReadInt32();
            tasks[i] = new StreamsTaskId(subtopologyId, partitionId);
        }
        return tasks;
    }

    private static void WriteTaskIds(ref SurgewavePayloadWriter writer, StreamsTaskId[] tasks)
    {
        var arr = tasks ?? [];
        writer.WriteInt32(arr.Length);
        foreach (var task in arr)
        {
            writer.WriteString(task.SubtopologyId);
            writer.WriteInt32(task.PartitionId);
        }
    }

    private static void WriteTaskIds(IPayloadWriter writer, StreamsTaskId[] tasks)
    {
        var arr = tasks ?? [];
        writer.WriteInt32(arr.Length);
        foreach (var task in arr)
        {
            writer.WriteString(task.SubtopologyId);
            writer.WriteInt32(task.PartitionId);
        }
    }

    private static int EstimateTaskIdsSize(StreamsTaskId[] tasks)
    {
        var arr = tasks ?? [];
        var size = 4; // array count
        foreach (var task in arr)
            size += 2 + Encoding.UTF8.GetByteCount(task.SubtopologyId ?? "") + 4;
        return size;
    }
}

/// <summary>
/// A described Streams group in KIP-1071 describe response.
/// </summary>
public readonly record struct DescribedStreamsGroup
{
    public short ErrorCode { get; init; }
    public string GroupId { get; init; }
    public string GroupState { get; init; }
    public int GroupEpoch { get; init; }
    public int TopologyEpoch { get; init; }
    public int AssignmentEpoch { get; init; }
    public StreamsGroupMember[] Members { get; init; }
}

/// <summary>
/// A member in a KIP-1071 Streams group describe response.
/// </summary>
public readonly record struct StreamsGroupMember
{
    public string MemberId { get; init; }
    public string? InstanceId { get; init; }
    public string? RackId { get; init; }
    public string ClientId { get; init; }
    public string ClientHost { get; init; }
    public Guid ProcessId { get; init; }
    public int TopologyEpoch { get; init; }
    public StreamsTaskAssignment Assignment { get; init; }
    public bool IsClassic { get; init; }
}
