using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.StreamsGroups;

/// <summary>
/// Wire format for Streams Group Heartbeat request (KIP-1071).
/// Streams-specific heartbeat that includes topology and task assignment information.
///
/// Wire layout:
///   GroupId              string
///   MemberId             string
///   MemberEpoch          int32
///   InstanceId           nullable string
///   RackId               nullable string
///   ProcessId            Guid (16 bytes, big-endian UUID)
///   TopologyEpoch        int32
///   Subtopologies        SubtopologyInfo[] (int32 count + entries)
///   ActiveTasks          TaskId[] (int32 count + entries)
///   StandbyTasks         TaskId[] (int32 count + entries)
///   WarmupTasks          TaskId[] (int32 count + entries)
///   ShutdownApplication  bool (1 byte)
/// </summary>
public readonly record struct StreamsGroupHeartbeatRequestPayload
{
    public string GroupId { get; init; }
    public string MemberId { get; init; }
    public int MemberEpoch { get; init; }
    public string? InstanceId { get; init; }
    public string? RackId { get; init; }
    public Guid ProcessId { get; init; }
    public int TopologyEpoch { get; init; }
    public SubtopologyInfo[] Subtopologies { get; init; }
    public StreamsTaskId[] ActiveTasks { get; init; }
    public StreamsTaskId[] StandbyTasks { get; init; }
    public StreamsTaskId[] WarmupTasks { get; init; }
    public bool ShutdownApplication { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static StreamsGroupHeartbeatRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var groupId = reader.ReadString() ?? "";
        var memberId = reader.ReadString() ?? "";
        var memberEpoch = reader.ReadInt32();
        var instanceId = reader.ReadNullableString();
        var rackId = reader.ReadNullableString();
        var processId = GuidHelper.ReadGuid(ref reader);
        var topologyEpoch = reader.ReadInt32();

        var subtopologyCount = reader.ReadInt32();
        var subtopologies = new SubtopologyInfo[subtopologyCount];
        for (var i = 0; i < subtopologyCount; i++)
        {
            var subtopologyId = reader.ReadString() ?? "";

            var sourceTopicCount = reader.ReadInt32();
            var sourceTopics = new string[sourceTopicCount];
            for (var j = 0; j < sourceTopicCount; j++)
                sourceTopics[j] = reader.ReadString() ?? "";

            var repartitionSinkTopicCount = reader.ReadInt32();
            var repartitionSinkTopics = new string[repartitionSinkTopicCount];
            for (var j = 0; j < repartitionSinkTopicCount; j++)
                repartitionSinkTopics[j] = reader.ReadString() ?? "";

            subtopologies[i] = new SubtopologyInfo(subtopologyId, sourceTopics, repartitionSinkTopics);
        }

        var activeTasks = ReadTaskIds(ref reader);
        var standbyTasks = ReadTaskIds(ref reader);
        var warmupTasks = ReadTaskIds(ref reader);
        var shutdownApplication = reader.ReadBoolean();

        return new StreamsGroupHeartbeatRequestPayload
        {
            GroupId = groupId,
            MemberId = memberId,
            MemberEpoch = memberEpoch,
            InstanceId = instanceId,
            RackId = rackId,
            ProcessId = processId,
            TopologyEpoch = topologyEpoch,
            Subtopologies = subtopologies,
            ActiveTasks = activeTasks,
            StandbyTasks = standbyTasks,
            WarmupTasks = warmupTasks,
            ShutdownApplication = shutdownApplication
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
        GuidHelper.WriteGuid(ref writer, ProcessId);
        writer.WriteInt32(TopologyEpoch);

        var subtopologies = Subtopologies ?? [];
        writer.WriteInt32(subtopologies.Length);
        foreach (var sub in subtopologies)
        {
            writer.WriteString(sub.SubtopologyId);

            var sourceTopics = sub.SourceTopics ?? [];
            writer.WriteInt32(sourceTopics.Length);
            foreach (var topic in sourceTopics)
                writer.WriteString(topic);

            var repartitionSinkTopics = sub.RepartitionSinkTopics ?? [];
            writer.WriteInt32(repartitionSinkTopics.Length);
            foreach (var topic in repartitionSinkTopics)
                writer.WriteString(topic);
        }

        WriteTaskIds(ref writer, ActiveTasks);
        WriteTaskIds(ref writer, StandbyTasks);
        WriteTaskIds(ref writer, WarmupTasks);
        writer.WriteBoolean(ShutdownApplication);
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
        GuidHelper.WriteGuid(writer, ProcessId);
        writer.WriteInt32(TopologyEpoch);

        var subtopologies = Subtopologies ?? [];
        writer.WriteInt32(subtopologies.Length);
        foreach (var sub in subtopologies)
        {
            writer.WriteString(sub.SubtopologyId);

            var sourceTopics = sub.SourceTopics ?? [];
            writer.WriteInt32(sourceTopics.Length);
            foreach (var topic in sourceTopics)
                writer.WriteString(topic);

            var repartitionSinkTopics = sub.RepartitionSinkTopics ?? [];
            writer.WriteInt32(repartitionSinkTopics.Length);
            foreach (var topic in repartitionSinkTopics)
                writer.WriteString(topic);
        }

        WriteTaskIds(writer, ActiveTasks);
        WriteTaskIds(writer, StandbyTasks);
        WriteTaskIds(writer, WarmupTasks);
        writer.WriteBoolean(ShutdownApplication);
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
            16 + // ProcessId
            4 +  // TopologyEpoch
            4;   // subtopology count

        foreach (var sub in Subtopologies ?? [])
        {
            size += 2 + Encoding.UTF8.GetByteCount(sub.SubtopologyId ?? "");
            size += 4; // source topic count
            foreach (var topic in sub.SourceTopics ?? [])
                size += 2 + Encoding.UTF8.GetByteCount(topic ?? "");
            size += 4; // repartition sink topic count
            foreach (var topic in sub.RepartitionSinkTopics ?? [])
                size += 2 + Encoding.UTF8.GetByteCount(topic ?? "");
        }

        size += EstimateTaskIdsSize(ActiveTasks);
        size += EstimateTaskIdsSize(StandbyTasks);
        size += EstimateTaskIdsSize(WarmupTasks);
        size += 1; // ShutdownApplication

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
/// Subtopology information for Streams group topology declaration.
/// </summary>
public readonly record struct SubtopologyInfo(string SubtopologyId, string[] SourceTopics, string[] RepartitionSinkTopics);

/// <summary>
/// Task identifier for Streams processing: a subtopology paired with a partition.
/// </summary>
public readonly record struct StreamsTaskId(string SubtopologyId, int PartitionId);
