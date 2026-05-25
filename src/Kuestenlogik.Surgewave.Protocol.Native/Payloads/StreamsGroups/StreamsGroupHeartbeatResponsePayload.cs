using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.StreamsGroups;

/// <summary>
/// Wire format for Streams Group Heartbeat response (KIP-1071).
/// Returned by the broker with the assigned tasks for this streams member.
///
/// Wire layout:
///   ThrottleTimeMs      int32
///   ErrorCode           int16
///   ErrorMessage        nullable string
///   MemberId            nullable string
///   MemberEpoch         int32
///   HeartbeatIntervalMs int32
///   ActiveTasks         TaskId[] (int32 count + entries)
///   StandbyTasks        TaskId[] (int32 count + entries)
///   WarmupTasks         TaskId[] (int32 count + entries)
/// </summary>
public readonly record struct StreamsGroupHeartbeatResponsePayload
{
    public int ThrottleTimeMs { get; init; }
    public short ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? MemberId { get; init; }
    public int MemberEpoch { get; init; }
    public int HeartbeatIntervalMs { get; init; }
    public StreamsTaskAssignment Assignment { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static StreamsGroupHeartbeatResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var throttleTimeMs = reader.ReadInt32();
        var errorCode = reader.ReadInt16();
        var errorMessage = reader.ReadNullableString();
        var memberId = reader.ReadNullableString();
        var memberEpoch = reader.ReadInt32();
        var heartbeatIntervalMs = reader.ReadInt32();

        var activeTasks = ReadTaskIds(ref reader);
        var standbyTasks = ReadTaskIds(ref reader);
        var warmupTasks = ReadTaskIds(ref reader);

        return new StreamsGroupHeartbeatResponsePayload
        {
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            MemberId = memberId,
            MemberEpoch = memberEpoch,
            HeartbeatIntervalMs = heartbeatIntervalMs,
            Assignment = new StreamsTaskAssignment(activeTasks, standbyTasks, warmupTasks)
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16(ErrorCode);
        writer.WriteNullableString(ErrorMessage);
        writer.WriteNullableString(MemberId);
        writer.WriteInt32(MemberEpoch);
        writer.WriteInt32(HeartbeatIntervalMs);

        WriteTaskIds(ref writer, Assignment.ActiveTasks);
        WriteTaskIds(ref writer, Assignment.StandbyTasks);
        WriteTaskIds(ref writer, Assignment.WarmupTasks);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16(ErrorCode);
        writer.WriteNullableString(ErrorMessage);
        writer.WriteNullableString(MemberId);
        writer.WriteInt32(MemberEpoch);
        writer.WriteInt32(HeartbeatIntervalMs);

        WriteTaskIds(writer, Assignment.ActiveTasks);
        WriteTaskIds(writer, Assignment.StandbyTasks);
        WriteTaskIds(writer, Assignment.WarmupTasks);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        return
            4 + // ThrottleTimeMs
            2 + // ErrorCode
            1 + (ErrorMessage != null ? 2 + Encoding.UTF8.GetByteCount(ErrorMessage) : 0) +
            1 + (MemberId != null ? 2 + Encoding.UTF8.GetByteCount(MemberId) : 0) +
            4 + // MemberEpoch
            4 + // HeartbeatIntervalMs
            EstimateTaskIdsSize(Assignment.ActiveTasks) +
            EstimateTaskIdsSize(Assignment.StandbyTasks) +
            EstimateTaskIdsSize(Assignment.WarmupTasks);
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
/// Task assignment for a Streams group member: active, standby, and warmup tasks.
/// </summary>
public readonly record struct StreamsTaskAssignment(
    StreamsTaskId[] ActiveTasks,
    StreamsTaskId[] StandbyTasks,
    StreamsTaskId[] WarmupTasks);
