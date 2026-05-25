using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for Consumer Group Heartbeat response (KIP-848).
///
/// Wire layout:
///   ThrottleTimeMs      int32
///   ErrorCode           int16
///   ErrorMessage        nullable string (bool prefix + string)
///   MemberId            nullable string (bool prefix + string)
///   MemberEpoch         int32
///   HeartbeatIntervalMs int32
///   Assignment          nullable TopicPartitionAssignment[] (bool prefix + int32 count + entries)
/// </summary>
public readonly record struct ConsumerGroupHeartbeatResponsePayload
{
    public int ThrottleTimeMs { get; init; }
    public short ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? MemberId { get; init; }
    public int MemberEpoch { get; init; }
    public int HeartbeatIntervalMs { get; init; }
    public TopicPartitionAssignment[]? Assignment { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static ConsumerGroupHeartbeatResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var throttleTimeMs = reader.ReadInt32();
        var errorCode = reader.ReadInt16();
        var errorMessage = reader.ReadNullableString();
        var memberId = reader.ReadNullableString();
        var memberEpoch = reader.ReadInt32();
        var heartbeatIntervalMs = reader.ReadInt32();

        TopicPartitionAssignment[]? assignment = null;
        if (reader.ReadBoolean())
        {
            var count = reader.ReadInt32();
            assignment = new TopicPartitionAssignment[count];
            for (var i = 0; i < count; i++)
            {
                var topicId = GuidHelper.ReadGuid(ref reader);
                var pCount = reader.ReadInt32();
                var partitions = new int[pCount];
                for (var j = 0; j < pCount; j++)
                    partitions[j] = reader.ReadInt32();
                assignment[i] = new TopicPartitionAssignment(topicId, partitions);
            }
        }

        return new ConsumerGroupHeartbeatResponsePayload
        {
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            MemberId = memberId,
            MemberEpoch = memberEpoch,
            HeartbeatIntervalMs = heartbeatIntervalMs,
            Assignment = assignment
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

        if (Assignment is not null)
        {
            writer.WriteBoolean(true);
            writer.WriteInt32(Assignment.Length);
            foreach (var tp in Assignment)
            {
                GuidHelper.WriteGuid(ref writer, tp.TopicId);
                writer.WriteInt32(tp.Partitions.Length);
                foreach (var p in tp.Partitions)
                    writer.WriteInt32(p);
            }
        }
        else
        {
            writer.WriteBoolean(false);
        }
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

        if (Assignment is not null)
        {
            writer.WriteBoolean(true);
            writer.WriteInt32(Assignment.Length);
            foreach (var tp in Assignment)
            {
                GuidHelper.WriteGuid(writer, tp.TopicId);
                writer.WriteInt32(tp.Partitions.Length);
                foreach (var p in tp.Partitions)
                    writer.WriteInt32(p);
            }
        }
        else
        {
            writer.WriteBoolean(false);
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
            1 + (ErrorMessage != null ? 2 + Encoding.UTF8.GetByteCount(ErrorMessage) : 0) +
            1 + (MemberId != null ? 2 + Encoding.UTF8.GetByteCount(MemberId) : 0) +
            4 + // MemberEpoch
            4 + // HeartbeatIntervalMs
            1;  // Assignment null marker

        if (Assignment is not null)
        {
            size += 4; // array count
            foreach (var tp in Assignment)
                size += 16 + 4 + tp.Partitions.Length * 4;
        }

        return size;
    }
}
