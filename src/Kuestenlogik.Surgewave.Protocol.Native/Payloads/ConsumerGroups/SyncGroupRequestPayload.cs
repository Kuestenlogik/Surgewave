using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for SyncGroup request.
/// Shared between client (write) and broker (read) to ensure consistency.
/// </summary>
public readonly record struct SyncGroupRequestPayload
{
    public string GroupId { get; init; }
    public string MemberId { get; init; }
    public int GenerationId { get; init; }
    public MemberAssignmentPayload[] Assignments { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static SyncGroupRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var groupId = reader.ReadString() ?? "";
        var memberId = reader.ReadString() ?? "";
        var generationId = reader.ReadInt32();

        var assignmentCount = reader.ReadInt16();
        var assignments = new MemberAssignmentPayload[assignmentCount];
        for (int i = 0; i < assignmentCount; i++)
        {
            var memberIdAssign = reader.ReadString() ?? "";
            var assignmentLength = reader.ReadInt32();
            var assignment = assignmentLength > 0 ? reader.ReadRaw(assignmentLength).ToArray() : Array.Empty<byte>();
            assignments[i] = new MemberAssignmentPayload
            {
                MemberId = memberIdAssign,
                Assignment = assignment
            };
        }

        return new SyncGroupRequestPayload
        {
            GroupId = groupId,
            MemberId = memberId,
            GenerationId = generationId,
            Assignments = assignments
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteInt32(GenerationId);
        writer.WriteInt16((short)Assignments.Length);

        foreach (var assignment in Assignments)
        {
            writer.WriteString(assignment.MemberId);
            writer.WriteInt32(assignment.Assignment.Length);
            writer.WriteRaw(assignment.Assignment);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteInt32(GenerationId);
        writer.WriteInt16((short)Assignments.Length);

        foreach (var assignment in Assignments)
        {
            writer.WriteString(assignment.MemberId);
            writer.WriteInt32(assignment.Assignment.Length);
            writer.WriteBytes(assignment.Assignment);
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size =
            2 + System.Text.Encoding.UTF8.GetByteCount(GroupId ?? "") +
            2 + System.Text.Encoding.UTF8.GetByteCount(MemberId ?? "") +
            4 + // GenerationId
            2;  // Assignment count

        foreach (var assignment in Assignments)
        {
            size += 2 + System.Text.Encoding.UTF8.GetByteCount(assignment.MemberId);
            size += 4 + assignment.Assignment.Length;
        }

        return size;
    }
}

/// <summary>
/// Member assignment in sync group request.
/// </summary>
public readonly record struct MemberAssignmentPayload
{
    public string MemberId { get; init; }
    public byte[] Assignment { get; init; }
}
