using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for DescribeGroup response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct DescribeGroupResponsePayload
{
    public ushort ErrorCode { get; init; }
    public string GroupId { get; init; }
    public string State { get; init; }
    public string ProtocolType { get; init; }
    public string ProtocolName { get; init; }
    public int GenerationId { get; init; }
    public GroupMemberPayload[] Members { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static DescribeGroupResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var errorCode = reader.ReadUInt16();
        var groupId = reader.ReadString() ?? "";
        var state = reader.ReadString() ?? "";
        var protocolType = reader.ReadString() ?? "";
        var protocolName = reader.ReadString() ?? "";
        var generationId = reader.ReadInt32();
        var memberCount = reader.ReadInt16();

        var members = new GroupMemberPayload[memberCount];
        for (int i = 0; i < memberCount; i++)
        {
            var memberId = reader.ReadString() ?? "";
            var groupInstanceId = reader.ReadString();
            var clientId = reader.ReadString() ?? "";
            var metadataLength = reader.ReadInt32();
            var metadata = metadataLength > 0 ? reader.ReadRaw(metadataLength).ToArray() : Array.Empty<byte>();
            var assignmentLength = reader.ReadInt32();
            var assignment = assignmentLength > 0 ? reader.ReadRaw(assignmentLength).ToArray() : Array.Empty<byte>();

            members[i] = new GroupMemberPayload
            {
                MemberId = memberId,
                GroupInstanceId = groupInstanceId,
                ClientId = clientId,
                Metadata = metadata,
                Assignment = assignment
            };
        }

        return new DescribeGroupResponsePayload
        {
            ErrorCode = errorCode,
            GroupId = groupId,
            State = state,
            ProtocolType = protocolType,
            ProtocolName = protocolName,
            GenerationId = generationId,
            Members = members
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteString(GroupId);
        writer.WriteString(State);
        writer.WriteString(ProtocolType);
        writer.WriteString(ProtocolName);
        writer.WriteInt32(GenerationId);
        writer.WriteInt16((short)Members.Length);

        foreach (var member in Members)
        {
            writer.WriteString(member.MemberId);
            writer.WriteString(member.GroupInstanceId);
            writer.WriteString(member.ClientId);
            writer.WriteInt32(member.Metadata.Length);
            writer.WriteRaw(member.Metadata);
            writer.WriteInt32(member.Assignment.Length);
            writer.WriteRaw(member.Assignment);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteString(GroupId);
        writer.WriteString(State);
        writer.WriteString(ProtocolType);
        writer.WriteString(ProtocolName);
        writer.WriteInt32(GenerationId);
        writer.WriteInt16((short)Members.Length);

        foreach (var member in Members)
        {
            writer.WriteString(member.MemberId);
            writer.WriteString(member.GroupInstanceId ?? "");
            writer.WriteString(member.ClientId);
            writer.WriteInt32(member.Metadata.Length);
            writer.WriteBytes(member.Metadata);
            writer.WriteInt32(member.Assignment.Length);
            writer.WriteBytes(member.Assignment);
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size =
            2 + // ErrorCode
            2 + System.Text.Encoding.UTF8.GetByteCount(GroupId ?? "") +
            2 + System.Text.Encoding.UTF8.GetByteCount(State ?? "") +
            2 + System.Text.Encoding.UTF8.GetByteCount(ProtocolType ?? "") +
            2 + System.Text.Encoding.UTF8.GetByteCount(ProtocolName ?? "") +
            4 + // GenerationId
            2;  // Member count

        foreach (var member in Members)
        {
            size += 2 + System.Text.Encoding.UTF8.GetByteCount(member.MemberId);
            size += 2 + (member.GroupInstanceId != null ? System.Text.Encoding.UTF8.GetByteCount(member.GroupInstanceId) : 0);
            size += 2 + System.Text.Encoding.UTF8.GetByteCount(member.ClientId);
            size += 4 + member.Metadata.Length;
            size += 4 + member.Assignment.Length;
        }

        return size;
    }
}

/// <summary>
/// Group member info in describe group response.
/// </summary>
public readonly record struct GroupMemberPayload
{
    public string MemberId { get; init; }
    public string? GroupInstanceId { get; init; }
    public string ClientId { get; init; }
    public byte[] Metadata { get; init; }
    public byte[] Assignment { get; init; }
}
