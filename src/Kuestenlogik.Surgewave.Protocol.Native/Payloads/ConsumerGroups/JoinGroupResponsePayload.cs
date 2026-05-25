using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for JoinGroup response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct JoinGroupResponsePayload
{
    public ushort ErrorCode { get; init; }
    public int GenerationId { get; init; }
    public string ProtocolName { get; init; }
    public string LeaderId { get; init; }
    public string MemberId { get; init; }
    public JoinGroupMemberPayload[] Members { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static JoinGroupResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var errorCode = reader.ReadUInt16();
        var generationId = reader.ReadInt32();
        var protocolName = reader.ReadString() ?? "";
        var leaderId = reader.ReadString() ?? "";
        var memberId = reader.ReadString() ?? "";
        var memberCount = reader.ReadInt16();

        var members = new JoinGroupMemberPayload[memberCount];
        for (int i = 0; i < memberCount; i++)
        {
            var mId = reader.ReadString() ?? "";
            var mGroupInstanceId = reader.ReadString();
            var metadataLength = reader.ReadInt32();
            var mMetadata = metadataLength > 0 ? reader.ReadRaw(metadataLength).ToArray() : Array.Empty<byte>();
            members[i] = new JoinGroupMemberPayload
            {
                MemberId = mId,
                GroupInstanceId = mGroupInstanceId,
                Metadata = mMetadata
            };
        }

        return new JoinGroupResponsePayload
        {
            ErrorCode = errorCode,
            GenerationId = generationId,
            ProtocolName = protocolName,
            LeaderId = leaderId,
            MemberId = memberId,
            Members = members
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(GenerationId);
        writer.WriteString(ProtocolName);
        writer.WriteString(LeaderId);
        writer.WriteString(MemberId);
        writer.WriteInt16((short)Members.Length);

        foreach (var member in Members)
        {
            writer.WriteString(member.MemberId);
            writer.WriteString(member.GroupInstanceId);
            writer.WriteInt32(member.Metadata.Length);
            writer.WriteRaw(member.Metadata);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(GenerationId);
        writer.WriteString(ProtocolName);
        writer.WriteString(LeaderId);
        writer.WriteString(MemberId);
        writer.WriteInt16((short)Members.Length);

        foreach (var member in Members)
        {
            writer.WriteString(member.MemberId);
            writer.WriteString(member.GroupInstanceId ?? "");
            writer.WriteInt32(member.Metadata.Length);
            writer.WriteBytes(member.Metadata);
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size =
            2 + // ErrorCode
            4 + // GenerationId
            2 + System.Text.Encoding.UTF8.GetByteCount(ProtocolName ?? "") +
            2 + System.Text.Encoding.UTF8.GetByteCount(LeaderId ?? "") +
            2 + System.Text.Encoding.UTF8.GetByteCount(MemberId ?? "") +
            2;  // Member count

        foreach (var member in Members)
        {
            size += 2 + System.Text.Encoding.UTF8.GetByteCount(member.MemberId);
            size += 2 + (member.GroupInstanceId != null ? System.Text.Encoding.UTF8.GetByteCount(member.GroupInstanceId) : 0);
            size += 4 + member.Metadata.Length;
        }

        return size;
    }
}

/// <summary>
/// Join group member info in response.
/// </summary>
public readonly record struct JoinGroupMemberPayload
{
    public string MemberId { get; init; }
    public string? GroupInstanceId { get; init; }
    public byte[] Metadata { get; init; }
}
