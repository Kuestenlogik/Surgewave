using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for ListGroups response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct ListGroupsResponsePayload
{
    public ushort ErrorCode { get; init; }
    public GroupInfoPayload[] Groups { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static ListGroupsResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var errorCode = reader.ReadUInt16();
        var groupCount = reader.ReadInt16();

        var groups = new GroupInfoPayload[groupCount];
        for (int i = 0; i < groupCount; i++)
        {
            var groupId = reader.ReadString() ?? "";
            var protocolType = reader.ReadString() ?? "";
            var state = reader.ReadString() ?? "";
            groups[i] = new GroupInfoPayload
            {
                GroupId = groupId,
                ProtocolType = protocolType,
                State = state
            };
        }

        return new ListGroupsResponsePayload
        {
            ErrorCode = errorCode,
            Groups = groups
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt16((short)Groups.Length);

        foreach (var group in Groups)
        {
            writer.WriteString(group.GroupId);
            writer.WriteString(group.ProtocolType);
            writer.WriteString(group.State);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt16((short)Groups.Length);

        foreach (var group in Groups)
        {
            writer.WriteString(group.GroupId);
            writer.WriteString(group.ProtocolType);
            writer.WriteString(group.State);
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size =
            2 + // ErrorCode
            2;  // Group count

        foreach (var group in Groups)
        {
            size += 2 + System.Text.Encoding.UTF8.GetByteCount(group.GroupId);
            size += 2 + System.Text.Encoding.UTF8.GetByteCount(group.ProtocolType);
            size += 2 + System.Text.Encoding.UTF8.GetByteCount(group.State);
        }

        return size;
    }
}

/// <summary>
/// Group information in list groups response.
/// </summary>
public readonly record struct GroupInfoPayload
{
    public string GroupId { get; init; }
    public string ProtocolType { get; init; }
    public string State { get; init; }
}
