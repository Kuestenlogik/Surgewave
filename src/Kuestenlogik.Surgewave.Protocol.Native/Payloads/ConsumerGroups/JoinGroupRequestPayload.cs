using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for JoinGroup request.
/// Shared between client (write) and broker (read) to ensure consistency.
/// </summary>
public readonly record struct JoinGroupRequestPayload
{
    public string GroupId { get; init; }
    public string? MemberId { get; init; }
    public string? GroupInstanceId { get; init; }
    public string ClientId { get; init; }
    public string ProtocolType { get; init; }
    public int SessionTimeoutMs { get; init; }
    public int RebalanceTimeoutMs { get; init; }
    public GroupProtocol[] Protocols { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static JoinGroupRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var groupId = reader.ReadString() ?? "";
        var memberId = reader.ReadString();
        var groupInstanceId = reader.ReadString();
        var clientId = reader.ReadString() ?? "";
        var protocolType = reader.ReadString() ?? "consumer";
        var sessionTimeoutMs = reader.ReadInt32();
        var rebalanceTimeoutMs = reader.ReadInt32();

        var protocolCount = reader.ReadInt16();
        var protocols = new GroupProtocol[protocolCount];
        for (int i = 0; i < protocolCount; i++)
        {
            var name = reader.ReadString() ?? "range";
            var metadataLength = reader.ReadInt32();
            var metadata = metadataLength > 0 ? reader.ReadRaw(metadataLength).ToArray() : Array.Empty<byte>();
            protocols[i] = new GroupProtocol(name, metadata);
        }

        return new JoinGroupRequestPayload
        {
            GroupId = groupId,
            MemberId = memberId,
            GroupInstanceId = groupInstanceId,
            ClientId = clientId,
            ProtocolType = protocolType,
            SessionTimeoutMs = sessionTimeoutMs,
            RebalanceTimeoutMs = rebalanceTimeoutMs,
            Protocols = protocols
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteString(GroupInstanceId);
        writer.WriteString(ClientId);
        writer.WriteString(ProtocolType);
        writer.WriteInt32(SessionTimeoutMs);
        writer.WriteInt32(RebalanceTimeoutMs);
        writer.WriteInt16((short)Protocols.Length);

        foreach (var protocol in Protocols)
        {
            writer.WriteString(protocol.Name);
            writer.WriteInt32(protocol.Metadata.Length);
            writer.WriteRaw(protocol.Metadata);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteString(GroupInstanceId);
        writer.WriteString(ClientId);
        writer.WriteString(ProtocolType);
        writer.WriteInt32(SessionTimeoutMs);
        writer.WriteInt32(RebalanceTimeoutMs);
        writer.WriteInt16((short)Protocols.Length);

        foreach (var protocol in Protocols)
        {
            writer.WriteString(protocol.Name);
            writer.WriteInt32(protocol.Metadata.Length);
            writer.WriteBytes(protocol.Metadata);
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size =
            2 + System.Text.Encoding.UTF8.GetByteCount(GroupId ?? "") +
            2 + (MemberId != null ? System.Text.Encoding.UTF8.GetByteCount(MemberId) : 0) +
            2 + (GroupInstanceId != null ? System.Text.Encoding.UTF8.GetByteCount(GroupInstanceId) : 0) +
            2 + System.Text.Encoding.UTF8.GetByteCount(ClientId ?? "") +
            2 + System.Text.Encoding.UTF8.GetByteCount(ProtocolType ?? "") +
            4 + // SessionTimeoutMs
            4 + // RebalanceTimeoutMs
            2;  // Protocol count

        foreach (var protocol in Protocols)
        {
            size += 2 + System.Text.Encoding.UTF8.GetByteCount(protocol.Name);
            size += 4 + protocol.Metadata.Length;
        }

        return size;
    }
}

/// <summary>
/// Group protocol with name and metadata.
/// </summary>
public readonly record struct GroupProtocol(string Name, byte[] Metadata);
