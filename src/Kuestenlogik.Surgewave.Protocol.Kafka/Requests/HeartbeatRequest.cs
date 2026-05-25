namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Heartbeat request - Keep group membership alive
/// </summary>
public sealed class HeartbeatRequest : KafkaRequest
{
    public required string GroupId { get; init; }
    public required int GenerationId { get; init; }
    public required string MemberId { get; init; }
    public string? GroupInstanceId { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteString(GroupId);
        writer.WriteInt32(GenerationId);
        writer.WriteString(MemberId);

        if (ApiVersion >= 3)
        {
            writer.WriteString(GroupInstanceId);
        }
    }

    public static HeartbeatRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var groupId = BinaryHelpers.ReadString(reader);
        var generationId = BinaryHelpers.ReadInt32BigEndian(reader);
        var memberId = BinaryHelpers.ReadString(reader);
        string? groupInstanceId = apiVersion >= 3 ? BinaryHelpers.ReadString(reader) : null;

        return new HeartbeatRequest
        {
            ApiKey = ApiKey.Heartbeat,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId,
            GenerationId = generationId,
            MemberId = memberId,
            GroupInstanceId = groupInstanceId
        };
    }

}

/// <summary>
/// Heartbeat response
/// </summary>
public sealed class HeartbeatResponse : KafkaResponse
{
    public required ErrorCode ErrorCode { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // Response header
        writer.WriteInt32(CorrelationId);
        bool isFlexible = ApiVersion >= 4;
        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        // Response body
        // Version 0: error_code
        // Version 1+: throttle_time_ms, error_code
        // Version 4+: Flexible format with tagged fields

        if (ApiVersion >= 1)
        {
            writer.WriteInt32(0); // ThrottleTimeMs
        }

        writer.WriteInt16((short)ErrorCode);

        // Body tagged fields for flexible versions (v4+)
        if (isFlexible)
        {
            writer.WriteVarInt(0); // No body tagged fields
        }
    }
}
