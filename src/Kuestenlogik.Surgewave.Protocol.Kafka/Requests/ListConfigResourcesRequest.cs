namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka ListConfigResources request (API Key 74, v0-0).
/// KIP-714: Client telemetry - list metrics resources.
/// Lists available client metrics resources for telemetry subscriptions.
/// </summary>
public sealed class ListConfigResourcesRequest : KafkaRequest
{
    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ListConfigResourcesRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        reader.SkipTaggedFields();

        return new ListConfigResourcesRequest
        {
            ApiKey = ApiKey.ListConfigResources,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId
        };
    }
}

/// <summary>
/// Kafka ListConfigResources response (API Key 74, v0-0).
/// </summary>
public sealed class ListConfigResourcesResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>Each client metrics resource in the response.</summary>
    public required List<ConfigResource> ConfigResources { get; init; }

    public sealed class ConfigResource
    {
        /// <summary>The resource name.</summary>
        public required string Name { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);

        writer.WriteVarInt(ConfigResources.Count + 1);
        foreach (var resource in ConfigResources)
        {
            writer.WriteCompactString(resource.Name);
            writer.WriteVarInt(0); // Resource tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ListConfigResourcesResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();

        var resourceCount = reader.ReadVarInt() - 1;
        var clientMetricsResources = new List<ConfigResource>(resourceCount);
        for (int i = 0; i < resourceCount; i++)
        {
            var name = reader.ReadCompactString() ?? "";
            reader.SkipTaggedFields();

            clientMetricsResources.Add(new ConfigResource
            {
                Name = name
            });
        }

        reader.SkipTaggedFields();

        return new ListConfigResourcesResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ConfigResources = clientMetricsResources
        };
    }
}
