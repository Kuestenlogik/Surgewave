namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka AlterConfigs request (API Key 33)
/// Alters configuration settings for resources like topics and brokers.
/// </summary>
public sealed class AlterConfigsRequest : KafkaRequest
{
    public required List<AlterConfigResource> Resources { get; init; }
    public bool ValidateOnly { get; init; }

    public sealed class AlterConfigResource
    {
        public required ConfigResourceType ResourceType { get; init; }
        public required string ResourceName { get; init; }
        public required List<AlterConfigEntry> Configs { get; init; }
    }

    public sealed class AlterConfigEntry
    {
        public required string Name { get; init; }
        public string? Value { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteCompactString(ClientId);
            writer.WriteVarInt(0); // Header tagged fields

            // Resources COMPACT_ARRAY (count + 1)
            writer.WriteVarInt(Resources.Count + 1);
            foreach (var resource in Resources)
            {
                writer.WriteInt8((sbyte)resource.ResourceType);
                writer.WriteCompactString(resource.ResourceName);

                // Configs COMPACT_ARRAY
                writer.WriteVarInt(resource.Configs.Count + 1);
                foreach (var config in resource.Configs)
                {
                    writer.WriteCompactString(config.Name);
                    writer.WriteCompactString(config.Value);
                    writer.WriteVarInt(0); // Config-entry tagged fields
                }

                writer.WriteVarInt(0); // Resource tagged fields
            }

            writer.WriteInt8(ValidateOnly ? (sbyte)1 : (sbyte)0);
            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteString(ClientId);

            writer.WriteInt32(Resources.Count);
            foreach (var resource in Resources)
            {
                writer.WriteInt8((sbyte)resource.ResourceType);
                writer.WriteString(resource.ResourceName);
                writer.WriteInt32(resource.Configs.Count);
                foreach (var config in resource.Configs)
                {
                    writer.WriteString(config.Name);
                    writer.WriteString(config.Value);
                }
            }
            writer.WriteInt8(ValidateOnly ? (sbyte)1 : (sbyte)0);
        }
    }

    public static AlterConfigsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;
        var resources = new List<AlterConfigResource>();

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            var resourceCount = protocolReader.ReadVarInt() - 1;
            for (int i = 0; i < resourceCount; i++)
            {
                var resourceType = (ConfigResourceType)protocolReader.ReadInt8();
                var resourceName = protocolReader.ReadCompactString()!;

                var configCount = protocolReader.ReadVarInt() - 1;
                var configs = new List<AlterConfigEntry>();
                for (int j = 0; j < configCount; j++)
                {
                    var name = protocolReader.ReadCompactString()!;
                    var value = protocolReader.ReadCompactString();
                    protocolReader.ReadVarInt(); // tagged fields
                    configs.Add(new AlterConfigEntry { Name = name, Value = value });
                }

                protocolReader.ReadVarInt(); // resource tagged fields

                resources.Add(new AlterConfigResource
                {
                    ResourceType = resourceType,
                    ResourceName = resourceName,
                    Configs = configs
                });
            }

            var validateOnly = protocolReader.ReadInt8() != 0;
            protocolReader.ReadVarInt(); // request tagged fields

            return new AlterConfigsRequest
            {
                ApiKey = ApiKey.AlterConfigs,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                Resources = resources,
                ValidateOnly = validateOnly
            };
        }
        else
        {
            var resourceCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < resourceCount; i++)
            {
                var resourceType = (ConfigResourceType)reader.ReadSByte();
                var resourceName = BinaryHelpers.ReadString(reader);

                var configCount = BinaryHelpers.ReadInt32BigEndian(reader);
                var configs = new List<AlterConfigEntry>();
                for (int j = 0; j < configCount; j++)
                {
                    var name = BinaryHelpers.ReadString(reader);
                    var value = BinaryHelpers.ReadString(reader);
                    configs.Add(new AlterConfigEntry { Name = name, Value = value });
                }

                resources.Add(new AlterConfigResource
                {
                    ResourceType = resourceType,
                    ResourceName = resourceName,
                    Configs = configs
                });
            }

            var validateOnly = reader.ReadByte() != 0;

            return new AlterConfigsRequest
            {
                ApiKey = ApiKey.AlterConfigs,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                Resources = resources,
                ValidateOnly = validateOnly
            };
        }
    }
}

/// <summary>
/// Kafka AlterConfigs response
/// </summary>
public sealed class AlterConfigsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required List<AlterConfigsResult> Results { get; init; }

    public sealed class AlterConfigsResult
    {
        public required ErrorCode ErrorCode { get; init; }
        public string? ErrorMessage { get; init; }
        public required ConfigResourceType ResourceType { get; init; }
        public required string ResourceName { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);

        if (isFlexible)
        {
            writer.WriteVarInt(Results.Count + 1); // COMPACT_ARRAY
            foreach (var result in Results)
            {
                writer.WriteInt16((short)result.ErrorCode);
                writer.WriteCompactString(result.ErrorMessage);
                writer.WriteInt8((sbyte)result.ResourceType);
                writer.WriteCompactString(result.ResourceName);
                writer.WriteVarInt(0); // result tagged fields
            }
        }
        else
        {
            writer.WriteInt32(Results.Count);
            foreach (var result in Results)
            {
                writer.WriteInt16((short)result.ErrorCode);
                writer.WriteString(result.ErrorMessage);
                writer.WriteInt8((sbyte)result.ResourceType);
                writer.WriteString(result.ResourceName);
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // response tagged fields
        }
    }
}
