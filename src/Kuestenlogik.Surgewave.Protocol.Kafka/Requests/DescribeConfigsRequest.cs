namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DescribeConfigs request (API Key 32)
/// Returns configuration settings for resources like topics and brokers.
/// </summary>
public sealed class DescribeConfigsRequest : KafkaRequest
{
    public required List<ConfigResource> Resources { get; init; }
    public bool IncludeSynonyms { get; init; } // v1+
    public bool IncludeDocumentation { get; init; } // v3+

    public sealed class ConfigResource
    {
        public required ConfigResourceType ResourceType { get; init; }
        public required string ResourceName { get; init; }
        public List<string>? ConfigurationKeys { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteInt32(Resources.Count);
        foreach (var resource in Resources)
        {
            writer.WriteInt8((sbyte)resource.ResourceType);
            writer.WriteString(resource.ResourceName);
            if (resource.ConfigurationKeys == null)
            {
                writer.WriteInt32(-1); // null array
            }
            else
            {
                writer.WriteInt32(resource.ConfigurationKeys.Count);
                foreach (var key in resource.ConfigurationKeys)
                {
                    writer.WriteString(key);
                }
            }
        }
    }

    public static DescribeConfigsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 4;
        var resources = new List<ConfigResource>();

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

                var keyCount = protocolReader.ReadVarInt() - 1;
                List<string>? configKeys = null;
                if (keyCount >= 0)
                {
                    configKeys = new List<string>();
                    for (int j = 0; j < keyCount; j++)
                    {
                        var key = protocolReader.ReadCompactString();
                        if (key != null)
                        {
                            configKeys.Add(key);
                        }
                    }
                }

                protocolReader.ReadVarInt(); // tagged fields

                resources.Add(new ConfigResource
                {
                    ResourceType = resourceType,
                    ResourceName = resourceName,
                    ConfigurationKeys = configKeys
                });
            }

            var includeSynonyms = apiVersion >= 1 && protocolReader.ReadInt8() != 0;
            var includeDocumentation = apiVersion >= 3 && protocolReader.ReadInt8() != 0;
            protocolReader.ReadVarInt(); // tagged fields

            return new DescribeConfigsRequest
            {
                ApiKey = ApiKey.DescribeConfigs,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                Resources = resources,
                IncludeSynonyms = includeSynonyms,
                IncludeDocumentation = includeDocumentation
            };
        }
        else
        {
            var resourceCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < resourceCount; i++)
            {
                var resourceType = (ConfigResourceType)reader.ReadSByte();
                var resourceName = BinaryHelpers.ReadString(reader);

                var keyCount = BinaryHelpers.ReadInt32BigEndian(reader);
                List<string>? configKeys = null;
                if (keyCount >= 0)
                {
                    configKeys = new List<string>();
                    for (int j = 0; j < keyCount; j++)
                    {
                        configKeys.Add(BinaryHelpers.ReadString(reader));
                    }
                }

                resources.Add(new ConfigResource
                {
                    ResourceType = resourceType,
                    ResourceName = resourceName,
                    ConfigurationKeys = configKeys
                });
            }

            var includeSynonyms = apiVersion >= 1 && reader.BaseStream.Position < reader.BaseStream.Length && reader.ReadByte() != 0;
            var includeDocumentation = apiVersion >= 3 && reader.BaseStream.Position < reader.BaseStream.Length && reader.ReadByte() != 0;

            return new DescribeConfigsRequest
            {
                ApiKey = ApiKey.DescribeConfigs,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                Resources = resources,
                IncludeSynonyms = includeSynonyms,
                IncludeDocumentation = includeDocumentation
            };
        }
    }
}

public enum ConfigResourceType : sbyte
{
    Unknown = 0,
    Topic = 2,
    Broker = 4,
    BrokerLogger = 8,
    ClientMetrics = 16,
    /// <summary>
    /// Group-level config resource (KIP-848 / KIP-932 / KIP-1240).
    /// Used by IncrementalAlterConfigs to mutate per-group settings like
    /// <c>share.delivery.count.limit</c>, <c>share.renew.acknowledge.enable</c>.
    /// </summary>
    Group = 32,
}

/// <summary>
/// Kafka DescribeConfigs response
/// </summary>
public sealed class DescribeConfigsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required List<DescribeConfigsResult> Results { get; init; }

    public sealed class DescribeConfigsResult
    {
        public required ErrorCode ErrorCode { get; init; }
        public string? ErrorMessage { get; init; }
        public required ConfigResourceType ResourceType { get; init; }
        public required string ResourceName { get; init; }
        public required List<ConfigEntry> Configs { get; init; }
    }

    public sealed class ConfigEntry
    {
        public required string Name { get; init; }
        public string? Value { get; init; }
        public bool ReadOnly { get; init; }
        public bool IsDefault { get; init; } // v0 only
        public ConfigSource ConfigSource { get; init; } // v1+
        public bool IsSensitive { get; init; }
        public List<ConfigSynonym>? Synonyms { get; init; } // v1+
        public ConfigType ConfigType { get; init; } // v3+
        public string? Documentation { get; init; } // v3+
    }

    public sealed class ConfigSynonym
    {
        public required string Name { get; init; }
        public string? Value { get; init; }
        public required ConfigSource Source { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 4;

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

                writer.WriteVarInt(result.Configs.Count + 1); // COMPACT_ARRAY
                foreach (var config in result.Configs)
                {
                    writer.WriteCompactString(config.Name);
                    writer.WriteCompactString(config.Value);
                    writer.WriteInt8(config.ReadOnly ? (sbyte)1 : (sbyte)0);

                    if (ApiVersion == 0)
                    {
                        writer.WriteInt8(config.IsDefault ? (sbyte)1 : (sbyte)0);
                    }
                    else
                    {
                        writer.WriteInt8((sbyte)config.ConfigSource);
                    }

                    writer.WriteInt8(config.IsSensitive ? (sbyte)1 : (sbyte)0);

                    // Synonyms (v1+)
                    if (ApiVersion >= 1)
                    {
                        if (config.Synonyms == null || config.Synonyms.Count == 0)
                        {
                            writer.WriteVarInt(1); // empty COMPACT_ARRAY
                        }
                        else
                        {
                            writer.WriteVarInt(config.Synonyms.Count + 1);
                            foreach (var synonym in config.Synonyms)
                            {
                                writer.WriteCompactString(synonym.Name);
                                writer.WriteCompactString(synonym.Value);
                                writer.WriteInt8((sbyte)synonym.Source);
                                writer.WriteVarInt(0); // tagged fields
                            }
                        }
                    }

                    // ConfigType and Documentation (v3+)
                    if (ApiVersion >= 3)
                    {
                        writer.WriteInt8((sbyte)config.ConfigType);
                        writer.WriteCompactString(config.Documentation);
                    }

                    writer.WriteVarInt(0); // config tagged fields
                }

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

                writer.WriteInt32(result.Configs.Count);
                foreach (var config in result.Configs)
                {
                    writer.WriteString(config.Name);
                    writer.WriteString(config.Value);
                    writer.WriteInt8(config.ReadOnly ? (sbyte)1 : (sbyte)0);

                    if (ApiVersion == 0)
                    {
                        writer.WriteInt8(config.IsDefault ? (sbyte)1 : (sbyte)0);
                    }
                    else
                    {
                        writer.WriteInt8((sbyte)config.ConfigSource);
                    }

                    writer.WriteInt8(config.IsSensitive ? (sbyte)1 : (sbyte)0);

                    // Synonyms (v1+)
                    if (ApiVersion >= 1)
                    {
                        if (config.Synonyms == null)
                        {
                            writer.WriteInt32(0);
                        }
                        else
                        {
                            writer.WriteInt32(config.Synonyms.Count);
                            foreach (var synonym in config.Synonyms)
                            {
                                writer.WriteString(synonym.Name);
                                writer.WriteString(synonym.Value);
                                writer.WriteInt8((sbyte)synonym.Source);
                            }
                        }
                    }

                    // ConfigType and Documentation (v3+)
                    if (ApiVersion >= 3)
                    {
                        writer.WriteInt8((sbyte)config.ConfigType);
                        writer.WriteString(config.Documentation);
                    }
                }
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // response tagged fields
        }
    }
}

public enum ConfigSource : sbyte
{
    Unknown = 0,
    DynamicTopicConfig = 1,
    DynamicBrokerConfig = 2,
    DynamicDefaultBrokerConfig = 3,
    StaticBrokerConfig = 4,
    DefaultConfig = 5,
    DynamicBrokerLoggerConfig = 6
}

public enum ConfigType : sbyte
{
    Unknown = 0,
    Boolean = 1,
    String = 2,
    Int = 3,
    Short = 4,
    Long = 5,
    Double = 6,
    List = 7,
    Class = 8,
    Password = 9
}
