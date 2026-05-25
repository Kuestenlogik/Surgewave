namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka IncrementalAlterConfigs request (API Key 44, v0-1).
/// Incrementally modifies broker/topic configurations.
/// Supports Set, Delete, Append, and Subtract operations.
/// </summary>
public sealed class IncrementalAlterConfigsRequest : KafkaRequest
{
    /// <summary>The incremental updates for each resource.</summary>
    public required List<AlterConfigsResource> Resources { get; init; }

    /// <summary>True if we should validate the request, but not change the configurations.</summary>
    public bool ValidateOnly { get; init; }

    public sealed class AlterConfigsResource
    {
        /// <summary>
        /// The resource type (0 = UNKNOWN, 2 = TOPIC, 4 = BROKER, 8 = BROKER_LOGGER).
        /// </summary>
        public required sbyte ResourceType { get; init; }

        /// <summary>The resource name.</summary>
        public required string ResourceName { get; init; }

        /// <summary>The configurations to alter.</summary>
        public required List<AlterableConfig> Configs { get; init; }
    }

    public sealed class AlterableConfig
    {
        /// <summary>The configuration key name.</summary>
        public required string Name { get; init; }

        /// <summary>
        /// The type of operation (0 = SET, 1 = DELETE, 2 = APPEND, 3 = SUBTRACT).
        /// </summary>
        public required sbyte ConfigOperation { get; init; }

        /// <summary>The value to set for the configuration key.</summary>
        public string? Value { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 1;

        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteCompactString(ClientId);
            writer.WriteVarInt(0); // Header tagged fields

            // Resources array (compact)
            writer.WriteVarInt(Resources.Count + 1);
            foreach (var resource in Resources)
            {
                writer.WriteInt8(resource.ResourceType);
                writer.WriteCompactString(resource.ResourceName);
                writer.WriteVarInt(resource.Configs.Count + 1);
                foreach (var config in resource.Configs)
                {
                    writer.WriteCompactString(config.Name);
                    writer.WriteInt8(config.ConfigOperation);
                    writer.WriteCompactString(config.Value);
                    writer.WriteVarInt(0); // Config tagged fields
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
                writer.WriteInt8(resource.ResourceType);
                writer.WriteString(resource.ResourceName);
                writer.WriteInt32(resource.Configs.Count);
                foreach (var config in resource.Configs)
                {
                    writer.WriteString(config.Name);
                    writer.WriteInt8(config.ConfigOperation);
                    writer.WriteString(config.Value);
                }
            }

            writer.WriteInt8(ValidateOnly ? (sbyte)1 : (sbyte)0);
        }
    }

    public static IncrementalAlterConfigsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 1;
        var resources = new List<AlterConfigsResource>();

        if (isFlexible)
        {
            var resourceCount = reader.ReadVarInt() - 1;
            for (int i = 0; i < resourceCount; i++)
            {
                var resourceType = reader.ReadInt8();
                var resourceName = reader.ReadCompactString() ?? "";
                var configCount = reader.ReadVarInt() - 1;
                var configs = new List<AlterableConfig>(configCount);

                for (int j = 0; j < configCount; j++)
                {
                    configs.Add(new AlterableConfig
                    {
                        Name = reader.ReadCompactString() ?? "",
                        ConfigOperation = reader.ReadInt8(),
                        Value = reader.ReadCompactString()
                    });
                    reader.SkipTaggedFields();
                }
                reader.SkipTaggedFields();

                resources.Add(new AlterConfigsResource
                {
                    ResourceType = resourceType,
                    ResourceName = resourceName,
                    Configs = configs
                });
            }
        }
        else
        {
            var resourceCount = reader.ReadInt32();
            for (int i = 0; i < resourceCount; i++)
            {
                var resourceType = reader.ReadInt8();
                var resourceName = reader.ReadString() ?? "";
                var configCount = reader.ReadInt32();
                var configs = new List<AlterableConfig>(configCount);

                for (int j = 0; j < configCount; j++)
                {
                    configs.Add(new AlterableConfig
                    {
                        Name = reader.ReadString() ?? "",
                        ConfigOperation = reader.ReadInt8(),
                        Value = reader.ReadString()
                    });
                }

                resources.Add(new AlterConfigsResource
                {
                    ResourceType = resourceType,
                    ResourceName = resourceName,
                    Configs = configs
                });
            }
        }

        var validateOnly = reader.ReadInt8() != 0;

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new IncrementalAlterConfigsRequest
        {
            ApiKey = ApiKey.IncrementalAlterConfigs,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Resources = resources,
            ValidateOnly = validateOnly
        };
    }
}

/// <summary>
/// Kafka IncrementalAlterConfigs response (API Key 44, v0-1).
/// </summary>
public sealed class IncrementalAlterConfigsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The responses for each resource.</summary>
    public required List<AlterConfigsResourceResponse> Responses { get; init; }

    public sealed class AlterConfigsResourceResponse
    {
        /// <summary>The resource error code.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The resource error message, or null if there was no error.</summary>
        public string? ErrorMessage { get; init; }

        /// <summary>The resource type.</summary>
        public required sbyte ResourceType { get; init; }

        /// <summary>The resource name.</summary>
        public required string ResourceName { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 1;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);

        if (isFlexible)
        {
            writer.WriteVarInt(Responses.Count + 1);
            foreach (var response in Responses)
            {
                writer.WriteInt16((short)response.ErrorCode);
                writer.WriteCompactString(response.ErrorMessage);
                writer.WriteInt8(response.ResourceType);
                writer.WriteCompactString(response.ResourceName);
                writer.WriteVarInt(0); // Response tagged fields
            }
            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteInt32(Responses.Count);
            foreach (var response in Responses)
            {
                writer.WriteInt16((short)response.ErrorCode);
                writer.WriteString(response.ErrorMessage);
                writer.WriteInt8(response.ResourceType);
                writer.WriteString(response.ResourceName);
            }
        }
    }

    public static IncrementalAlterConfigsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 1;

        if (isFlexible)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var throttleTimeMs = reader.ReadInt32();
        var responses = new List<AlterConfigsResourceResponse>();

        if (isFlexible)
        {
            var responseCount = reader.ReadVarInt() - 1;
            for (int i = 0; i < responseCount; i++)
            {
                responses.Add(new AlterConfigsResourceResponse
                {
                    ErrorCode = (ErrorCode)reader.ReadInt16(),
                    ErrorMessage = reader.ReadCompactString(),
                    ResourceType = reader.ReadInt8(),
                    ResourceName = reader.ReadCompactString() ?? ""
                });
                reader.SkipTaggedFields();
            }
            reader.SkipTaggedFields();
        }
        else
        {
            var responseCount = reader.ReadInt32();
            for (int i = 0; i < responseCount; i++)
            {
                responses.Add(new AlterConfigsResourceResponse
                {
                    ErrorCode = (ErrorCode)reader.ReadInt16(),
                    ErrorMessage = reader.ReadString(),
                    ResourceType = reader.ReadInt8(),
                    ResourceName = reader.ReadString() ?? ""
                });
            }
        }

        return new IncrementalAlterConfigsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            Responses = responses
        };
    }
}
