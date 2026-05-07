namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DescribeClientQuotas request (API Key 48, v0-1).
/// Describes client quota configurations.
/// </summary>
public sealed class DescribeClientQuotasRequest : KafkaRequest
{
    /// <summary>Filter components to apply to the quota query.</summary>
    public required List<ComponentData> Components { get; init; }

    /// <summary>Whether the match is strict (exact) or not.</summary>
    public bool Strict { get; init; }

    public sealed class ComponentData
    {
        /// <summary>The entity type that the filter component applies to.</summary>
        public required string EntityType { get; init; }

        /// <summary>
        /// The match type of the filter component.
        /// 0 = exact match, 1 = match by default, 2 = match any.
        /// </summary>
        public required sbyte MatchType { get; init; }

        /// <summary>The name of the entity to match (null for default/any match).</summary>
        public string? Match { get; init; }
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

            writer.WriteVarInt(Components.Count + 1);
            foreach (var component in Components)
            {
                writer.WriteCompactString(component.EntityType);
                writer.WriteInt8(component.MatchType);
                writer.WriteCompactString(component.Match);
                writer.WriteVarInt(0); // Component tagged fields
            }

            writer.WriteInt8(Strict ? (sbyte)1 : (sbyte)0);
            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteString(ClientId);

            writer.WriteInt32(Components.Count);
            foreach (var component in Components)
            {
                writer.WriteString(component.EntityType);
                writer.WriteInt8(component.MatchType);
                writer.WriteString(component.Match);
            }

            writer.WriteInt8(Strict ? (sbyte)1 : (sbyte)0);
        }
    }

    public static DescribeClientQuotasRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 1;
        var components = new List<ComponentData>();

        if (isFlexible)
        {
            var componentCount = reader.ReadVarInt() - 1;
            for (int i = 0; i < componentCount; i++)
            {
                components.Add(new ComponentData
                {
                    EntityType = reader.ReadCompactString() ?? "",
                    MatchType = reader.ReadInt8(),
                    Match = reader.ReadCompactString()
                });
                reader.SkipTaggedFields();
            }
        }
        else
        {
            var componentCount = reader.ReadInt32();
            for (int i = 0; i < componentCount; i++)
            {
                components.Add(new ComponentData
                {
                    EntityType = reader.ReadString() ?? "",
                    MatchType = reader.ReadInt8(),
                    Match = reader.ReadString()
                });
            }
        }

        var strict = reader.ReadInt8() != 0;

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new DescribeClientQuotasRequest
        {
            ApiKey = ApiKey.DescribeClientQuotas,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Components = components,
            Strict = strict
        };
    }
}

/// <summary>
/// Kafka DescribeClientQuotas response (API Key 48, v0-1).
/// </summary>
public sealed class DescribeClientQuotasResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if the quota description succeeded.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>A result entry.</summary>
    public string? ErrorMessage { get; init; }

    /// <summary>A result entry.</summary>
    public required List<EntryData> Entries { get; init; }

    public sealed class EntryData
    {
        /// <summary>The quota entity description.</summary>
        public required List<EntityData> Entity { get; init; }

        /// <summary>The quota values for the entity.</summary>
        public required List<ValueData> Values { get; init; }
    }

    public sealed class EntityData
    {
        /// <summary>The entity type.</summary>
        public required string EntityType { get; init; }

        /// <summary>The entity name, or null if the default.</summary>
        public string? EntityName { get; init; }
    }

    public sealed class ValueData
    {
        /// <summary>The quota configuration key.</summary>
        public required string Key { get; init; }

        /// <summary>The quota configuration value.</summary>
        public double Value { get; init; }
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
        writer.WriteInt16((short)ErrorCode);

        if (isFlexible)
        {
            writer.WriteCompactString(ErrorMessage);

            writer.WriteVarInt(Entries.Count + 1);
            foreach (var entry in Entries)
            {
                writer.WriteVarInt(entry.Entity.Count + 1);
                foreach (var entity in entry.Entity)
                {
                    writer.WriteCompactString(entity.EntityType);
                    writer.WriteCompactString(entity.EntityName);
                    writer.WriteVarInt(0); // Entity tagged fields
                }

                writer.WriteVarInt(entry.Values.Count + 1);
                foreach (var value in entry.Values)
                {
                    writer.WriteCompactString(value.Key);
                    writer.WriteFloat64(value.Value);
                    writer.WriteVarInt(0); // Value tagged fields
                }

                writer.WriteVarInt(0); // Entry tagged fields
            }

            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteString(ErrorMessage);

            writer.WriteInt32(Entries.Count);
            foreach (var entry in Entries)
            {
                writer.WriteInt32(entry.Entity.Count);
                foreach (var entity in entry.Entity)
                {
                    writer.WriteString(entity.EntityType);
                    writer.WriteString(entity.EntityName);
                }

                writer.WriteInt32(entry.Values.Count);
                foreach (var value in entry.Values)
                {
                    writer.WriteString(value.Key);
                    writer.WriteFloat64(value.Value);
                }
            }
        }
    }

    public static DescribeClientQuotasResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 1;

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();

        string? errorMessage;
        var entries = new List<EntryData>();

        if (isFlexible)
        {
            errorMessage = reader.ReadCompactString();

            var entryCount = reader.ReadVarInt() - 1;
            for (int i = 0; i < entryCount; i++)
            {
                var entityCount = reader.ReadVarInt() - 1;
                var entity = new List<EntityData>(entityCount);
                for (int j = 0; j < entityCount; j++)
                {
                    entity.Add(new EntityData
                    {
                        EntityType = reader.ReadCompactString() ?? "",
                        EntityName = reader.ReadCompactString()
                    });
                    reader.SkipTaggedFields();
                }

                var valueCount = reader.ReadVarInt() - 1;
                var values = new List<ValueData>(valueCount);
                for (int k = 0; k < valueCount; k++)
                {
                    values.Add(new ValueData
                    {
                        Key = reader.ReadCompactString() ?? "",
                        Value = reader.ReadFloat64()
                    });
                    reader.SkipTaggedFields();
                }

                reader.SkipTaggedFields();

                entries.Add(new EntryData
                {
                    Entity = entity,
                    Values = values
                });
            }

            reader.SkipTaggedFields();
        }
        else
        {
            errorMessage = reader.ReadString();

            var entryCount = reader.ReadInt32();
            for (int i = 0; i < entryCount; i++)
            {
                var entityCount = reader.ReadInt32();
                var entity = new List<EntityData>(entityCount);
                for (int j = 0; j < entityCount; j++)
                {
                    entity.Add(new EntityData
                    {
                        EntityType = reader.ReadString() ?? "",
                        EntityName = reader.ReadString()
                    });
                }

                var valueCount = reader.ReadInt32();
                var values = new List<ValueData>(valueCount);
                for (int k = 0; k < valueCount; k++)
                {
                    values.Add(new ValueData
                    {
                        Key = reader.ReadString() ?? "",
                        Value = reader.ReadFloat64()
                    });
                }

                entries.Add(new EntryData
                {
                    Entity = entity,
                    Values = values
                });
            }
        }

        return new DescribeClientQuotasResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            Entries = entries
        };
    }
}

/// <summary>
/// Kafka AlterClientQuotas request (API Key 49, v0-1).
/// Alters client quota configurations.
/// </summary>
public sealed class AlterClientQuotasRequest : KafkaRequest
{
    /// <summary>The quota alterations to perform.</summary>
    public required List<EntryData> Entries { get; init; }

    /// <summary>Whether the alteration should be validated, but not performed.</summary>
    public bool ValidateOnly { get; init; }

    public sealed class EntryData
    {
        /// <summary>The quota entity to alter.</summary>
        public required List<EntityData> Entity { get; init; }

        /// <summary>An individual quota configuration entry to alter.</summary>
        public required List<OpData> Ops { get; init; }
    }

    public sealed class EntityData
    {
        /// <summary>The entity type.</summary>
        public required string EntityType { get; init; }

        /// <summary>The name of the entity, or null if the default.</summary>
        public string? EntityName { get; init; }
    }

    public sealed class OpData
    {
        /// <summary>The quota configuration key.</summary>
        public required string Key { get; init; }

        /// <summary>The value to set (0 = SET, 1 = REMOVE).</summary>
        public double Value { get; init; }

        /// <summary>Whether to remove the quota configuration (true = remove).</summary>
        public bool Remove { get; init; }
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

            writer.WriteVarInt(Entries.Count + 1);
            foreach (var entry in Entries)
            {
                writer.WriteVarInt(entry.Entity.Count + 1);
                foreach (var entity in entry.Entity)
                {
                    writer.WriteCompactString(entity.EntityType);
                    writer.WriteCompactString(entity.EntityName);
                    writer.WriteVarInt(0); // Entity tagged fields
                }

                writer.WriteVarInt(entry.Ops.Count + 1);
                foreach (var op in entry.Ops)
                {
                    writer.WriteCompactString(op.Key);
                    writer.WriteFloat64(op.Value);
                    writer.WriteInt8(op.Remove ? (sbyte)1 : (sbyte)0);
                    writer.WriteVarInt(0); // Op tagged fields
                }

                writer.WriteVarInt(0); // Entry tagged fields
            }

            writer.WriteInt8(ValidateOnly ? (sbyte)1 : (sbyte)0);
            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteString(ClientId);

            writer.WriteInt32(Entries.Count);
            foreach (var entry in Entries)
            {
                writer.WriteInt32(entry.Entity.Count);
                foreach (var entity in entry.Entity)
                {
                    writer.WriteString(entity.EntityType);
                    writer.WriteString(entity.EntityName);
                }

                writer.WriteInt32(entry.Ops.Count);
                foreach (var op in entry.Ops)
                {
                    writer.WriteString(op.Key);
                    writer.WriteFloat64(op.Value);
                    writer.WriteInt8(op.Remove ? (sbyte)1 : (sbyte)0);
                }
            }

            writer.WriteInt8(ValidateOnly ? (sbyte)1 : (sbyte)0);
        }
    }

    public static AlterClientQuotasRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 1;
        var entries = new List<EntryData>();

        if (isFlexible)
        {
            var entryCount = reader.ReadVarInt() - 1;
            for (int i = 0; i < entryCount; i++)
            {
                var entityCount = reader.ReadVarInt() - 1;
                var entity = new List<EntityData>(entityCount);
                for (int j = 0; j < entityCount; j++)
                {
                    entity.Add(new EntityData
                    {
                        EntityType = reader.ReadCompactString() ?? "",
                        EntityName = reader.ReadCompactString()
                    });
                    reader.SkipTaggedFields();
                }

                var opCount = reader.ReadVarInt() - 1;
                var ops = new List<OpData>(opCount);
                for (int k = 0; k < opCount; k++)
                {
                    ops.Add(new OpData
                    {
                        Key = reader.ReadCompactString() ?? "",
                        Value = reader.ReadFloat64(),
                        Remove = reader.ReadInt8() != 0
                    });
                    reader.SkipTaggedFields();
                }

                reader.SkipTaggedFields();

                entries.Add(new EntryData
                {
                    Entity = entity,
                    Ops = ops
                });
            }
        }
        else
        {
            var entryCount = reader.ReadInt32();
            for (int i = 0; i < entryCount; i++)
            {
                var entityCount = reader.ReadInt32();
                var entity = new List<EntityData>(entityCount);
                for (int j = 0; j < entityCount; j++)
                {
                    entity.Add(new EntityData
                    {
                        EntityType = reader.ReadString() ?? "",
                        EntityName = reader.ReadString()
                    });
                }

                var opCount = reader.ReadInt32();
                var ops = new List<OpData>(opCount);
                for (int k = 0; k < opCount; k++)
                {
                    ops.Add(new OpData
                    {
                        Key = reader.ReadString() ?? "",
                        Value = reader.ReadFloat64(),
                        Remove = reader.ReadInt8() != 0
                    });
                }

                entries.Add(new EntryData
                {
                    Entity = entity,
                    Ops = ops
                });
            }
        }

        var validateOnly = reader.ReadInt8() != 0;

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new AlterClientQuotasRequest
        {
            ApiKey = ApiKey.AlterClientQuotas,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Entries = entries,
            ValidateOnly = validateOnly
        };
    }
}

/// <summary>
/// Kafka AlterClientQuotas response (API Key 49, v0-1).
/// </summary>
public sealed class AlterClientQuotasResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The results for each entry.</summary>
    public required List<EntryData> Entries { get; init; }

    public sealed class EntryData
    {
        /// <summary>The error code.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The error message.</summary>
        public string? ErrorMessage { get; init; }

        /// <summary>The altered quota entity.</summary>
        public required List<EntityData> Entity { get; init; }
    }

    public sealed class EntityData
    {
        /// <summary>The entity type.</summary>
        public required string EntityType { get; init; }

        /// <summary>The entity name, or null if the default.</summary>
        public string? EntityName { get; init; }
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
            writer.WriteVarInt(Entries.Count + 1);
            foreach (var entry in Entries)
            {
                writer.WriteInt16((short)entry.ErrorCode);
                writer.WriteCompactString(entry.ErrorMessage);

                writer.WriteVarInt(entry.Entity.Count + 1);
                foreach (var entity in entry.Entity)
                {
                    writer.WriteCompactString(entity.EntityType);
                    writer.WriteCompactString(entity.EntityName);
                    writer.WriteVarInt(0); // Entity tagged fields
                }

                writer.WriteVarInt(0); // Entry tagged fields
            }

            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteInt32(Entries.Count);
            foreach (var entry in Entries)
            {
                writer.WriteInt16((short)entry.ErrorCode);
                writer.WriteString(entry.ErrorMessage);

                writer.WriteInt32(entry.Entity.Count);
                foreach (var entity in entry.Entity)
                {
                    writer.WriteString(entity.EntityType);
                    writer.WriteString(entity.EntityName);
                }
            }
        }
    }

    public static AlterClientQuotasResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 1;

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        var throttleTimeMs = reader.ReadInt32();
        var entries = new List<EntryData>();

        if (isFlexible)
        {
            var entryCount = reader.ReadVarInt() - 1;
            for (int i = 0; i < entryCount; i++)
            {
                var errorCode = (ErrorCode)reader.ReadInt16();
                var errorMessage = reader.ReadCompactString();

                var entityCount = reader.ReadVarInt() - 1;
                var entity = new List<EntityData>(entityCount);
                for (int j = 0; j < entityCount; j++)
                {
                    entity.Add(new EntityData
                    {
                        EntityType = reader.ReadCompactString() ?? "",
                        EntityName = reader.ReadCompactString()
                    });
                    reader.SkipTaggedFields();
                }

                reader.SkipTaggedFields();

                entries.Add(new EntryData
                {
                    ErrorCode = errorCode,
                    ErrorMessage = errorMessage,
                    Entity = entity
                });
            }

            reader.SkipTaggedFields();
        }
        else
        {
            var entryCount = reader.ReadInt32();
            for (int i = 0; i < entryCount; i++)
            {
                var errorCode = (ErrorCode)reader.ReadInt16();
                var errorMessage = reader.ReadString();

                var entityCount = reader.ReadInt32();
                var entity = new List<EntityData>(entityCount);
                for (int j = 0; j < entityCount; j++)
                {
                    entity.Add(new EntityData
                    {
                        EntityType = reader.ReadString() ?? "",
                        EntityName = reader.ReadString()
                    });
                }

                entries.Add(new EntryData
                {
                    ErrorCode = errorCode,
                    ErrorMessage = errorMessage,
                    Entity = entity
                });
            }
        }

        return new AlterClientQuotasResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            Entries = entries
        };
    }
}
