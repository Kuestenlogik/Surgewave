namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DeleteGroups request (API Key 42, v0-2).
/// Deletes consumer groups from the cluster.
/// Groups must be empty (no active members) to be deleted.
/// </summary>
public sealed class DeleteGroupsRequest : KafkaRequest
{
    /// <summary>The group IDs to delete.</summary>
    public required List<string> GroupIds { get; init; }

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

            // GroupIds array (compact)
            writer.WriteVarInt(GroupIds.Count + 1);
            foreach (var groupId in GroupIds)
            {
                writer.WriteCompactString(groupId);
            }

            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteString(ClientId);

            writer.WriteInt32(GroupIds.Count);
            foreach (var groupId in GroupIds)
            {
                writer.WriteString(groupId);
            }
        }
    }

    public static DeleteGroupsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;
        var groupIds = new List<string>();

        if (isFlexible)
        {
            var groupCount = reader.ReadVarInt() - 1;
            for (int i = 0; i < groupCount; i++)
            {
                groupIds.Add(reader.ReadCompactString() ?? "");
            }
            reader.SkipTaggedFields();
        }
        else
        {
            var groupCount = reader.ReadInt32();
            for (int i = 0; i < groupCount; i++)
            {
                groupIds.Add(reader.ReadString() ?? "");
            }
        }

        return new DeleteGroupsRequest
        {
            ApiKey = ApiKey.DeleteGroups,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupIds = groupIds
        };
    }
}

/// <summary>
/// Kafka DeleteGroups response (API Key 42, v0-2).
/// </summary>
public sealed class DeleteGroupsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The deletion results for each group.</summary>
    public required List<DeletableGroupResult> Results { get; init; }

    public sealed class DeletableGroupResult
    {
        /// <summary>The group ID.</summary>
        public required string GroupId { get; init; }

        /// <summary>The error code, or 0 if the deletion succeeded.</summary>
        public ErrorCode ErrorCode { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);

        if (isFlexible)
        {
            // Results array (compact)
            writer.WriteVarInt(Results.Count + 1);
            foreach (var result in Results)
            {
                writer.WriteCompactString(result.GroupId);
                writer.WriteInt16((short)result.ErrorCode);
                writer.WriteVarInt(0); // Result tagged fields
            }

            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteInt32(Results.Count);
            foreach (var result in Results)
            {
                writer.WriteString(result.GroupId);
                writer.WriteInt16((short)result.ErrorCode);
            }
        }
    }

    public static DeleteGroupsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 2;

        if (isFlexible)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var throttleTimeMs = reader.ReadInt32();
        var results = new List<DeletableGroupResult>();

        if (isFlexible)
        {
            var resultCount = reader.ReadVarInt() - 1;
            for (int i = 0; i < resultCount; i++)
            {
                var groupId = reader.ReadCompactString() ?? "";
                var errorCode = (ErrorCode)reader.ReadInt16();
                reader.SkipTaggedFields();

                results.Add(new DeletableGroupResult
                {
                    GroupId = groupId,
                    ErrorCode = errorCode
                });
            }
            reader.SkipTaggedFields();
        }
        else
        {
            var resultCount = reader.ReadInt32();
            for (int i = 0; i < resultCount; i++)
            {
                var groupId = reader.ReadString() ?? "";
                var errorCode = (ErrorCode)reader.ReadInt16();

                results.Add(new DeletableGroupResult
                {
                    GroupId = groupId,
                    ErrorCode = errorCode
                });
            }
        }

        return new DeleteGroupsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            Results = results
        };
    }
}
