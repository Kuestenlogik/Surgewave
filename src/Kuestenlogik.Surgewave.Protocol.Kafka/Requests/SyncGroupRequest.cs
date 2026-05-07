namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// SyncGroup request - Synchronize group state for a member
/// </summary>
public sealed class SyncGroupRequest : KafkaRequest
{
    public required string GroupId { get; init; }
    public required int GenerationId { get; init; }
    public required string MemberId { get; init; }
    public string? GroupInstanceId { get; init; }
    public required GroupAssignment[] Assignments { get; init; }

    public sealed class GroupAssignment
    {
        public required string MemberId { get; init; }
        public required byte[] Assignment { get; init; }
    }

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

        writer.WriteArray(Assignments, assignment =>
        {
            writer.WriteString(assignment.MemberId);
            writer.WriteBytes(assignment.Assignment);
        });
    }

    public static SyncGroupRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        // Version 4+ uses flexible format (compact strings/arrays)
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.SyncGroup, apiVersion);

        var groupId = isFlexible ? reader.ReadCompactString() : reader.ReadString();
        var generationId = reader.ReadInt32();
        var memberId = isFlexible ? reader.ReadCompactString() : reader.ReadString();

        string? groupInstanceId = apiVersion >= 3
            ? (isFlexible ? reader.ReadCompactString() : reader.ReadString())
            : null;

        GroupAssignment[] assignments;
        if (isFlexible)
        {
            // Compact array: length+1 as varint, then elements
            assignments = reader.ReadCompactArray(() =>
            {
                var assignmentMemberId = reader.ReadCompactString();
                var assignment = reader.ReadCompactBytes();

                // Skip tagged fields for each assignment
                var assignmentTags = reader.ReadVarInt();
                for (int j = 0; j < assignmentTags; j++)
                {
                    var tag = reader.ReadVarInt();
                    var size = reader.ReadVarInt();
                    reader.Skip(size);
                }

                return new GroupAssignment
                {
                    MemberId = assignmentMemberId ?? string.Empty,
                    Assignment = assignment ?? Array.Empty<byte>()
                };
            });
        }
        else
        {
            // Regular array: length as int32, then elements
            assignments = reader.ReadArray(() =>
            {
                var assignmentMemberId = reader.ReadString();
                var assignment = reader.ReadBytes();
                return new GroupAssignment
                {
                    MemberId = assignmentMemberId ?? string.Empty,
                    Assignment = assignment ?? Array.Empty<byte>()
                };
            });
        }

        // Skip tagged fields for flexible versions
        if (isFlexible)
        {
            var taggedFieldCount = reader.ReadVarInt();
            for (int i = 0; i < taggedFieldCount; i++)
            {
                var tag = reader.ReadVarInt();
                var size = reader.ReadVarInt();
                reader.Skip(size);
            }
        }

        return new SyncGroupRequest
        {
            ApiKey = ApiKey.SyncGroup,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId ?? string.Empty,
            GenerationId = generationId,
            MemberId = memberId ?? string.Empty,
            GroupInstanceId = groupInstanceId,
            Assignments = assignments
        };
    }
}

/// <summary>
/// SyncGroup response
/// </summary>
public sealed class SyncGroupResponse : KafkaResponse
{
    public required ErrorCode ErrorCode { get; init; }
    public required byte[] Assignment { get; init; }

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
        // Version 0: error_code, assignment
        // Version 1+: throttle_time_ms, error_code, assignment
        // Version 4+: Flexible format with compact bytes and tagged fields

        if (ApiVersion >= 1)
        {
            writer.WriteInt32(0); // ThrottleTimeMs
        }

        writer.WriteInt16((short)ErrorCode);

        // Assignment - use compact bytes for v4+
        if (isFlexible)
        {
            writer.WriteCompactBytes(Assignment);
        }
        else
        {
            writer.WriteBytes(Assignment);
        }

        // Body tagged fields for flexible versions (v4+)
        if (isFlexible)
        {
            writer.WriteVarInt(0); // No body tagged fields
        }
    }
}
