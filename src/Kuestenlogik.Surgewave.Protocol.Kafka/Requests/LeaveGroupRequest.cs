namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// LeaveGroup request - Leave a consumer group
/// </summary>
public sealed class LeaveGroupRequest : KafkaRequest
{
    public required string GroupId { get; init; }
    public required string MemberId { get; init; }
    public MemberIdentity[] Members { get; init; } = [];

    public sealed class MemberIdentity
    {
        public required string MemberId { get; init; }
        public string? GroupInstanceId { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteString(GroupId);
        writer.WriteString(MemberId);

        if (ApiVersion >= 3)
        {
            writer.WriteArray(Members, member =>
            {
                writer.WriteString(member.MemberId);
                if (ApiVersion >= 3)
                {
                    writer.WriteString(member.GroupInstanceId);
                }
            });
        }
    }

    public static LeaveGroupRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.LeaveGroup, apiVersion);

        var groupId = isFlexible ? reader.ReadCompactString() : reader.ReadString();
        string? memberId = null;
        MemberIdentity[] members = [];

        if (apiVersion >= 3)
        {
            // V3+ uses members array instead of single member
            if (isFlexible)
            {
                members = reader.ReadCompactArray(() =>
                {
                    var mId = reader.ReadCompactString();
                    var groupInstanceId = reader.ReadCompactString();

                    // Skip tagged fields
                    var tagCount = reader.ReadVarInt();
                    for (int j = 0; j < tagCount; j++)
                    {
                        var tag = reader.ReadVarInt();
                        var size = reader.ReadVarInt();
                        reader.Skip(size);
                    }

                    return new MemberIdentity
                    {
                        MemberId = mId ?? string.Empty,
                        GroupInstanceId = groupInstanceId
                    };
                });
            }
            else
            {
                members = reader.ReadArray(() =>
                {
                    var mId = reader.ReadString();
                    var groupInstanceId = reader.ReadString();
                    return new MemberIdentity
                    {
                        MemberId = mId ?? string.Empty,
                        GroupInstanceId = groupInstanceId
                    };
                });
            }
        }
        else
        {
            // V0-2 uses single member_id field
            memberId = isFlexible ? reader.ReadCompactString() : reader.ReadString();
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

        return new LeaveGroupRequest
        {
            ApiKey = ApiKey.LeaveGroup,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId ?? string.Empty,
            MemberId = memberId ?? string.Empty,
            Members = members
        };
    }
}

/// <summary>
/// LeaveGroup response
/// </summary>
public sealed class LeaveGroupResponse : KafkaResponse
{
    public required ErrorCode ErrorCode { get; init; }
    public MemberResponse[] Members { get; init; } = [];

    public sealed class MemberResponse
    {
        public required string MemberId { get; init; }
        public string? GroupInstanceId { get; init; }
        public required ErrorCode ErrorCode { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.LeaveGroup, ApiVersion);

        // Response header
        writer.WriteInt32(CorrelationId);
        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        if (ApiVersion >= 1)
        {
            writer.WriteInt32(0); // ThrottleTimeMs
        }

        writer.WriteInt16((short)ErrorCode);

        if (ApiVersion >= 3)
        {
            // V3+ includes members array with per-member error codes
            if (isFlexible)
            {
                writer.WriteVarInt(Members.Length + 1);
                foreach (var member in Members)
                {
                    writer.WriteCompactString(member.MemberId);
                    writer.WriteCompactString(member.GroupInstanceId);
                    writer.WriteInt16((short)member.ErrorCode);
                    writer.WriteVarInt(0); // Tagged fields
                }
            }
            else
            {
                writer.WriteArray(Members, member =>
                {
                    writer.WriteString(member.MemberId);
                    writer.WriteString(member.GroupInstanceId);
                    writer.WriteInt16((short)member.ErrorCode);
                });
            }
        }

        // Tagged fields for flexible versions
        if (isFlexible)
        {
            writer.WriteVarInt(0);
        }
    }
}
