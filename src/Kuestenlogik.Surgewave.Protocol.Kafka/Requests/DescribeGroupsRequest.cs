namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DescribeGroups request (API Key 15)
/// Returns details about consumer groups including members and their assignments.
/// </summary>
public sealed class DescribeGroupsRequest : KafkaRequest
{
    public required List<string> GroupIds { get; init; }
    public bool IncludeAuthorizedOperations { get; init; } // v3+

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteInt32(GroupIds.Count);
        foreach (var groupId in GroupIds)
        {
            writer.WriteString(groupId);
        }
    }

    public static DescribeGroupsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 5;
        var groupIds = new List<string>();

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            var groupCount = protocolReader.ReadVarInt() - 1;
            for (int i = 0; i < groupCount; i++)
            {
                var groupId = protocolReader.ReadCompactString();
                if (groupId != null)
                {
                    groupIds.Add(groupId);
                }
            }

            var includeAuthorizedOperations = apiVersion >= 3 && protocolReader.ReadInt8() != 0;
            protocolReader.ReadVarInt(); // tagged fields

            return new DescribeGroupsRequest
            {
                ApiKey = ApiKey.DescribeGroups,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                GroupIds = groupIds,
                IncludeAuthorizedOperations = includeAuthorizedOperations
            };
        }
        else
        {
            var groupCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < groupCount; i++)
            {
                groupIds.Add(BinaryHelpers.ReadString(reader));
            }

            var includeAuthorizedOperations = apiVersion >= 3 && reader.BaseStream.Position < reader.BaseStream.Length && reader.ReadByte() != 0;

            return new DescribeGroupsRequest
            {
                ApiKey = ApiKey.DescribeGroups,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                GroupIds = groupIds,
                IncludeAuthorizedOperations = includeAuthorizedOperations
            };
        }
    }
}

/// <summary>
/// Kafka DescribeGroups response
/// </summary>
public sealed class DescribeGroupsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; } // v1+
    public required List<DescribedGroup> Groups { get; init; }

    public sealed class DescribedGroup
    {
        public required ErrorCode ErrorCode { get; init; }
        public required string GroupId { get; init; }
        public required string GroupState { get; init; }
        public required string ProtocolType { get; init; }
        public required string ProtocolData { get; init; }
        public required List<GroupMember> Members { get; init; }
        public int AuthorizedOperations { get; init; } // v3+
    }

    public sealed class GroupMember
    {
        public required string MemberId { get; init; }
        public string? GroupInstanceId { get; init; } // v4+
        public required string ClientId { get; init; }
        public required string ClientHost { get; init; }
        public required byte[] MemberMetadata { get; init; }
        public required byte[] MemberAssignment { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 5;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // header tagged fields
        }

        // ThrottleTimeMs (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }

        if (isFlexible)
        {
            writer.WriteVarInt(Groups.Count + 1); // COMPACT_ARRAY
            foreach (var group in Groups)
            {
                writer.WriteInt16((short)group.ErrorCode);
                writer.WriteCompactString(group.GroupId);
                writer.WriteCompactString(group.GroupState);
                writer.WriteCompactString(group.ProtocolType);
                writer.WriteCompactString(group.ProtocolData);

                writer.WriteVarInt(group.Members.Count + 1); // COMPACT_ARRAY
                foreach (var member in group.Members)
                {
                    writer.WriteCompactString(member.MemberId);
                    if (ApiVersion >= 4)
                    {
                        writer.WriteCompactString(member.GroupInstanceId);
                    }
                    writer.WriteCompactString(member.ClientId);
                    writer.WriteCompactString(member.ClientHost);
                    writer.WriteCompactBytes(member.MemberMetadata);
                    writer.WriteCompactBytes(member.MemberAssignment);
                    writer.WriteVarInt(0); // member tagged fields
                }

                if (ApiVersion >= 3)
                {
                    writer.WriteInt32(group.AuthorizedOperations);
                }
                writer.WriteVarInt(0); // group tagged fields
            }
        }
        else
        {
            writer.WriteInt32(Groups.Count);
            foreach (var group in Groups)
            {
                writer.WriteInt16((short)group.ErrorCode);
                writer.WriteString(group.GroupId);
                writer.WriteString(group.GroupState);
                writer.WriteString(group.ProtocolType);
                writer.WriteString(group.ProtocolData);

                writer.WriteInt32(group.Members.Count);
                foreach (var member in group.Members)
                {
                    writer.WriteString(member.MemberId);
                    if (ApiVersion >= 4)
                    {
                        writer.WriteString(member.GroupInstanceId);
                    }
                    writer.WriteString(member.ClientId);
                    writer.WriteString(member.ClientHost);
                    writer.WriteBytes(member.MemberMetadata);
                    writer.WriteBytes(member.MemberAssignment);
                }

                if (ApiVersion >= 3)
                {
                    writer.WriteInt32(group.AuthorizedOperations);
                }
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // response tagged fields
        }
    }
}
