namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DescribeAcls request (API Key 29)
/// Lists ACLs matching the specified filter.
/// </summary>
public sealed class DescribeAclsRequest : KafkaRequest
{
    public required AclResourceTypeFilter ResourceTypeFilter { get; init; }
    public string? ResourceNameFilter { get; init; }
    public required AclPatternTypeFilter PatternTypeFilter { get; init; }
    public string? PrincipalFilter { get; init; }
    public string? HostFilter { get; init; }
    public required AclOperationFilter OperationFilter { get; init; }
    public required AclPermissionTypeFilter PermissionTypeFilter { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteInt8((sbyte)ResourceTypeFilter);
        writer.WriteString(ResourceNameFilter);
        writer.WriteInt8((sbyte)PatternTypeFilter);
        writer.WriteString(PrincipalFilter);
        writer.WriteString(HostFilter);
        writer.WriteInt8((sbyte)OperationFilter);
        writer.WriteInt8((sbyte)PermissionTypeFilter);
    }

    public static DescribeAclsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            var resourceTypeFilter = (AclResourceTypeFilter)protocolReader.ReadInt8();
            var resourceNameFilter = protocolReader.ReadCompactString();
            var patternTypeFilter = (AclPatternTypeFilter)protocolReader.ReadInt8();
            var principalFilter = protocolReader.ReadCompactString();
            var hostFilter = protocolReader.ReadCompactString();
            var operationFilter = (AclOperationFilter)protocolReader.ReadInt8();
            var permissionTypeFilter = (AclPermissionTypeFilter)protocolReader.ReadInt8();
            protocolReader.ReadVarInt(); // tagged fields

            return new DescribeAclsRequest
            {
                ApiKey = ApiKey.DescribeAcls,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                ResourceTypeFilter = resourceTypeFilter,
                ResourceNameFilter = resourceNameFilter,
                PatternTypeFilter = patternTypeFilter,
                PrincipalFilter = principalFilter,
                HostFilter = hostFilter,
                OperationFilter = operationFilter,
                PermissionTypeFilter = permissionTypeFilter
            };
        }
        else
        {
            var resourceTypeFilter = (AclResourceTypeFilter)reader.ReadByte();
            var resourceNameFilter = BinaryHelpers.ReadNullableString(reader);
            var patternTypeFilter = apiVersion >= 1 ? (AclPatternTypeFilter)reader.ReadByte() : AclPatternTypeFilter.Literal;
            var principalFilter = BinaryHelpers.ReadNullableString(reader);
            var hostFilter = BinaryHelpers.ReadNullableString(reader);
            var operationFilter = (AclOperationFilter)reader.ReadByte();
            var permissionTypeFilter = (AclPermissionTypeFilter)reader.ReadByte();

            return new DescribeAclsRequest
            {
                ApiKey = ApiKey.DescribeAcls,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                ResourceTypeFilter = resourceTypeFilter,
                ResourceNameFilter = resourceNameFilter,
                PatternTypeFilter = patternTypeFilter,
                PrincipalFilter = principalFilter,
                HostFilter = hostFilter,
                OperationFilter = operationFilter,
                PermissionTypeFilter = permissionTypeFilter
            };
        }
    }
}

/// <summary>
/// Kafka CreateAcls request (API Key 30)
/// Creates new ACL bindings.
/// </summary>
public sealed class CreateAclsRequest : KafkaRequest
{
    public required List<AclCreation> Creations { get; init; }

    public sealed class AclCreation
    {
        public required AclResourceTypeFilter ResourceType { get; init; }
        public required string ResourceName { get; init; }
        public required AclPatternTypeFilter PatternType { get; init; }
        public required string Principal { get; init; }
        public required string Host { get; init; }
        public required AclOperationFilter Operation { get; init; }
        public required AclPermissionTypeFilter PermissionType { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteInt32(Creations.Count);
        foreach (var creation in Creations)
        {
            writer.WriteInt8((sbyte)creation.ResourceType);
            writer.WriteString(creation.ResourceName);
            writer.WriteInt8((sbyte)creation.PatternType);
            writer.WriteString(creation.Principal);
            writer.WriteString(creation.Host);
            writer.WriteInt8((sbyte)creation.Operation);
            writer.WriteInt8((sbyte)creation.PermissionType);
        }
    }

    public static CreateAclsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;
        var creations = new List<AclCreation>();

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            var creationCount = protocolReader.ReadVarInt() - 1;
            for (int i = 0; i < creationCount; i++)
            {
                var resourceType = (AclResourceTypeFilter)protocolReader.ReadInt8();
                var resourceName = protocolReader.ReadCompactString()!;
                var patternType = (AclPatternTypeFilter)protocolReader.ReadInt8();
                var principal = protocolReader.ReadCompactString()!;
                var host = protocolReader.ReadCompactString()!;
                var operation = (AclOperationFilter)protocolReader.ReadInt8();
                var permissionType = (AclPermissionTypeFilter)protocolReader.ReadInt8();
                protocolReader.ReadVarInt(); // tagged fields

                creations.Add(new AclCreation
                {
                    ResourceType = resourceType,
                    ResourceName = resourceName,
                    PatternType = patternType,
                    Principal = principal,
                    Host = host,
                    Operation = operation,
                    PermissionType = permissionType
                });
            }
            protocolReader.ReadVarInt(); // tagged fields
        }
        else
        {
            var creationCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < creationCount; i++)
            {
                var resourceType = (AclResourceTypeFilter)reader.ReadByte();
                var resourceName = BinaryHelpers.ReadString(reader);
                var patternType = apiVersion >= 1 ? (AclPatternTypeFilter)reader.ReadByte() : AclPatternTypeFilter.Literal;
                var principal = BinaryHelpers.ReadString(reader);
                var host = BinaryHelpers.ReadString(reader);
                var operation = (AclOperationFilter)reader.ReadByte();
                var permissionType = (AclPermissionTypeFilter)reader.ReadByte();

                creations.Add(new AclCreation
                {
                    ResourceType = resourceType,
                    ResourceName = resourceName,
                    PatternType = patternType,
                    Principal = principal,
                    Host = host,
                    Operation = operation,
                    PermissionType = permissionType
                });
            }
        }

        return new CreateAclsRequest
        {
            ApiKey = ApiKey.CreateAcls,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Creations = creations
        };
    }
}

/// <summary>
/// Kafka DeleteAcls request (API Key 31)
/// Deletes ACLs matching the specified filters.
/// </summary>
public sealed class DeleteAclsRequest : KafkaRequest
{
    public required List<AclFilter> Filters { get; init; }

    public sealed class AclFilter
    {
        public required AclResourceTypeFilter ResourceTypeFilter { get; init; }
        public string? ResourceNameFilter { get; init; }
        public required AclPatternTypeFilter PatternTypeFilter { get; init; }
        public string? PrincipalFilter { get; init; }
        public string? HostFilter { get; init; }
        public required AclOperationFilter OperationFilter { get; init; }
        public required AclPermissionTypeFilter PermissionTypeFilter { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteInt32(Filters.Count);
        foreach (var filter in Filters)
        {
            writer.WriteInt8((sbyte)filter.ResourceTypeFilter);
            writer.WriteString(filter.ResourceNameFilter);
            writer.WriteInt8((sbyte)filter.PatternTypeFilter);
            writer.WriteString(filter.PrincipalFilter);
            writer.WriteString(filter.HostFilter);
            writer.WriteInt8((sbyte)filter.OperationFilter);
            writer.WriteInt8((sbyte)filter.PermissionTypeFilter);
        }
    }

    public static DeleteAclsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;
        var filters = new List<AclFilter>();

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            var filterCount = protocolReader.ReadVarInt() - 1;
            for (int i = 0; i < filterCount; i++)
            {
                var resourceTypeFilter = (AclResourceTypeFilter)protocolReader.ReadInt8();
                var resourceNameFilter = protocolReader.ReadCompactString();
                var patternTypeFilter = (AclPatternTypeFilter)protocolReader.ReadInt8();
                var principalFilter = protocolReader.ReadCompactString();
                var hostFilter = protocolReader.ReadCompactString();
                var operationFilter = (AclOperationFilter)protocolReader.ReadInt8();
                var permissionTypeFilter = (AclPermissionTypeFilter)protocolReader.ReadInt8();
                protocolReader.ReadVarInt(); // tagged fields

                filters.Add(new AclFilter
                {
                    ResourceTypeFilter = resourceTypeFilter,
                    ResourceNameFilter = resourceNameFilter,
                    PatternTypeFilter = patternTypeFilter,
                    PrincipalFilter = principalFilter,
                    HostFilter = hostFilter,
                    OperationFilter = operationFilter,
                    PermissionTypeFilter = permissionTypeFilter
                });
            }
            protocolReader.ReadVarInt(); // tagged fields
        }
        else
        {
            var filterCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < filterCount; i++)
            {
                var resourceTypeFilter = (AclResourceTypeFilter)reader.ReadByte();
                var resourceNameFilter = BinaryHelpers.ReadNullableString(reader);
                var patternTypeFilter = apiVersion >= 1 ? (AclPatternTypeFilter)reader.ReadByte() : AclPatternTypeFilter.Literal;
                var principalFilter = BinaryHelpers.ReadNullableString(reader);
                var hostFilter = BinaryHelpers.ReadNullableString(reader);
                var operationFilter = (AclOperationFilter)reader.ReadByte();
                var permissionTypeFilter = (AclPermissionTypeFilter)reader.ReadByte();

                filters.Add(new AclFilter
                {
                    ResourceTypeFilter = resourceTypeFilter,
                    ResourceNameFilter = resourceNameFilter,
                    PatternTypeFilter = patternTypeFilter,
                    PrincipalFilter = principalFilter,
                    HostFilter = hostFilter,
                    OperationFilter = operationFilter,
                    PermissionTypeFilter = permissionTypeFilter
                });
            }
        }

        return new DeleteAclsRequest
        {
            ApiKey = ApiKey.DeleteAcls,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Filters = filters
        };
    }
}

#region ACL Filter Enums (match Kafka protocol values)

/// <summary>
/// ACL resource type filter (matches Kafka protocol)
/// </summary>
public enum AclResourceTypeFilter : sbyte
{
    Unknown = 0,
    Any = 1,
    Topic = 2,
    Group = 3,
    Cluster = 4,
    TransactionalId = 5,
    DelegationToken = 6
}

/// <summary>
/// ACL pattern type filter (matches Kafka protocol)
/// </summary>
public enum AclPatternTypeFilter : sbyte
{
    Unknown = 0,
    Any = 1,
    Match = 2,
    Literal = 3,
    Prefixed = 4
}

/// <summary>
/// ACL operation filter (matches Kafka protocol)
/// </summary>
public enum AclOperationFilter : sbyte
{
    Unknown = 0,
    Any = 1,
    All = 2,
    Read = 3,
    Write = 4,
    Create = 5,
    Delete = 6,
    Alter = 7,
    Describe = 8,
    ClusterAction = 9,
    DescribeConfigs = 10,
    AlterConfigs = 11,
    IdempotentWrite = 12
}

/// <summary>
/// ACL permission type filter (matches Kafka protocol)
/// </summary>
public enum AclPermissionTypeFilter : sbyte
{
    Unknown = 0,
    Any = 1,
    Deny = 2,
    Allow = 3
}

#endregion

#region ACL Responses

/// <summary>
/// Kafka DescribeAcls response
/// </summary>
public sealed class DescribeAclsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public ErrorCode ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public required List<AclResource> Resources { get; init; }

    public sealed class AclResource
    {
        public required AclResourceTypeFilter ResourceType { get; init; }
        public required string ResourceName { get; init; }
        public required AclPatternTypeFilter PatternType { get; init; }
        public required List<AclBinding> Acls { get; init; }
    }

    public sealed class AclBinding
    {
        public required string Principal { get; init; }
        public required string Host { get; init; }
        public required AclOperationFilter Operation { get; init; }
        public required AclPermissionTypeFilter PermissionType { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        // Response header
        writer.WriteInt32(CorrelationId);
        if (isFlexible)
        {
            writer.WriteVarInt(0); // header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);

        if (isFlexible)
        {
            writer.WriteCompactString(ErrorMessage);
            writer.WriteVarInt(Resources.Count + 1);
        }
        else
        {
            writer.WriteString(ErrorMessage);
            writer.WriteInt32(Resources.Count);
        }

        foreach (var resource in Resources)
        {
            writer.WriteInt8((sbyte)resource.ResourceType);
            if (isFlexible)
            {
                writer.WriteCompactString(resource.ResourceName);
            }
            else
            {
                writer.WriteString(resource.ResourceName);
            }
            writer.WriteInt8((sbyte)resource.PatternType);

            if (isFlexible)
            {
                writer.WriteVarInt(resource.Acls.Count + 1);
            }
            else
            {
                writer.WriteInt32(resource.Acls.Count);
            }

            foreach (var acl in resource.Acls)
            {
                if (isFlexible)
                {
                    writer.WriteCompactString(acl.Principal);
                    writer.WriteCompactString(acl.Host);
                }
                else
                {
                    writer.WriteString(acl.Principal);
                    writer.WriteString(acl.Host);
                }
                writer.WriteInt8((sbyte)acl.Operation);
                writer.WriteInt8((sbyte)acl.PermissionType);
                if (isFlexible)
                {
                    writer.WriteVarInt(0); // tagged fields
                }
            }
            if (isFlexible)
            {
                writer.WriteVarInt(0); // tagged fields
            }
        }
        if (isFlexible)
        {
            writer.WriteVarInt(0); // tagged fields
        }
    }
}

/// <summary>
/// Kafka CreateAcls response
/// </summary>
public sealed class CreateAclsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required List<AclCreationResult> Results { get; init; }

    public sealed class AclCreationResult
    {
        public ErrorCode ErrorCode { get; init; }
        public string? ErrorMessage { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        // Response header
        writer.WriteInt32(CorrelationId);
        if (isFlexible)
        {
            writer.WriteVarInt(0); // header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);

        if (isFlexible)
        {
            writer.WriteVarInt(Results.Count + 1);
        }
        else
        {
            writer.WriteInt32(Results.Count);
        }

        foreach (var result in Results)
        {
            writer.WriteInt16((short)result.ErrorCode);
            if (isFlexible)
            {
                writer.WriteCompactString(result.ErrorMessage);
                writer.WriteVarInt(0); // tagged fields
            }
            else
            {
                writer.WriteString(result.ErrorMessage);
            }
        }
        if (isFlexible)
        {
            writer.WriteVarInt(0); // tagged fields
        }
    }
}

/// <summary>
/// Kafka DeleteAcls response
/// </summary>
public sealed class DeleteAclsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required List<AclFilterResult> FilterResults { get; init; }

    public sealed class AclFilterResult
    {
        public ErrorCode ErrorCode { get; init; }
        public string? ErrorMessage { get; init; }
        public required List<MatchingAcl> MatchingAcls { get; init; }
    }

    public sealed class MatchingAcl
    {
        public ErrorCode ErrorCode { get; init; }
        public string? ErrorMessage { get; init; }
        public required AclResourceTypeFilter ResourceType { get; init; }
        public required string ResourceName { get; init; }
        public required AclPatternTypeFilter PatternType { get; init; }
        public required string Principal { get; init; }
        public required string Host { get; init; }
        public required AclOperationFilter Operation { get; init; }
        public required AclPermissionTypeFilter PermissionType { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        // Response header
        writer.WriteInt32(CorrelationId);
        if (isFlexible)
        {
            writer.WriteVarInt(0); // header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);

        if (isFlexible)
        {
            writer.WriteVarInt(FilterResults.Count + 1);
        }
        else
        {
            writer.WriteInt32(FilterResults.Count);
        }

        foreach (var result in FilterResults)
        {
            writer.WriteInt16((short)result.ErrorCode);
            if (isFlexible)
            {
                writer.WriteCompactString(result.ErrorMessage);
                writer.WriteVarInt(result.MatchingAcls.Count + 1);
            }
            else
            {
                writer.WriteString(result.ErrorMessage);
                writer.WriteInt32(result.MatchingAcls.Count);
            }

            foreach (var acl in result.MatchingAcls)
            {
                writer.WriteInt16((short)acl.ErrorCode);
                if (isFlexible)
                {
                    writer.WriteCompactString(acl.ErrorMessage);
                    writer.WriteInt8((sbyte)acl.ResourceType);
                    writer.WriteCompactString(acl.ResourceName);
                    writer.WriteInt8((sbyte)acl.PatternType);
                    writer.WriteCompactString(acl.Principal);
                    writer.WriteCompactString(acl.Host);
                }
                else
                {
                    writer.WriteString(acl.ErrorMessage);
                    writer.WriteInt8((sbyte)acl.ResourceType);
                    writer.WriteString(acl.ResourceName);
                    writer.WriteInt8((sbyte)acl.PatternType);
                    writer.WriteString(acl.Principal);
                    writer.WriteString(acl.Host);
                }
                writer.WriteInt8((sbyte)acl.Operation);
                writer.WriteInt8((sbyte)acl.PermissionType);
                if (isFlexible)
                {
                    writer.WriteVarInt(0); // tagged fields
                }
            }
            if (isFlexible)
            {
                writer.WriteVarInt(0); // tagged fields
            }
        }
        if (isFlexible)
        {
            writer.WriteVarInt(0); // tagged fields
        }
    }
}

#endregion
