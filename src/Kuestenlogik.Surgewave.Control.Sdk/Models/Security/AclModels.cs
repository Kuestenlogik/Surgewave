namespace Kuestenlogik.Surgewave.Control.Models.Security;

/// <summary>
/// ACL entry response from the API.
/// </summary>
public sealed record AclEntryModel(
    string Principal,
    string Host,
    AclResourceType ResourceType,
    AclPatternType PatternType,
    string ResourceName,
    AclOperation Operation,
    AclPermission Permission);

/// <summary>
/// Request to create a new ACL entry.
/// </summary>
public sealed record CreateAclRequest(
    string Principal,
    AclResourceType ResourceType,
    string ResourceName,
    AclOperation Operation,
    AclPermission Permission,
    string? Host = "*",
    AclPatternType? PatternType = AclPatternType.Literal);

/// <summary>
/// Response for delete ACLs operation.
/// </summary>
public sealed record AclDeleteResult(int DeletedCount);

/// <summary>
/// Resource types that can be protected by ACLs.
/// </summary>
public enum AclResourceType
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
/// Pattern types for matching resource names.
/// </summary>
public enum AclPatternType
{
    Unknown = 0,
    Any = 1,
    Match = 2,
    Literal = 3,
    Prefixed = 4,
    Suffix = 5,
    Regex = 6
}

/// <summary>
/// Operations that can be authorized.
/// </summary>
public enum AclOperation
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
/// Permission types.
/// </summary>
public enum AclPermission
{
    Unknown = 0,
    Any = 1,
    Deny = 2,
    Allow = 3
}
