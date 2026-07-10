namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Operations that can be authorized
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
