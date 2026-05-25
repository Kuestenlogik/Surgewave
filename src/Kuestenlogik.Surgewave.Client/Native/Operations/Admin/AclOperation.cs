namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// ACL operation.
/// </summary>
public enum AclOperation : byte
{
    Any = 0,
    All = 1,
    Read = 2,
    Write = 3,
    Create = 4,
    Delete = 5,
    Alter = 6,
    Describe = 7,
    ClusterAction = 8,
    DescribeConfigs = 9,
    AlterConfigs = 10,
    IdempotentWrite = 11
}
