namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// ACL resource type.
/// </summary>
public enum AclResourceType : byte
{
    Any = 0,
    Topic = 1,
    Group = 2,
    Cluster = 3,
    TransactionalId = 4,
    DelegationToken = 5
}
