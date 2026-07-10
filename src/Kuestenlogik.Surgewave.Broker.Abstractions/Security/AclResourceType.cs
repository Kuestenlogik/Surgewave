namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Resource types that can be protected by ACLs
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
