namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Protocol-neutral authorization seam. Checks whether a principal is permitted to
/// perform an operation on a resource. Implemented by the broker's ACL authorizer.
/// </summary>
public interface IAuthorizer
{
    /// <summary>
    /// Check if a principal is authorized to perform an operation on a resource.
    /// </summary>
    AuthorizationResult Authorize(
        string principal,
        string host,
        AclResourceType resourceType,
        string resourceName,
        AclOperation operation);

    /// <summary>Add an ACL entry. Admin surface used by the Kafka CreateAcls handler.</summary>
    void AddAcl(AclEntry acl);

    /// <summary>List ACL entries matching the optional filter (all if null). Used by DescribeAcls / DeleteAcls.</summary>
    IEnumerable<AclEntry> ListAcls(Func<AclEntry, bool>? filter = null);

    /// <summary>Remove ACL entries matching the filter, returning the removed count. Used by DeleteAcls.</summary>
    int RemoveAcls(Func<AclEntry, bool> filter);

    /// <summary>Persist the current ACL set to the given path (or the configured default). Used by Create/DeleteAcls.</summary>
    void SaveToFile(string? path = null);
}
