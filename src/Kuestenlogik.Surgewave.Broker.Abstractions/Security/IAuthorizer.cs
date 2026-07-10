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
}
