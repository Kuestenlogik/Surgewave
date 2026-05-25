using Kuestenlogik.Surgewave.Control.Models.Security;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for ACL management REST API.
/// </summary>
public interface IAclApiClient
{
    /// <summary>
    /// List all ACLs with optional filtering.
    /// </summary>
    Task<IReadOnlyList<AclEntryModel>> ListAclsAsync(
        string? principal = null,
        AclResourceType? resourceType = null,
        string? resourceName = null,
        AclOperation? operation = null,
        AclPermission? permission = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Create a new ACL entry.
    /// </summary>
    Task<AclEntryModel?> CreateAclAsync(CreateAclRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Create multiple ACL entries.
    /// </summary>
    Task<IReadOnlyList<AclEntryModel>?> CreateAclsBatchAsync(IReadOnlyList<CreateAclRequest> requests, CancellationToken cancellationToken = default);

    /// <summary>
    /// Delete ACLs matching the filter.
    /// </summary>
    Task<int> DeleteAclsAsync(
        string? principal = null,
        AclResourceType? resourceType = null,
        string? resourceName = null,
        AclOperation? operation = null,
        AclPermission? permission = null,
        CancellationToken cancellationToken = default);
}
