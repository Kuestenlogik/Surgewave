using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// Fluent builder for creating ACL entries.
/// </summary>
public sealed class AclBuilder
{
    private readonly SurgewaveNativeClient _client;
    private AclResourceType _resourceType = AclResourceType.Topic;
    private string _resourceName = string.Empty;
    private AclPatternType _patternType = AclPatternType.Literal;
    private string _principal = string.Empty;
    private string _host = "*";
    private AclOperation _operation = AclOperation.All;
    private AclPermission _permission = AclPermission.Allow;

    internal AclBuilder(SurgewaveNativeClient client)
    {
        _client = client;
    }

    /// <summary>
    /// Set the resource type.
    /// </summary>
    public AclBuilder ForResourceType(AclResourceType resourceType)
    {
        _resourceType = resourceType;
        return this;
    }

    /// <summary>
    /// Set the resource name.
    /// </summary>
    public AclBuilder ForResource(string resourceName)
    {
        _resourceName = resourceName;
        return this;
    }

    /// <summary>
    /// Set the pattern type.
    /// </summary>
    public AclBuilder WithPatternType(AclPatternType patternType)
    {
        _patternType = patternType;
        return this;
    }

    /// <summary>
    /// Set the principal.
    /// </summary>
    public AclBuilder ForPrincipal(string principal)
    {
        _principal = principal;
        return this;
    }

    /// <summary>
    /// Set the host.
    /// </summary>
    public AclBuilder FromHost(string host)
    {
        _host = host;
        return this;
    }

    /// <summary>
    /// Set the operation.
    /// </summary>
    public AclBuilder WithOperation(AclOperation operation)
    {
        _operation = operation;
        return this;
    }

    /// <summary>
    /// Set the permission.
    /// </summary>
    public AclBuilder WithPermission(AclPermission permission)
    {
        _permission = permission;
        return this;
    }

    /// <summary>
    /// Create the ACL entry.
    /// </summary>
    public Task<List<AclCreateResult>> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        var entry = new AclEntry
        {
            ResourceType = _resourceType,
            ResourceName = _resourceName,
            PatternType = _patternType,
            Principal = _principal,
            Host = _host,
            Operation = _operation,
            Permission = _permission
        };

        return _client.Admin.CreateAclsAsync(new List<AclEntry> { entry }, cancellationToken);
    }
}
