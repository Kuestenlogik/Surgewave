using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Authorizer that checks ACLs to determine if operations are permitted.
/// Implements Kafka-compatible authorization semantics.
/// </summary>
public sealed class AclAuthorizer
{
    private readonly ConcurrentBag<AclEntry> _acls = [];
    private readonly ILogger<AclAuthorizer>? _logger;
    private readonly bool _allowIfNoAclFound;
    private readonly HashSet<string> _superUsers;
    private readonly string? _aclFilePath;

    public AclAuthorizer(
        ILogger<AclAuthorizer>? logger = null,
        bool allowIfNoAclFound = false,
        IEnumerable<string>? superUsers = null,
        string? aclFilePath = null)
    {
        _logger = logger;
        _allowIfNoAclFound = allowIfNoAclFound;
        _superUsers = superUsers?.ToHashSet(StringComparer.OrdinalIgnoreCase) ?? [];
        _aclFilePath = aclFilePath;

        if (!string.IsNullOrEmpty(aclFilePath) && File.Exists(aclFilePath))
        {
            LoadFromFile(aclFilePath);
        }
    }

    /// <summary>
    /// Check if a principal is authorized to perform an operation on a resource
    /// </summary>
    public AuthorizationResult Authorize(
        string principal,
        string host,
        AclResourceType resourceType,
        string resourceName,
        AclOperation operation)
    {
        // Super users bypass all ACL checks
        if (_superUsers.Contains(principal))
        {
            _logger?.LogDebug("Super user {Principal} authorized for {Operation} on {ResourceType}:{ResourceName}",
                principal, operation, resourceType, resourceName);
            return AuthorizationResult.Allowed("Super user");
        }

        // Find all matching ACLs
        var matchingAcls = _acls
            .Where(acl => acl.Matches(principal, host, resourceType, resourceName, operation))
            .ToList();

        // If no ACLs match, use default behavior
        if (matchingAcls.Count == 0)
        {
            if (_allowIfNoAclFound)
            {
                _logger?.LogDebug("No ACL found for {Principal} {Operation} on {ResourceType}:{ResourceName}, allowing by default",
                    principal, operation, resourceType, resourceName);
                return AuthorizationResult.Allowed("No ACL found, default allow");
            }

            _logger?.LogDebug("No ACL found for {Principal} {Operation} on {ResourceType}:{ResourceName}, denying by default",
                principal, operation, resourceType, resourceName);
            return AuthorizationResult.Denied("No ACL found");
        }

        // Deny takes precedence over Allow
        var denyAcl = matchingAcls.FirstOrDefault(a => a.Permission == AclPermission.Deny);
        if (denyAcl != null)
        {
            _logger?.LogDebug("Denied {Principal} {Operation} on {ResourceType}:{ResourceName} by {Acl}",
                principal, operation, resourceType, resourceName, denyAcl);
            return AuthorizationResult.Denied($"Denied by ACL: {denyAcl}");
        }

        // Check for Allow
        var allowAcl = matchingAcls.FirstOrDefault(a => a.Permission == AclPermission.Allow);
        if (allowAcl != null)
        {
            _logger?.LogDebug("Allowed {Principal} {Operation} on {ResourceType}:{ResourceName} by {Acl}",
                principal, operation, resourceType, resourceName, allowAcl);
            return AuthorizationResult.Allowed($"Allowed by ACL: {allowAcl}");
        }

        // Should not reach here, but deny by default
        return AuthorizationResult.Denied("No matching allow ACL");
    }

    /// <summary>
    /// Add an ACL entry
    /// </summary>
    public void AddAcl(AclEntry acl)
    {
        _acls.Add(acl);
        _logger?.LogInformation("Added ACL: {Acl}", acl);
    }

    /// <summary>
    /// Add multiple ACL entries
    /// </summary>
    public void AddAcls(IEnumerable<AclEntry> acls)
    {
        foreach (var acl in acls)
        {
            AddAcl(acl);
        }
    }

    /// <summary>
    /// Remove ACLs matching the filter
    /// </summary>
    public int RemoveAcls(Func<AclEntry, bool> filter)
    {
        // ConcurrentBag doesn't support removal, so we need to rebuild
        var toKeep = _acls.Where(a => !filter(a)).ToList();
        var removed = _acls.Count - toKeep.Count;

        // Clear and re-add (not ideal for production, but works for simplicity)
        while (_acls.TryTake(out _)) { }
        foreach (var acl in toKeep)
        {
            _acls.Add(acl);
        }

        _logger?.LogInformation("Removed {Count} ACLs", removed);
        return removed;
    }

    /// <summary>
    /// List all ACLs matching the filter
    /// </summary>
    public IEnumerable<AclEntry> ListAcls(Func<AclEntry, bool>? filter = null)
    {
        return filter == null ? _acls.ToList() : _acls.Where(filter).ToList();
    }

    /// <summary>
    /// Get the count of ACLs
    /// </summary>
    public int AclCount => _acls.Count;

    /// <summary>
    /// Get a bitmask of authorized operations for a resource.
    /// Each bit represents an operation where bit position = (int)AclOperation.
    /// Bit is set if the operation is authorized.
    /// </summary>
    public int GetAuthorizedOperations(
        string principal,
        string host,
        AclResourceType resourceType,
        string resourceName)
    {
        // Operations relevant to Group resources
        var groupOperations = new[] { AclOperation.Read, AclOperation.Describe, AclOperation.Delete };

        // Operations relevant to Topic resources
        var topicOperations = new[] { AclOperation.Read, AclOperation.Write, AclOperation.Create, AclOperation.Delete,
            AclOperation.Alter, AclOperation.Describe, AclOperation.DescribeConfigs, AclOperation.AlterConfigs };

        // Operations relevant to Cluster resources
        var clusterOperations = new[] { AclOperation.Create, AclOperation.Alter, AclOperation.Describe,
            AclOperation.ClusterAction, AclOperation.DescribeConfigs, AclOperation.AlterConfigs, AclOperation.IdempotentWrite };

        var operationsToCheck = resourceType switch
        {
            AclResourceType.Group => groupOperations,
            AclResourceType.Topic => topicOperations,
            AclResourceType.Cluster => clusterOperations,
            _ => Enum.GetValues<AclOperation>().Where(o => o > AclOperation.All).ToArray()
        };

        int authorizedMask = 0;
        foreach (var operation in operationsToCheck)
        {
            var result = Authorize(principal, host, resourceType, resourceName, operation);
            if (result.IsAllowed)
            {
                authorizedMask |= (1 << (int)operation);
            }
        }

        return authorizedMask;
    }

    /// <summary>
    /// Save ACLs to file
    /// </summary>
    public void SaveToFile(string? path = null)
    {
        var filePath = path ?? _aclFilePath
            ?? throw new InvalidOperationException("No ACL file path specified");

        var lines = _acls.Select(acl =>
            $"{acl.Principal}|{acl.Host}|{acl.ResourceType}|{acl.PatternType}|{acl.ResourceName}|{acl.Operation}|{acl.Permission}");

        File.WriteAllLines(filePath, lines);
        _logger?.LogInformation("Saved {Count} ACLs to {Path}", _acls.Count, filePath);
    }

    /// <summary>
    /// Load ACLs from file
    /// </summary>
    public void LoadFromFile(string path)
    {
        foreach (var line in File.ReadAllLines(path))
        {
            if (string.IsNullOrWhiteSpace(line) || line.StartsWith('#'))
                continue;

            var parts = line.Split('|');
            if (parts.Length >= 7)
            {
                try
                {
                    var acl = new AclEntry
                    {
                        Principal = parts[0],
                        Host = parts[1],
                        ResourceType = Enum.Parse<AclResourceType>(parts[2], ignoreCase: true),
                        PatternType = Enum.Parse<AclPatternType>(parts[3], ignoreCase: true),
                        ResourceName = parts[4],
                        Operation = Enum.Parse<AclOperation>(parts[5], ignoreCase: true),
                        Permission = Enum.Parse<AclPermission>(parts[6], ignoreCase: true)
                    };
                    _acls.Add(acl);
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Failed to parse ACL line: {Line}", line);
                }
            }
        }

        _logger?.LogInformation("Loaded {Count} ACLs from {Path}", _acls.Count, path);
    }

    /// <summary>
    /// Create common ACL entries for convenience
    /// </summary>
    public static class CommonAcls
    {
        /// <summary>
        /// Allow a user to read from a topic
        /// </summary>
        public static AclEntry AllowTopicRead(string principal, string topicName) => new()
        {
            Principal = principal,
            ResourceType = AclResourceType.Topic,
            ResourceName = topicName,
            Operation = AclOperation.Read,
            Permission = AclPermission.Allow
        };

        /// <summary>
        /// Allow a user to write to a topic
        /// </summary>
        public static AclEntry AllowTopicWrite(string principal, string topicName) => new()
        {
            Principal = principal,
            ResourceType = AclResourceType.Topic,
            ResourceName = topicName,
            Operation = AclOperation.Write,
            Permission = AclPermission.Allow
        };

        /// <summary>
        /// Allow a user to read from topics with a prefix
        /// </summary>
        public static AclEntry AllowTopicPrefixRead(string principal, string topicPrefix) => new()
        {
            Principal = principal,
            ResourceType = AclResourceType.Topic,
            ResourceName = topicPrefix,
            PatternType = AclPatternType.Prefixed,
            Operation = AclOperation.Read,
            Permission = AclPermission.Allow
        };

        /// <summary>
        /// Allow a user to use a consumer group
        /// </summary>
        public static AclEntry AllowGroupRead(string principal, string groupId) => new()
        {
            Principal = principal,
            ResourceType = AclResourceType.Group,
            ResourceName = groupId,
            Operation = AclOperation.Read,
            Permission = AclPermission.Allow
        };

        /// <summary>
        /// Allow all operations on all resources (admin access)
        /// </summary>
        public static AclEntry AllowAll(string principal) => new()
        {
            Principal = principal,
            ResourceType = AclResourceType.Cluster,
            ResourceName = "*",
            Operation = AclOperation.All,
            Permission = AclPermission.Allow
        };
    }
}

/// <summary>
/// Result of an authorization check
/// </summary>
public readonly struct AuthorizationResult : IEquatable<AuthorizationResult>
{
    public bool IsAllowed { get; }
    public string Reason { get; }

    private AuthorizationResult(bool isAllowed, string reason)
    {
        IsAllowed = isAllowed;
        Reason = reason;
    }

    public static AuthorizationResult Allowed(string reason) => new(true, reason);
    public static AuthorizationResult Denied(string reason) => new(false, reason);

    public bool Equals(AuthorizationResult other) =>
        IsAllowed == other.IsAllowed && Reason == other.Reason;

    public override bool Equals(object? obj) =>
        obj is AuthorizationResult other && Equals(other);

    public override int GetHashCode() =>
        HashCode.Combine(IsAllowed, Reason);

    public static bool operator ==(AuthorizationResult left, AuthorizationResult right) =>
        left.Equals(right);

    public static bool operator !=(AuthorizationResult left, AuthorizationResult right) =>
        !left.Equals(right);
}
