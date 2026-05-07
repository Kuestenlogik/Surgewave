using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Represents an Access Control List entry for Kafka authorization.
/// Based on Kafka's ACL model (KIP-11).
/// </summary>
public sealed record AclEntry
{
    /// <summary>
    /// Cache for compiled regex patterns to avoid recompilation on every match.
    /// </summary>
    private static readonly ConcurrentDictionary<string, Regex> RegexCache = new();

    /// <summary>
    /// Timeout for regex matching to prevent ReDoS attacks.
    /// </summary>
    private static readonly TimeSpan RegexTimeout = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// The principal this ACL applies to (e.g., "User:alice", "User:*")
    /// </summary>
    public required string Principal { get; init; }

    /// <summary>
    /// The host from which access is allowed/denied ("*" for any host)
    /// </summary>
    public string Host { get; init; } = "*";

    /// <summary>
    /// The type of resource this ACL applies to
    /// </summary>
    public required AclResourceType ResourceType { get; init; }

    /// <summary>
    /// The name of the resource (topic name, group ID, etc.)
    /// "*" matches all resources of the type
    /// </summary>
    public required string ResourceName { get; init; }

    /// <summary>
    /// Pattern type for resource name matching
    /// </summary>
    public AclPatternType PatternType { get; init; } = AclPatternType.Literal;

    /// <summary>
    /// The operation being authorized
    /// </summary>
    public required AclOperation Operation { get; init; }

    /// <summary>
    /// Whether access is allowed or denied
    /// </summary>
    public required AclPermission Permission { get; init; }

    public override string ToString() =>
        $"(principal={Principal}, host={Host}, resource={ResourceType}:{PatternType}:{ResourceName}, operation={Operation}, permission={Permission})";

    /// <summary>
    /// Check if this ACL matches the given resource and operation
    /// </summary>
    public bool Matches(string principal, string host, AclResourceType resourceType, string resourceName, AclOperation operation)
    {
        if (!MatchesPrincipal(principal))
            return false;

        if (Host != "*" && !string.Equals(Host, host, StringComparison.OrdinalIgnoreCase))
            return false;

        if (ResourceType != resourceType)
            return false;

        if (!MatchesResource(resourceName))
            return false;

        if (Operation != AclOperation.All && Operation != operation)
            return false;

        return true;
    }

    private bool MatchesPrincipal(string principal) =>
        Principal is "User:*" or "*" ||
        string.Equals(Principal, principal, StringComparison.OrdinalIgnoreCase);

    private bool MatchesResource(string resourceName) =>
        PatternType switch
        {
            AclPatternType.Literal => ResourceName == "*" ||
                string.Equals(ResourceName, resourceName, StringComparison.OrdinalIgnoreCase),
            AclPatternType.Prefixed => resourceName.StartsWith(ResourceName, StringComparison.OrdinalIgnoreCase),
            AclPatternType.Suffix => resourceName.EndsWith(ResourceName, StringComparison.OrdinalIgnoreCase),
            AclPatternType.Regex => MatchesRegex(resourceName),
            _ => false
        };

    private bool MatchesRegex(string resourceName)
    {
        try
        {
            var regex = RegexCache.GetOrAdd(ResourceName, pattern =>
                new Regex(pattern, RegexOptions.Compiled | RegexOptions.IgnoreCase, RegexTimeout));
            return regex.IsMatch(resourceName);
        }
        catch (RegexMatchTimeoutException)
        {
            // Pattern took too long - treat as no match for safety
            return false;
        }
        catch (ArgumentException)
        {
            // Invalid regex pattern - treat as no match
            return false;
        }
    }

    /// <summary>
    /// Validate the ACL entry. Returns null if valid, error message if invalid.
    /// </summary>
    public string? Validate()
    {
        if (string.IsNullOrWhiteSpace(Principal))
            return "Principal is required";

        if (string.IsNullOrWhiteSpace(ResourceName))
            return "ResourceName is required";

        if (PatternType == AclPatternType.Regex)
        {
            try
            {
                _ = new Regex(ResourceName, RegexOptions.Compiled | RegexOptions.IgnoreCase, RegexTimeout);
            }
            catch (ArgumentException ex)
            {
                return $"Invalid regex pattern: {ex.Message}";
            }
        }

        return null;
    }
}

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

/// <summary>
/// Pattern types for matching resource names
/// </summary>
public enum AclPatternType
{
    Unknown = 0,
    Any = 1,
    Match = 2,
    Literal = 3,
    Prefixed = 4,
    /// <summary>
    /// Suffix matching - resource name must end with the pattern
    /// </summary>
    Suffix = 5,
    /// <summary>
    /// Regex matching - resource name must match the regex pattern
    /// </summary>
    Regex = 6
}

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

/// <summary>
/// Permission types
/// </summary>
public enum AclPermission
{
    Unknown = 0,
    Any = 1,
    Deny = 2,
    Allow = 3
}
