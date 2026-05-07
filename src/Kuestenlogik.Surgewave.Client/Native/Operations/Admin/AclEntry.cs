namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// ACL entry.
/// </summary>
public record AclEntry
{
    public AclResourceType ResourceType { get; init; }
    public string ResourceName { get; init; } = string.Empty;
    public AclPatternType PatternType { get; init; }
    public string Principal { get; init; } = string.Empty;
    public string Host { get; init; } = string.Empty;
    public AclOperation Operation { get; init; }
    public AclPermission Permission { get; init; }
}
