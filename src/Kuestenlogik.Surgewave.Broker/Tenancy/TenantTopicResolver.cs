namespace Kuestenlogik.Surgewave.Broker.Tenancy;

/// <summary>
/// Resolves tenant-scoped topic names. Topics are scoped as "tenantId/topicName"
/// for non-default tenants, or just "topicName" for the default tenant.
/// </summary>
public static class TenantTopicResolver
{
    private const char Separator = '/';

    /// <summary>
    /// Qualifies a topic name with a tenant prefix.
    /// Returns "tenantId/topicName" for non-default tenants, or "topicName" for the default tenant.
    /// </summary>
    public static string QualifyTopicName(TenantId tenant, string topicName)
    {
        if (tenant.IsDefault)
            return topicName;

        return $"{tenant.Value}{Separator}{topicName}";
    }

    /// <summary>
    /// Parses a qualified topic name into its tenant and topic components.
    /// Returns (default, name) if no slash is present.
    /// </summary>
    public static (TenantId Tenant, string TopicName) ParseQualifiedName(string qualifiedName)
    {
        var separatorIndex = qualifiedName.IndexOf(Separator);
        if (separatorIndex < 0)
            return (TenantId.Default, qualifiedName);

        var tenantValue = qualifiedName[..separatorIndex];
        var topicName = qualifiedName[(separatorIndex + 1)..];
        return (new TenantId(tenantValue), topicName);
    }

    /// <summary>
    /// Checks if a topic name contains a tenant qualifier (i.e., contains a slash).
    /// </summary>
    public static bool IsQualifiedName(string name) =>
        name.Contains(Separator);

    /// <summary>
    /// Qualifies multiple topic names with a tenant prefix.
    /// </summary>
    public static string[] QualifyTopicNames(TenantId tenant, IEnumerable<string> topicNames) =>
        topicNames.Select(name => QualifyTopicName(tenant, name)).ToArray();
}
