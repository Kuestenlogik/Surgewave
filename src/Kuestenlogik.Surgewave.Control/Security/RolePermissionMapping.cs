namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Maps the RoleManagement UI's dotted permission strings onto the ASP.NET
/// policy role claims that <see cref="SurgewavePolicies"/> actually checks.
/// A permission only becomes enforceable if it has an entry here — the store
/// grants exactly what the policy layer consumes, no fourth vocabulary.
/// Permissions without a mapping (topics.produce/consume, catalog.edit,
/// alerts.manage) are display-only until a matching policy exists.
/// </summary>
public static class RolePermissionMapping
{
    /// <summary>
    /// Build the permission→role-claim map for the given admin role (which is
    /// configurable via <c>Auth:AdminRole</c>). cluster.admin grants the admin
    /// role; every other mapped permission grants its feature role.
    /// </summary>
    public static IReadOnlyDictionary<string, string> Build(string adminRole) =>
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["topics.manage"] = SurgewavePolicies.TopicsRole,
            ["connectors.manage"] = SurgewavePolicies.ConnectorsRole,
            ["pipelines.manage"] = SurgewavePolicies.PipelinesRole,
            ["acls.manage"] = SurgewavePolicies.AclRole,
            ["schemas.manage"] = SurgewavePolicies.SchemasRole,
            ["quotas.manage"] = SurgewavePolicies.QuotasRole,
            ["plugins.manage"] = SurgewavePolicies.PluginsRole,
            ["cluster.admin"] = adminRole,
        };

    /// <summary>
    /// The permission keys that map onto an enforcing policy role (admin-role
    /// value aside, the key set is constant). Used by the UI to flag which
    /// permissions are actually enforced.
    /// </summary>
    public static readonly IReadOnlySet<string> EnforcedPermissions =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "topics.manage", "connectors.manage", "pipelines.manage", "acls.manage",
            "schemas.manage", "quotas.manage", "plugins.manage", "cluster.admin",
        };
}
