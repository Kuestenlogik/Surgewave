namespace Kuestenlogik.Surgewave.Control.Services.Security;

/// <summary>
/// Canonical RBAC permission catalogue and built-in role definitions. Shared by
/// the RoleManagement UI and the role service so both agree on what a role
/// grants. Permission strings that map onto a real ASP.NET policy role are the
/// ones actually enforced (see the claims transformation); the rest are
/// display-only until a matching policy exists.
/// </summary>
public static class RolePermissions
{
    /// <summary>All known permission keys with human-readable descriptions.</summary>
    public static readonly IReadOnlyDictionary<string, string> Catalogue = new Dictionary<string, string>
    {
        ["topics.manage"] = "Create, delete, and configure topics",
        ["topics.produce"] = "Produce messages to topics",
        ["topics.consume"] = "Consume/browse messages from topics",
        ["connectors.manage"] = "Create, delete, and configure connectors",
        ["pipelines.manage"] = "Create, delete, and run pipelines",
        ["schemas.manage"] = "Register and delete schemas",
        ["acls.manage"] = "Create and delete ACL entries",
        ["quotas.manage"] = "Configure client quotas",
        ["plugins.manage"] = "Upload and manage plugins",
        ["cluster.admin"] = "Full cluster administration",
        ["catalog.edit"] = "Edit data catalog entries",
        ["alerts.manage"] = "Create and manage alert rules",
    };

    /// <summary>Names of the four built-in roles that always exist.</summary>
    public const string Admin = "Admin";
    public const string Operator = "Operator";
    public const string Developer = "Developer";
    public const string Viewer = "Viewer";

    public static bool IsBuiltInName(string name) =>
        name is Admin or Operator or Developer or Viewer;

    /// <summary>The permission set each built-in role grants.</summary>
    public static readonly IReadOnlyDictionary<string, string[]> BuiltInPermissions = new Dictionary<string, string[]>
    {
        [Admin] = [.. Catalogue.Keys],
        [Operator] = ["topics.manage", "topics.produce", "topics.consume", "connectors.manage", "catalog.edit", "alerts.manage"],
        [Developer] = ["topics.produce", "topics.consume", "schemas.manage", "catalog.edit"],
        [Viewer] = ["topics.consume"],
    };

    public static readonly IReadOnlyDictionary<string, string> BuiltInDescriptions = new Dictionary<string, string>
    {
        [Admin] = "Full access to all features",
        [Operator] = "Manage topics, connectors, and monitor cluster",
        [Developer] = "Read topics, produce messages, manage schemas",
        [Viewer] = "Read-only access to all resources",
    };
}
