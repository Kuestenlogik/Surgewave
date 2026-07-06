using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Defines authorization policy names and registers them for Surgewave Control RBAC.
/// </summary>
public static class SurgewavePolicies
{
    public const string TopicsManage = nameof(TopicsManage);
    public const string ConnectorsManage = nameof(ConnectorsManage);
    public const string PipelinesManage = nameof(PipelinesManage);
    public const string AclManage = nameof(AclManage);
    public const string SchemasManage = nameof(SchemasManage);
    public const string QuotasManage = nameof(QuotasManage);
    public const string PluginsManage = nameof(PluginsManage);

    /// <summary>
    /// Administering the RBAC system itself (roles + assignments) — admin-only,
    /// NOT satisfied by any feature role. The RoleManagement console is gated on
    /// this so a feature-scoped delegate (e.g. surgewave-acl) cannot grant itself
    /// the admin role.
    /// </summary>
    public const string RoleManage = nameof(RoleManage);

    /// <summary>
    /// Feature-specific role names each policy grants (alongside the configured
    /// admin role). These are the canonical role claims the store-backed RBAC
    /// maps its permissions onto — see RolePermissionMapping.
    /// </summary>
    public const string TopicsRole = "surgewave-topics";
    public const string ConnectorsRole = "surgewave-connectors";
    public const string PipelinesRole = "surgewave-pipelines";
    public const string AclRole = "surgewave-acl";
    public const string SchemasRole = "surgewave-schemas";
    public const string QuotasRole = "surgewave-quotas";
    public const string PluginsRole = "surgewave-plugins";

    /// <summary>
    /// Registers all Surgewave authorization policies.
    /// Each policy allows the admin role plus the feature-specific role.
    /// </summary>
    public static AuthorizationBuilder AddSurgewavePolicies(this AuthorizationBuilder builder, SurgewaveAuthConfig config)
    {
        var adminRole = config.AdminRole;

        builder.AddPolicy(TopicsManage, p => p.RequireRole(adminRole, TopicsRole));
        builder.AddPolicy(ConnectorsManage, p => p.RequireRole(adminRole, ConnectorsRole));
        builder.AddPolicy(PipelinesManage, p => p.RequireRole(adminRole, PipelinesRole));
        builder.AddPolicy(AclManage, p => p.RequireRole(adminRole, AclRole));
        builder.AddPolicy(SchemasManage, p => p.RequireRole(adminRole, SchemasRole));
        builder.AddPolicy(QuotasManage, p => p.RequireRole(adminRole, QuotasRole));
        builder.AddPolicy(PluginsManage, p => p.RequireRole(adminRole, PluginsRole));

        // RBAC administration is admin-only — deliberately no feature role.
        builder.AddPolicy(RoleManage, p => p.RequireRole(adminRole));

        return builder;
    }
}
