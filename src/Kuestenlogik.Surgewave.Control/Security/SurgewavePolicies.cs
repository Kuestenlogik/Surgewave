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
    /// Registers all Surgewave authorization policies.
    /// Each policy allows the admin role plus the feature-specific role.
    /// </summary>
    public static AuthorizationBuilder AddSurgewavePolicies(this AuthorizationBuilder builder, SurgewaveAuthConfig config)
    {
        var adminRole = config.AdminRole;

        builder.AddPolicy(TopicsManage, p => p.RequireRole(adminRole, "surgewave-topics"));
        builder.AddPolicy(ConnectorsManage, p => p.RequireRole(adminRole, "surgewave-connectors"));
        builder.AddPolicy(PipelinesManage, p => p.RequireRole(adminRole, "surgewave-pipelines"));
        builder.AddPolicy(AclManage, p => p.RequireRole(adminRole, "surgewave-acl"));
        builder.AddPolicy(SchemasManage, p => p.RequireRole(adminRole, "surgewave-schemas"));
        builder.AddPolicy(QuotasManage, p => p.RequireRole(adminRole, "surgewave-quotas"));
        builder.AddPolicy(PluginsManage, p => p.RequireRole(adminRole, "surgewave-plugins"));

        return builder;
    }
}
