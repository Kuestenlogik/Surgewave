using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Licensing;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Broker.Plugins;

/// <summary>
/// Read-only REST endpoint surface that lets the Control UI render
/// the License page (G15 follow-up — Roadmap item "Control UI license
/// page"). Exposes the edition / licensee / expiry resolved at startup
/// plus the list of every discovered broker plugin together with its
/// licence requirement and whether it actually activated.
/// </summary>
public static class LicenseRestApi
{
    /// <summary>Wire <c>/api/license/*</c> into the application.</summary>
    public static IEndpointRouteBuilder MapLicenseApi(
        this IEndpointRouteBuilder app,
        ILicenseProvider? license,
        IReadOnlyList<IBrokerPlugin> activatedPlugins)
    {
        // Re-run the assembly scan so the page also shows plugins that
        // were discovered but skipped (config disabled / licence
        // missing). Cheap — reflection over already-loaded assemblies.
        var discoveredPlugins = BrokerPluginActivator.Discover<IBrokerPlugin>();
        var group = app.MapGroup("/api/license").WithTags("License");

        group.MapGet("/status", () =>
        {
            var status = new LicenseStatusResponse(
                Edition: license?.Edition ?? SurgewaveEdition.Community,
                LicensedTo: license?.LicensedTo,
                ExpiresAt: license?.ExpiresAt,
                HasProvider: license is not null);
            return Results.Ok(status);
        }).WithName("GetLicenseStatus")
          .WithSummary("Get the active edition, licensee and expiry")
          .Produces<LicenseStatusResponse>();

        group.MapGet("/plugins", () =>
        {
            var activatedIds = activatedPlugins.Select(p => p.FeatureId).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var rows = discoveredPlugins
                .Select(p => new LicensePluginRow(
                    FeatureId: p.FeatureId,
                    DisplayName: p.DisplayName,
                    RequiresLicense: p.RequiresLicense,
                    IsActive: activatedIds.Contains(p.FeatureId),
                    IsLicensed: !p.RequiresLicense || (license?.IsFeatureEnabled(p.FeatureId) ?? false)))
                .OrderBy(r => r.DisplayName, StringComparer.OrdinalIgnoreCase)
                .ToList();
            return Results.Ok(rows);
        }).WithName("GetLicensePlugins")
          .WithSummary("List every discovered broker plugin with its licence requirement + activation status")
          .Produces<IReadOnlyList<LicensePluginRow>>();

        return app;
    }
}

/// <summary>Response shape for <c>GET /api/license/status</c>.</summary>
public sealed record LicenseStatusResponse(
    SurgewaveEdition Edition,
    string? LicensedTo,
    DateTimeOffset? ExpiresAt,
    bool HasProvider);

/// <summary>One row in <c>GET /api/license/plugins</c>.</summary>
public sealed record LicensePluginRow(
    string FeatureId,
    string DisplayName,
    bool RequiresLicense,
    bool IsActive,
    bool IsLicensed);
