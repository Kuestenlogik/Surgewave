using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Transforms Azure AD / Entra ID claims into standard .NET Role claims.
/// Handles flat "roles" array (App Roles), "groups" claim (Object IDs), and "wids" (Directory Roles).
/// </summary>
public sealed class EntraIdClaimsTransformation(IOptions<SurgewaveAuthConfig> authOptions) : IClaimsTransformation
{
    private const string TransformIssuer = "EntraIdTransform";

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal) =>
        Task.FromResult(principal);

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal, IdpProviderConfig providerConfig)
    {
        if (principal.Identity is not ClaimsIdentity identity)
            return Task.FromResult(principal);

        // Idempotency — already transformed
        if (identity.HasClaim(c => c.Type == ClaimTypes.Role && c.Issuer == TransformIssuer))
            return Task.FromResult(principal);

        // Entra ID sends App Roles as individual "roles" claims
        foreach (var roleClaim in identity.FindAll("roles"))
        {
            if (!string.IsNullOrEmpty(roleClaim.Value))
            {
                identity.AddClaim(new Claim(ClaimTypes.Role, roleClaim.Value, ClaimValueTypes.String, TransformIssuer));
            }
        }

        // Map group Object IDs to Surgewave roles via configured mappings
        var groupMappings = ParseGroupMappings(providerConfig.GroupRoleMappings);
        if (groupMappings.Count > 0)
        {
            foreach (var groupClaim in identity.FindAll("groups"))
            {
                if (groupMappings.TryGetValue(groupClaim.Value, out var surgewaveRole))
                {
                    identity.AddClaim(new Claim(ClaimTypes.Role, surgewaveRole, ClaimValueTypes.String, TransformIssuer));
                }
            }
        }

        // Entra ID "wids" claim contains Directory Role template IDs.
        // Global Administrator = 62e90394-69f5-4237-9190-012177145e10
        var adminRole = authOptions.Value.AdminRole;
        foreach (var widClaim in identity.FindAll("wids"))
        {
            if (widClaim.Value == "62e90394-69f5-4237-9190-012177145e10")
            {
                identity.AddClaim(new Claim(ClaimTypes.Role, adminRole, ClaimValueTypes.String, TransformIssuer));
            }
        }

        return Task.FromResult(principal);
    }

    private static Dictionary<string, string> ParseGroupMappings(string[] mappings)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var mapping in mappings)
        {
            var separatorIndex = mapping.IndexOf('=');
            if (separatorIndex > 0 && separatorIndex < mapping.Length - 1)
            {
                var groupId = mapping[..separatorIndex].Trim();
                var role = mapping[(separatorIndex + 1)..].Trim();
                result[groupId] = role;
            }
        }

        return result;
    }
}
