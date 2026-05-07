using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Transforms SAML 2.0 assertion attributes into standard .NET Role claims.
/// Reads roles from configurable SAML attributes and optionally maps groups to Surgewave roles.
/// </summary>
public sealed class SamlClaimsTransformation : IClaimsTransformation
{
    private const string TransformIssuer = "SamlTransform";

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal) =>
        Task.FromResult(principal);

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal, IdpProviderConfig providerConfig)
    {
        if (principal.Identity is not ClaimsIdentity identity)
            return Task.FromResult(principal);

        // Idempotency — already transformed
        if (identity.HasClaim(c => c.Type == ClaimTypes.Role && c.Issuer == TransformIssuer))
            return Task.FromResult(principal);

        var samlConfig = providerConfig.Saml;

        // Extract roles from the configured role attribute
        foreach (var roleClaim in identity.FindAll(samlConfig.RoleAttribute))
        {
            if (!string.IsNullOrEmpty(roleClaim.Value))
            {
                identity.AddClaim(new Claim(ClaimTypes.Role, roleClaim.Value, ClaimValueTypes.String, TransformIssuer));
            }
        }

        // Map groups to Surgewave roles if group attribute is configured
        if (!string.IsNullOrEmpty(samlConfig.GroupAttribute))
        {
            var groupMappings = ParseGroupMappings(providerConfig.GroupRoleMappings);
            foreach (var groupClaim in identity.FindAll(samlConfig.GroupAttribute))
            {
                if (groupMappings.TryGetValue(groupClaim.Value, out var surgewaveRole))
                {
                    identity.AddClaim(new Claim(ClaimTypes.Role, surgewaveRole, ClaimValueTypes.String, TransformIssuer));
                }
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
