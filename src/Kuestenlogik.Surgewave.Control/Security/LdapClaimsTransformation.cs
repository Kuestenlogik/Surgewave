using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Transforms LDAP group memberships into standard .NET Role claims
/// using the configured GroupRoleMappings.
/// </summary>
public sealed class LdapClaimsTransformation : IClaimsTransformation
{
    private const string TransformIssuer = "LdapTransform";
    private const string LdapGroupClaimType = "ldap:groups";

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal) =>
        Task.FromResult(principal);

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal, IdpProviderConfig providerConfig)
    {
        if (principal.Identity is not ClaimsIdentity identity)
            return Task.FromResult(principal);

        // Idempotency — already transformed
        if (identity.HasClaim(c => c.Type == ClaimTypes.Role && c.Issuer == TransformIssuer))
            return Task.FromResult(principal);

        var groupMappings = ParseGroupMappings(providerConfig.GroupRoleMappings);

        foreach (var groupClaim in identity.FindAll(LdapGroupClaimType))
        {
            if (groupMappings.TryGetValue(groupClaim.Value, out var surgewaveRole))
            {
                identity.AddClaim(new Claim(ClaimTypes.Role, surgewaveRole, ClaimValueTypes.String, TransformIssuer));
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
