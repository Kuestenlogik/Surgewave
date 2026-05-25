using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Transforms Okta claims into standard .NET Role claims.
/// Reads the "groups" claim (Okta sends groups as individual string claims)
/// and maps them to Surgewave roles via <see cref="IdpProviderConfig.GroupRoleMappings"/>.
/// </summary>
public sealed class OktaClaimsTransformation : IClaimsTransformation
{
    private const string TransformIssuer = "OktaTransform";

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal) =>
        Task.FromResult(principal);

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal, IdpProviderConfig providerConfig)
    {
        if (principal.Identity is not ClaimsIdentity identity)
            return Task.FromResult(principal);

        // Idempotency — already transformed
        if (identity.HasClaim(c => c.Type == ClaimTypes.Role && c.Issuer == TransformIssuer))
            return Task.FromResult(principal);

        // Okta sends groups as individual "groups" claims
        var groupMappings = ParseGroupMappings(providerConfig.GroupRoleMappings);

        foreach (var groupClaim in identity.FindAll("groups"))
        {
            if (string.IsNullOrEmpty(groupClaim.Value))
                continue;

            // Direct group-to-role mapping
            if (groupMappings.TryGetValue(groupClaim.Value, out var surgewaveRole))
            {
                identity.AddClaim(new Claim(ClaimTypes.Role, surgewaveRole, ClaimValueTypes.String, TransformIssuer));
            }
            else
            {
                // Pass through group name as role if no explicit mapping exists
                identity.AddClaim(new Claim(ClaimTypes.Role, groupClaim.Value, ClaimValueTypes.String, TransformIssuer));
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
                var groupName = mapping[..separatorIndex].Trim();
                var role = mapping[(separatorIndex + 1)..].Trim();
                result[groupName] = role;
            }
        }

        return result;
    }
}
