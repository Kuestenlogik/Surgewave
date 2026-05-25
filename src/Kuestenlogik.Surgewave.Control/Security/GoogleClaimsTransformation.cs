using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Transforms Google Workspace claims into standard .NET Role claims.
/// Validates the "hd" (hosted domain) claim and maps email-based roles
/// via <see cref="IdpProviderConfig.GroupRoleMappings"/>.
/// </summary>
public sealed class GoogleClaimsTransformation : IClaimsTransformation
{
    private const string TransformIssuer = "GoogleTransform";

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal) =>
        Task.FromResult(principal);

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal, IdpProviderConfig providerConfig)
    {
        if (principal.Identity is not ClaimsIdentity identity)
            return Task.FromResult(principal);

        // Idempotency — already transformed
        if (identity.HasClaim(c => c.Type == ClaimTypes.Role && c.Issuer == TransformIssuer))
            return Task.FromResult(principal);

        // Validate hosted domain if configured
        if (!string.IsNullOrEmpty(providerConfig.GoogleHostedDomain))
        {
            var hdClaim = identity.FindFirst("hd");
            if (hdClaim is null || !string.Equals(hdClaim.Value, providerConfig.GoogleHostedDomain, StringComparison.OrdinalIgnoreCase))
            {
                // Domain mismatch — do not assign any roles
                return Task.FromResult(principal);
            }
        }

        // Google has no native app roles — map via GroupRoleMappings using email
        var email = identity.FindFirst(ClaimTypes.Email)?.Value
                    ?? identity.FindFirst("email")?.Value;

        if (!string.IsNullOrEmpty(email))
        {
            var mappings = ParseGroupMappings(providerConfig.GroupRoleMappings);

            foreach (var (key, role) in mappings)
            {
                // Support email-based ("user@example.com=surgewave-admin") and
                // domain-based ("example.com=surgewave-viewer") mappings
                if (string.Equals(key, email, StringComparison.OrdinalIgnoreCase)
                    || email.EndsWith($"@{key}", StringComparison.OrdinalIgnoreCase))
                {
                    identity.AddClaim(new Claim(ClaimTypes.Role, role, ClaimValueTypes.String, TransformIssuer));
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
                var key = mapping[..separatorIndex].Trim();
                var role = mapping[(separatorIndex + 1)..].Trim();
                result[key] = role;
            }
        }

        return result;
    }
}
