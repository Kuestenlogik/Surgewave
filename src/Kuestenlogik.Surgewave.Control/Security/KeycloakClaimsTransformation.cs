using System.Security.Claims;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Transforms Keycloak realm_access.roles JSON array into standard .NET Role claims
/// so that User.IsInRole() and [Authorize(Roles = "...")] work correctly.
/// </summary>
public sealed class KeycloakClaimsTransformation : IClaimsTransformation
{
    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal) =>
        Task.FromResult(principal);

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal, IdpProviderConfig providerConfig)
    {
        if (principal.Identity is not ClaimsIdentity identity)
            return Task.FromResult(principal);

        // Already transformed — skip
        if (identity.HasClaim(c => c.Type == ClaimTypes.Role && c.Issuer == "KeycloakTransform"))
            return Task.FromResult(principal);

        var roleClaimType = providerConfig.RoleClaimType;

        // Keycloak embeds roles in realm_access: { "roles": ["role1", "role2"] }
        var realmAccessClaim = identity.FindFirst(roleClaimType);
        if (realmAccessClaim is not null)
        {
            AddRolesFromJson(identity, realmAccessClaim.Value);
            return Task.FromResult(principal);
        }

        // Some providers put roles in resource_access.<clientId>.roles
        var resourceAccessClaim = identity.FindFirst("resource_access");
        if (resourceAccessClaim is not null)
        {
            AddRolesFromResourceAccess(identity, resourceAccessClaim.Value, providerConfig.ClientId);
            return Task.FromResult(principal);
        }

        // Standard "roles" claim as JSON array
        var rolesClaim = identity.FindFirst("roles");
        if (rolesClaim is not null)
        {
            AddRolesFromJson(identity, rolesClaim.Value);
        }

        return Task.FromResult(principal);
    }

    private static void AddRolesFromJson(ClaimsIdentity identity, string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            // If it's an object with "roles" array (Keycloak realm_access format)
            if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("roles", out var rolesArray))
            {
                AddRolesFromArray(identity, rolesArray);
            }
            // If it's directly an array
            else if (root.ValueKind == JsonValueKind.Array)
            {
                AddRolesFromArray(identity, root);
            }
        }
        catch (JsonException)
        {
            // Not JSON — treat as single role value
            identity.AddClaim(new Claim(ClaimTypes.Role, json, ClaimValueTypes.String, "KeycloakTransform"));
        }
    }

    private static void AddRolesFromResourceAccess(ClaimsIdentity identity, string json, string clientId)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (root.ValueKind != JsonValueKind.Object)
                return;

            // Extract roles from client-specific access
            if (root.TryGetProperty(clientId, out var clientAccess)
                && clientAccess.TryGetProperty("roles", out var clientRoles))
            {
                AddRolesFromArray(identity, clientRoles);
            }

            // Also extract account roles if present
            if (root.TryGetProperty("account", out var accountAccess)
                && accountAccess.TryGetProperty("roles", out var accountRoles))
            {
                AddRolesFromArray(identity, accountRoles);
            }
        }
        catch (JsonException)
        {
            // Ignore malformed JSON
        }
    }

    private static void AddRolesFromArray(ClaimsIdentity identity, JsonElement rolesArray)
    {
        foreach (var role in rolesArray.EnumerateArray())
        {
            var roleName = role.GetString();
            if (!string.IsNullOrEmpty(roleName))
            {
                identity.AddClaim(new Claim(ClaimTypes.Role, roleName, ClaimValueTypes.String, "KeycloakTransform"));
            }
        }
    }
}
