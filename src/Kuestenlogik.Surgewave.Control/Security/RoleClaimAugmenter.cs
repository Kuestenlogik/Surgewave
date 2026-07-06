using System.Security.Claims;
using Kuestenlogik.Surgewave.Control.Services.Security;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Adds role claims to an authenticated principal based on the server-side role
/// store, mapping the store's dotted permissions onto the ASP.NET policy roles.
/// Extracted from the claims transformation so the augmentation is testable
/// without constructing the provider-routing composite.
/// </summary>
public sealed class RoleClaimAugmenter
{
    // Identity claims the RoleManagement UI may key assignments on, matched in
    // order so an admin can assign by username or email regardless of which the
    // IdP populates.
    private static readonly string[] IdentityClaimTypes =
    [
        ClaimTypes.Name,
        "preferred_username",
        "email",
        ClaimTypes.Email,
        "sub",
        ClaimTypes.NameIdentifier,
    ];

    private readonly IRoleService _roleService;
    private readonly IReadOnlyDictionary<string, string> _permissionToRole;

    public RoleClaimAugmenter(IRoleService roleService, string adminRole)
    {
        _roleService = roleService;
        _permissionToRole = RolePermissionMapping.Build(adminRole);
    }

    public void Augment(ClaimsPrincipal principal)
    {
        if (principal.Identity is not ClaimsIdentity { IsAuthenticated: true } identity)
            return;

        var permissions = ResolvePermissions(identity);
        if (permissions.Count == 0)
            return;

        var roleClaimType = identity.RoleClaimType;
        foreach (var permission in permissions)
        {
            if (!_permissionToRole.TryGetValue(permission, out var role))
                continue; // display-only permission with no enforcing policy
            if (!principal.IsInRole(role))
                identity.AddClaim(new Claim(roleClaimType, role));
        }
    }

    private HashSet<string> ResolvePermissions(ClaimsIdentity identity)
    {
        var permissions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var seenKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var claimType in IdentityClaimTypes)
        {
            var value = identity.FindFirst(claimType)?.Value;
            if (string.IsNullOrWhiteSpace(value) || !seenKeys.Add(value))
                continue;

            permissions.UnionWith(_roleService.GetPermissionsForUser(value));
        }

        return permissions;
    }
}
