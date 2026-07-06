using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services.Security;

/// <summary>
/// Server-side RBAC store: role definitions and user→role assignments,
/// persisted on the Control host so they are enforced (via the claims
/// transformation) rather than living only in a browser. Singleton — shared by
/// the RoleManagement page and the claims transformation.
/// </summary>
public interface IRoleService
{
    /// <summary>Raised after any change to roles or assignments.</summary>
    event Action? Changed;

    IReadOnlyList<RoleDefinition> GetRoles();

    /// <summary>All user→role-name assignments (user key → role names).</summary>
    IReadOnlyDictionary<string, IReadOnlyList<string>> GetUserRoles();

    /// <summary>Role names assigned to a single user (case-insensitive lookup).</summary>
    IReadOnlyList<string> GetRolesForUser(string user);

    /// <summary>
    /// The distinct dotted permission strings a user effectively holds, unioned
    /// across all their assigned roles.
    /// </summary>
    IReadOnlyCollection<string> GetPermissionsForUser(string user);

    /// <summary>Create or update a role. Returns whether the change reached disk.</summary>
    bool SaveRole(RoleDefinition role);

    /// <summary>Delete a custom role (built-ins are protected). Returns whether it was removed and persisted.</summary>
    bool DeleteRole(string roleName);

    /// <summary>Assign an existing role to a user. Returns whether the change reached disk.</summary>
    bool AssignRole(string user, string roleName);

    /// <summary>Remove a role from a user. Returns whether the change reached disk.</summary>
    bool RemoveRole(string user, string roleName);

    /// <summary>
    /// Replace the whole state at once (used by the one-time migration from the
    /// legacy browser LocalStorage blob). Built-in roles are re-ensured. Returns
    /// whether the change reached disk.
    /// </summary>
    bool ReplaceAll(RoleManagementState state);
}
