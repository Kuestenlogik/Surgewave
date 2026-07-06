using System.Text.Json;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services.Security;

/// <summary>
/// Default <see cref="IRoleService"/>. Singleton: the RoleManagement page and
/// the claims transformation share one state instance, persisted through
/// <see cref="RoleStore"/> on every mutation. Built-in roles are guaranteed to
/// exist. All reads return deep clones so callers cannot mutate shared state.
/// Blocking file I/O runs outside the lock the auth hot path contends on.
/// </summary>
public sealed class RoleService : IRoleService
{
    private readonly Lock _gate = new();       // guards _state (auth hot path contends on this)
    private readonly Lock _writeGate = new();  // serializes disk writes without blocking the hot path
    private readonly RoleStore _store;
    private readonly ILogger<RoleService>? _logger;
    private RoleManagementState _state;
    private long _writeSequence;   // bumped under _gate per mutation
    private long _lastWrittenSeq;  // guarded by _writeGate

    public RoleService(RoleStore store, ILogger<RoleService>? logger = null)
    {
        _store = store;
        _logger = logger;
        _state = store.Load();
        EnsureBuiltInRoles();
        _store.Save(_state); // persist any newly added built-ins at startup
    }

    public event Action? Changed;

    public IReadOnlyList<RoleDefinition> GetRoles()
    {
        lock (_gate) return DeepClone(_state.Roles);
    }

    public IReadOnlyDictionary<string, IReadOnlyList<string>> GetUserRoles()
    {
        lock (_gate)
        {
            return _state.UserRoles.ToDictionary(
                kvp => kvp.Key,
                kvp => (IReadOnlyList<string>)[.. kvp.Value],
                StringComparer.OrdinalIgnoreCase);
        }
    }

    public IReadOnlyList<string> GetRolesForUser(string user)
    {
        if (string.IsNullOrWhiteSpace(user))
            return [];

        lock (_gate)
            return _state.UserRoles.TryGetValue(user, out var roles) ? [.. roles] : [];
    }

    public IReadOnlyCollection<string> GetPermissionsForUser(string user)
    {
        if (string.IsNullOrWhiteSpace(user))
            return [];

        lock (_gate)
        {
            if (!_state.UserRoles.TryGetValue(user, out var roleNames) || roleNames.Count == 0)
                return [];

            var permissions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var role in _state.Roles.Where(r => roleNames.Contains(r.Name)))
                permissions.UnionWith(role.Permissions ?? []); // defense in depth against a null list
            return permissions;
        }
    }

    public bool SaveRole(RoleDefinition role)
    {
        ArgumentNullException.ThrowIfNull(role);
        if (string.IsNullOrWhiteSpace(role.Name))
            throw new ArgumentException("Role name is required", nameof(role));

        string json;
        long seq;
        lock (_gate)
        {
            var existing = _state.Roles.FirstOrDefault(r => r.Name == role.Name);
            if (existing is not null)
            {
                // Built-in roles keep their identity; only description/permissions change.
                existing.Description = role.Description;
                existing.Permissions = [.. role.Permissions];
            }
            else
            {
                _state.Roles.Add(new RoleDefinition
                {
                    Name = role.Name,
                    Description = role.Description,
                    Permissions = [.. role.Permissions],
                    Members = [.. role.Members],
                    IsBuiltIn = false,
                });
            }
            seq = ++_writeSequence;
            json = _store.Serialize(_state);
        }
        return Persist(json, seq);
    }

    public bool DeleteRole(string roleName)
    {
        if (string.IsNullOrWhiteSpace(roleName) || RolePermissions.IsBuiltInName(roleName))
            return false;

        string json;
        long seq;
        lock (_gate)
        {
            if (_state.Roles.RemoveAll(r => r.Name == roleName) == 0)
                return false;

            foreach (var assigned in _state.UserRoles.Values)
                assigned.Remove(roleName);
            foreach (var user in _state.UserRoles.Where(kvp => kvp.Value.Count == 0).Select(kvp => kvp.Key).ToList())
                _state.UserRoles.Remove(user);
            seq = ++_writeSequence;
            json = _store.Serialize(_state);
        }
        return Persist(json, seq);
    }

    public bool AssignRole(string user, string roleName)
    {
        if (string.IsNullOrWhiteSpace(user) || string.IsNullOrWhiteSpace(roleName))
            return false;

        string json;
        long seq;
        lock (_gate)
        {
            if (_state.Roles.All(r => r.Name != roleName))
                return false; // never assign a role that does not exist

            if (!_state.UserRoles.TryGetValue(user, out var roles))
            {
                roles = [];
                _state.UserRoles[user] = roles;
            }
            if (roles.Contains(roleName))
                return true; // already assigned, nothing to persist

            roles.Add(roleName);
            SyncMembers();
            seq = ++_writeSequence;
            json = _store.Serialize(_state);
        }
        return Persist(json, seq);
    }

    public bool RemoveRole(string user, string roleName)
    {
        if (string.IsNullOrWhiteSpace(user) || string.IsNullOrWhiteSpace(roleName))
            return false;

        string json;
        long seq;
        lock (_gate)
        {
            if (!_state.UserRoles.TryGetValue(user, out var roles) || !roles.Remove(roleName))
                return false;
            if (roles.Count == 0)
                _state.UserRoles.Remove(user);
            SyncMembers();
            seq = ++_writeSequence;
            json = _store.Serialize(_state);
        }
        return Persist(json, seq);
    }

    public bool ReplaceAll(RoleManagementState state)
    {
        ArgumentNullException.ThrowIfNull(state);

        string json;
        long seq;
        lock (_gate)
        {
            // Build the new state fully into a local (normalized, built-ins
            // re-ensured, members synced) BEFORE swapping _state, so a throw
            // cannot leave the shared singleton half-updated.
            var normalized = RoleStore.Normalize(state);
            EnsureBuiltInRolesInto(normalized);
            SyncMembersOf(normalized);
            _state = normalized;
            seq = ++_writeSequence;
            json = _store.Serialize(_state);
        }
        return Persist(json, seq);
    }

    private bool Persist(string json, long seq)
    {
        bool ok;
        // Serialize writes on a dedicated lock (not the hot-path _gate) so two
        // concurrent mutations cannot collide on disk or reorder: an older
        // snapshot is never written over a newer one.
        lock (_writeGate)
        {
            if (seq <= _lastWrittenSeq)
            {
                // A newer state — which already includes this mutation — reached
                // disk first; skip the stale write instead of reordering it.
                ok = true;
            }
            else
            {
                ok = _store.WriteAtomic(json);
                if (ok)
                    _lastWrittenSeq = seq;
                else
                    _logger?.LogError("Role state mutation was not persisted to disk; it will be lost on restart");
            }
        }
        Changed?.Invoke();
        return ok;
    }

    private void EnsureBuiltInRoles() => EnsureBuiltInRolesInto(_state);

    private static void EnsureBuiltInRolesInto(RoleManagementState state)
    {
        foreach (var name in new[] { RolePermissions.Admin, RolePermissions.Operator, RolePermissions.Developer, RolePermissions.Viewer })
        {
            if (state.Roles.Any(r => r.Name == name))
                continue;

            state.Roles.Add(new RoleDefinition
            {
                Name = name,
                Description = RolePermissions.BuiltInDescriptions[name],
                Permissions = [.. RolePermissions.BuiltInPermissions[name]],
                IsBuiltIn = true,
            });
        }
    }

    private void SyncMembers() => SyncMembersOf(_state);

    /// <summary>Rebuild each role's Members list from the authoritative user→role map.</summary>
    private static void SyncMembersOf(RoleManagementState state)
    {
        foreach (var role in state.Roles)
        {
            role.Members = state.UserRoles
                .Where(kvp => kvp.Value.Contains(role.Name))
                .Select(kvp => kvp.Key)
                .ToList();
        }
    }

    private static List<RoleDefinition> DeepClone(List<RoleDefinition> roles)
        => JsonSerializer.Deserialize<List<RoleDefinition>>(JsonSerializer.Serialize(roles))!;
}
