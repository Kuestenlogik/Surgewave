using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services.Security;

/// <summary>
/// File-backed persistence for RBAC state (role definitions + user→role
/// assignments). Mirrors the alerting store: atomic temp-file+move writes,
/// tolerant loads that degrade to an empty document, owner-only permissions on
/// Unix. This is what turns the RoleManagement page from browser-local preview
/// into server-enforced roles.
/// </summary>
public sealed class RoleStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly string _path;
    private readonly ILogger<RoleStore>? _logger;

    public RoleStore(string path, ILogger<RoleStore>? logger = null)
    {
        _path = path;
        _logger = logger;
    }

    /// <summary>Load persisted RBAC state; returns an empty document when the file is missing or unreadable.</summary>
    public RoleManagementState Load()
    {
        try
        {
            if (!File.Exists(_path))
                return new RoleManagementState();

            var json = File.ReadAllText(_path);
            var state = JsonSerializer.Deserialize<RoleManagementState>(json, SerializerOptions)
                ?? new RoleManagementState();
            return Normalize(state);
        }
        catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException)
        {
            _logger?.LogWarning(ex, "Could not read role state from {Path} — starting with empty state", _path);
            return new RoleManagementState();
        }
    }

    /// <summary>Serialize + write atomically; returns whether the write reached disk.</summary>
    public bool Save(RoleManagementState state) => WriteAtomic(Serialize(state));

    /// <summary>Serialize to JSON (cheap; callers hold this under a lock, then write outside it).</summary>
    public string Serialize(RoleManagementState state) => JsonSerializer.Serialize(state, SerializerOptions);

    /// <summary>
    /// Atomically write pre-serialized JSON (temp file + move). Returns false on
    /// an I/O failure so callers can surface a persistence error instead of
    /// silently diverging from disk. Kept out of any lock — this is blocking I/O.
    /// </summary>
    public bool WriteAtomic(string json)
    {
        // Unique temp path so concurrent writers never collide on one temp file.
        var tempPath = $"{_path}.{Guid.NewGuid():N}.tmp";
        try
        {
            var directory = Path.GetDirectoryName(_path);
            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(tempPath, json);
            RestrictPermissions(tempPath);
            File.Move(tempPath, _path, overwrite: true);
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger?.LogError(ex, "Could not persist role state to {Path}", _path);
            TryDelete(tempPath);
            return false;
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Best effort — an orphaned temp file is harmless.
        }
    }

    /// <summary>
    /// Coalesce null collections and drop null elements — including each role's
    /// inner Permissions/Members lists (System.Text.Json overwrites the `= []`
    /// initializer with null for an explicit JSON null) — and restore the
    /// case-insensitive user-role comparer (lost on round-trip). Without this a
    /// hand-edited roles.json with a null array null-derefs the claims
    /// transformation on every request.
    /// </summary>
    public static RoleManagementState Normalize(RoleManagementState state)
    {
        var roles = new List<RoleDefinition>();
        foreach (var role in state.Roles ?? [])
        {
            if (role is null)
                continue;
            role.Permissions = (role.Permissions ?? []).Where(p => !string.IsNullOrWhiteSpace(p)).ToList();
            role.Members = (role.Members ?? []).Where(m => !string.IsNullOrWhiteSpace(m)).ToList();
            roles.Add(role);
        }

        var userRoles = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var (user, assigned) in state.UserRoles ?? [])
        {
            if (string.IsNullOrWhiteSpace(user))
                continue;
            userRoles[user] = (assigned ?? []).Where(r => !string.IsNullOrWhiteSpace(r)).ToList();
        }

        return new RoleManagementState { Roles = roles, UserRoles = userRoles };
    }

    private void RestrictPermissions(string path)
    {
        if (OperatingSystem.IsWindows())
            return;

        try
        {
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            _logger?.LogDebug(ex, "Could not restrict permissions on {Path}", path);
        }
    }
}
