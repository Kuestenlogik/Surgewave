using Kuestenlogik.Surgewave.Control.Models;
using Kuestenlogik.Surgewave.Control.Services.Security;

namespace Kuestenlogik.Surgewave.Control.Tests.Services;

/// <summary>
/// Persistence + robustness tests for <see cref="RoleStore"/> (#37). A
/// null-list or garbage roles.json must degrade to an empty document rather
/// than throwing into the claims transformation on every request.
/// </summary>
public sealed class RoleStoreTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"surgewave-roles-test-{Guid.NewGuid():N}");
    private readonly string _path;

    public RoleStoreTests() => _path = Path.Combine(_directory, "roles.json");

    public void Dispose()
    {
        if (Directory.Exists(_directory))
            Directory.Delete(_directory, recursive: true);
    }

    [Fact]
    public void Load_MissingFile_ReturnsEmptyDocument()
    {
        var state = new RoleStore(_path).Load();

        Assert.Empty(state.Roles);
        Assert.Empty(state.UserRoles);
    }

    [Fact]
    public void SaveThenLoad_RoundTripsRolesAndAssignments()
    {
        var store = new RoleStore(_path);
        store.Save(new RoleManagementState
        {
            Roles = [new RoleDefinition { Name = "Ops", Permissions = ["topics.manage"] }],
            UserRoles = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase) { ["alice"] = ["Ops"] },
        });

        var loaded = new RoleStore(_path).Load();

        Assert.Equal("Ops", Assert.Single(loaded.Roles).Name);
        Assert.Equal("Ops", Assert.Single(loaded.UserRoles["alice"]));
    }

    [Fact]
    public void Load_RestoresCaseInsensitiveUserRoleLookup()
    {
        new RoleStore(_path).Save(new RoleManagementState
        {
            UserRoles = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase) { ["Alice"] = ["Admin"] },
        });

        var loaded = new RoleStore(_path).Load();

        // The comparer is lost on a naive JSON round-trip; Normalize must restore it.
        Assert.True(loaded.UserRoles.ContainsKey("alice"));
        Assert.True(loaded.UserRoles.ContainsKey("ALICE"));
    }

    [Fact]
    public void Load_NullCollections_NormalizedToEmpty()
    {
        Directory.CreateDirectory(_directory);
        File.WriteAllText(_path, """{"roles":null,"userRoles":null}""");

        var state = new RoleStore(_path).Load();

        Assert.NotNull(state.Roles);
        Assert.NotNull(state.UserRoles);
        Assert.Empty(state.Roles);
        Assert.Empty(state.UserRoles);
    }

    [Fact]
    public void Load_NullRoleElements_AreDropped()
    {
        Directory.CreateDirectory(_directory);
        File.WriteAllText(_path, """{"roles":[null],"userRoles":{}}""");

        var state = new RoleStore(_path).Load();

        Assert.Empty(state.Roles);
    }

    [Fact]
    public void Load_NullInnerPermissionAndMemberLists_CoalescedToEmpty()
    {
        // The security hot path (GetPermissionsForUser) does role.Permissions
        // unioning; a null inner list would ArgumentNullException on every request.
        Directory.CreateDirectory(_directory);
        File.WriteAllText(_path, """{"roles":[{"name":"Viewer","permissions":null,"members":null}],"userRoles":{"alice":["Viewer"]}}""");

        var viewer = Assert.Single(new RoleStore(_path).Load().Roles);

        Assert.NotNull(viewer.Permissions);
        Assert.Empty(viewer.Permissions);
        Assert.NotNull(viewer.Members);
        Assert.Empty(viewer.Members);
    }

    [Fact]
    public void WriteAtomic_ReturnsTrueOnSuccess()
    {
        var store = new RoleStore(_path);

        Assert.True(store.WriteAtomic(store.Serialize(new RoleManagementState())));
        Assert.True(File.Exists(_path));
    }

    [Fact]
    public void Load_GarbageFile_ReturnsEmptyDocument()
    {
        Directory.CreateDirectory(_directory);
        File.WriteAllText(_path, "not valid json {");

        var state = new RoleStore(_path).Load();

        Assert.Empty(state.Roles);
    }
}
