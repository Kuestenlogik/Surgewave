using Kuestenlogik.Surgewave.Control.Models;
using Kuestenlogik.Surgewave.Control.Services.Security;

namespace Kuestenlogik.Surgewave.Control.Tests.Services;

/// <summary>
/// Behaviour of the server-side role service (#37): built-in roles always
/// exist, custom roles/assignments persist across a restart, effective
/// permissions union across assigned roles, and built-in roles are protected.
/// </summary>
public sealed class RoleServiceTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"surgewave-roles-svc-{Guid.NewGuid():N}");
    private readonly string _path;

    public RoleServiceTests() => _path = Path.Combine(_directory, "roles.json");

    public void Dispose()
    {
        if (Directory.Exists(_directory))
            Directory.Delete(_directory, recursive: true);
    }

    private RoleService CreateService() => new(new RoleStore(_path));

    [Fact]
    public void Construction_EnsuresBuiltInRoles()
    {
        var service = CreateService();

        var names = service.GetRoles().Select(r => r.Name).ToList();
        Assert.Contains(RolePermissions.Admin, names);
        Assert.Contains(RolePermissions.Operator, names);
        Assert.Contains(RolePermissions.Developer, names);
        Assert.Contains(RolePermissions.Viewer, names);
        Assert.All(service.GetRoles().Where(r => RolePermissions.IsBuiltInName(r.Name)), r => Assert.True(r.IsBuiltIn));
    }

    [Fact]
    public void SaveRole_CreatesCustomRole_ThenUpdatesIt()
    {
        var service = CreateService();

        service.SaveRole(new RoleDefinition { Name = "Ops", Description = "v1", Permissions = ["topics.manage"] });
        Assert.Equal("v1", service.GetRoles().Single(r => r.Name == "Ops").Description);

        service.SaveRole(new RoleDefinition { Name = "Ops", Description = "v2", Permissions = ["topics.manage", "quotas.manage"] });
        var ops = service.GetRoles().Single(r => r.Name == "Ops");
        Assert.Equal("v2", ops.Description);
        Assert.Equal(2, ops.Permissions.Count);
        Assert.False(ops.IsBuiltIn);
    }

    [Fact]
    public void DeleteRole_RemovesCustom_ButProtectsBuiltIn()
    {
        var service = CreateService();
        service.SaveRole(new RoleDefinition { Name = "Ops", Permissions = ["topics.manage"] });

        Assert.False(service.DeleteRole(RolePermissions.Admin));
        Assert.Contains(service.GetRoles(), r => r.Name == RolePermissions.Admin);

        Assert.True(service.DeleteRole("Ops"));
        Assert.DoesNotContain(service.GetRoles(), r => r.Name == "Ops");
    }

    [Fact]
    public void AssignRole_UnknownRole_IsIgnored()
    {
        var service = CreateService();

        service.AssignRole("alice", "NoSuchRole");

        Assert.Empty(service.GetRolesForUser("alice"));
    }

    [Fact]
    public void AssignRole_ThenGetPermissions_UnionsAcrossRoles()
    {
        var service = CreateService();
        service.SaveRole(new RoleDefinition { Name = "Topics", Permissions = ["topics.manage"] });
        service.SaveRole(new RoleDefinition { Name = "Quotas", Permissions = ["quotas.manage"] });

        service.AssignRole("alice", "Topics");
        service.AssignRole("alice", "Quotas");

        var perms = service.GetPermissionsForUser("alice");
        Assert.Contains("topics.manage", perms);
        Assert.Contains("quotas.manage", perms);
    }

    [Fact]
    public void GetRolesForUser_IsCaseInsensitive()
    {
        var service = CreateService();
        service.AssignRole("Alice", RolePermissions.Viewer);

        Assert.Contains(RolePermissions.Viewer, service.GetRolesForUser("alice"));
        Assert.Contains(RolePermissions.Viewer, service.GetRolesForUser("ALICE"));
    }

    [Fact]
    public void RemoveRole_DropsAssignment_AndCleansEmptyUser()
    {
        var service = CreateService();
        service.AssignRole("alice", RolePermissions.Viewer);

        service.RemoveRole("alice", RolePermissions.Viewer);

        Assert.Empty(service.GetRolesForUser("alice"));
        Assert.DoesNotContain("alice", service.GetUserRoles().Keys);
    }

    [Fact]
    public void State_PersistsAcrossRestart()
    {
        var service = CreateService();
        service.SaveRole(new RoleDefinition { Name = "Ops", Permissions = ["topics.manage"] });
        service.AssignRole("alice", "Ops");

        var restarted = CreateService();
        Assert.Contains(restarted.GetRoles(), r => r.Name == "Ops");
        Assert.Contains("Ops", restarted.GetRolesForUser("alice"));
    }

    [Fact]
    public void GetRoles_ReturnsClones_MutationDoesNotCorruptState()
    {
        var service = CreateService();
        service.SaveRole(new RoleDefinition { Name = "Ops", Permissions = ["topics.manage"] });

        service.GetRoles().Single(r => r.Name == "Ops").Permissions.Add("cluster.admin");

        Assert.DoesNotContain("cluster.admin", service.GetRoles().Single(r => r.Name == "Ops").Permissions);
    }

    [Fact]
    public void ReplaceAll_MigratesStateAndReEnsuresBuiltIns()
    {
        var service = CreateService();

        service.ReplaceAll(new RoleManagementState
        {
            Roles = [new RoleDefinition { Name = "Legacy", Permissions = ["schemas.manage"] }],
            UserRoles = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase) { ["bob"] = ["Legacy"] },
        });

        Assert.Contains(service.GetRoles(), r => r.Name == "Legacy");
        Assert.Contains(service.GetRoles(), r => r.Name == RolePermissions.Admin); // built-ins re-ensured
        Assert.Contains("Legacy", service.GetRolesForUser("bob"));
    }

    [Fact]
    public void Changed_RaisedOnMutations()
    {
        var service = CreateService();
        var count = 0;
        service.Changed += () => count++;

        service.SaveRole(new RoleDefinition { Name = "Ops", Permissions = ["topics.manage"] });
        service.AssignRole("alice", "Ops");

        Assert.True(count >= 2);
    }

    [Fact]
    public void Mutators_ReturnTrue_OnSuccessfulPersist()
    {
        var service = CreateService();

        Assert.True(service.SaveRole(new RoleDefinition { Name = "Ops", Permissions = ["topics.manage"] }));
        Assert.True(service.AssignRole("alice", "Ops"));
        Assert.True(service.RemoveRole("alice", "Ops"));
        Assert.True(service.DeleteRole("Ops"));
    }

    [Fact]
    public void GetPermissionsForUser_RoleWithNullPermissions_DoesNotThrow()
    {
        // A hand-edited roles.json with a null permission list must not blow up
        // the claims transformation; Normalize coalesces it on load.
        Directory.CreateDirectory(_directory);
        File.WriteAllText(_path, """{"roles":[{"name":"Broken","permissions":null}],"userRoles":{"alice":["Broken"]}}""");

        var service = CreateService();

        Assert.Empty(service.GetPermissionsForUser("alice"));
    }

    [Fact]
    public void ReplaceAll_NullUserRoleList_DoesNotCorruptSingleton()
    {
        var service = CreateService();
        var crafted = new RoleManagementState
        {
            UserRoles = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase) { ["alice"] = null! },
        };

        var ok = service.ReplaceAll(crafted); // must not throw or leave state half-applied

        Assert.True(ok);
        Assert.Empty(service.GetRolesForUser("alice"));
        Assert.Contains(service.GetRoles(), r => r.Name == RolePermissions.Admin); // still usable
    }
}
