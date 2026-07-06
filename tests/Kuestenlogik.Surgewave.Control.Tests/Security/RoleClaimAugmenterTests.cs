using System.Security.Claims;
using Kuestenlogik.Surgewave.Control.Models;
using Kuestenlogik.Surgewave.Control.Security;
using Kuestenlogik.Surgewave.Control.Services.Security;

namespace Kuestenlogik.Surgewave.Control.Tests.Security;

/// <summary>
/// The bridge that makes stored roles real (#37): the augmenter maps a user's
/// stored permissions onto the ASP.NET policy role claims that RequireRole
/// checks. Unmapped permissions and unauthenticated principals are left alone.
/// </summary>
public sealed class RoleClaimAugmenterTests : IDisposable
{
    private const string AdminRole = "surgewave-admin";

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"surgewave-aug-{Guid.NewGuid():N}");
    private readonly RoleService _roleService;

    public RoleClaimAugmenterTests()
    {
        _roleService = new RoleService(new RoleStore(Path.Combine(_directory, "roles.json")));
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
            Directory.Delete(_directory, recursive: true);
    }

    private static ClaimsPrincipal AuthenticatedUser(string? name = null, string? email = null)
    {
        var claims = new List<Claim>();
        if (name is not null) claims.Add(new Claim(ClaimTypes.Name, name));
        if (email is not null) claims.Add(new Claim(ClaimTypes.Email, email));
        var identity = new ClaimsIdentity(claims, authenticationType: "Test", nameType: ClaimTypes.Name, roleType: ClaimTypes.Role);
        return new ClaimsPrincipal(identity);
    }

    private RoleClaimAugmenter Augmenter() => new(_roleService, AdminRole);

    [Fact]
    public void Augment_AddsMappedRoleClaim_ForAssignedPermission()
    {
        _roleService.SaveRole(new RoleDefinition { Name = "Ops", Permissions = ["topics.manage"] });
        _roleService.AssignRole("alice", "Ops");
        var principal = AuthenticatedUser(name: "alice");

        Augmenter().Augment(principal);

        Assert.True(principal.IsInRole(SurgewavePolicies.TopicsRole));
    }

    [Fact]
    public void Augment_MapsClusterAdmin_ToConfiguredAdminRole()
    {
        _roleService.AssignRole("root", RolePermissions.Admin); // Admin grants everything incl. cluster.admin
        var principal = AuthenticatedUser(name: "root");

        Augmenter().Augment(principal);

        Assert.True(principal.IsInRole(AdminRole));
    }

    [Fact]
    public void Augment_IgnoresPermissionsWithoutAPolicy()
    {
        // alerts.manage has no enforcing policy role — it must not add a claim.
        _roleService.SaveRole(new RoleDefinition { Name = "Alerters", Permissions = ["alerts.manage"] });
        _roleService.AssignRole("alice", "Alerters");
        var principal = AuthenticatedUser(name: "alice");

        Augmenter().Augment(principal);

        Assert.DoesNotContain(principal.Claims, c => c.Type == ClaimTypes.Role);
    }

    [Fact]
    public void Augment_MatchesByEmailClaim_NotJustName()
    {
        _roleService.SaveRole(new RoleDefinition { Name = "Ops", Permissions = ["topics.manage"] });
        _roleService.AssignRole("alice@example.com", "Ops");
        var principal = AuthenticatedUser(email: "alice@example.com");

        Augmenter().Augment(principal);

        Assert.True(principal.IsInRole(SurgewavePolicies.TopicsRole));
    }

    [Fact]
    public void Augment_UnauthenticatedPrincipal_IsUnchanged()
    {
        _roleService.SaveRole(new RoleDefinition { Name = "Ops", Permissions = ["topics.manage"] });
        _roleService.AssignRole("alice", "Ops");
        var anonymous = new ClaimsPrincipal(new ClaimsIdentity()); // not authenticated

        Augmenter().Augment(anonymous);

        Assert.Empty(anonymous.Claims);
    }

    [Fact]
    public void Augment_DoesNotDuplicateExistingRoleClaim()
    {
        _roleService.SaveRole(new RoleDefinition { Name = "Ops", Permissions = ["topics.manage"] });
        _roleService.AssignRole("alice", "Ops");
        var principal = AuthenticatedUser(name: "alice");
        ((ClaimsIdentity)principal.Identity!).AddClaim(new Claim(ClaimTypes.Role, SurgewavePolicies.TopicsRole));

        Augmenter().Augment(principal);

        Assert.Single(principal.Claims, c => c.Type == ClaimTypes.Role && c.Value == SurgewavePolicies.TopicsRole);
    }

    [Fact]
    public void Augment_UserWithNoAssignments_AddsNothing()
    {
        var principal = AuthenticatedUser(name: "nobody");

        Augmenter().Augment(principal);

        Assert.DoesNotContain(principal.Claims, c => c.Type == ClaimTypes.Role);
    }
}
