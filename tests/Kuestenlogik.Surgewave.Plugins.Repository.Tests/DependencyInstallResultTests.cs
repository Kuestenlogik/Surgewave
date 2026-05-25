using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

public sealed class DependencyInstallResultTests
{
    [Fact]
    public void Succeeded_NewInstall_TracksAllProperties()
    {
        var installed = new InstalledPackageInfo[]
        {
            new() { PackageId = "a", Version = "1.0.0" },
            new() { PackageId = "b", Version = "1.0.0", IsDependency = true },
        };
        var already = new InstalledPackageInfo[]
        {
            new() { PackageId = "c", Version = "1.0.0" },
        };

        var r = DependencyInstallResult.Succeeded(installed, already);

        Assert.True(r.IsSuccess);
        Assert.False(r.IsPartialSuccess);
        Assert.Equal(installed, r.InstalledPackages);
        Assert.Equal(already, r.AlreadyInstalledPackages);
        Assert.Equal(3, r.TotalPackages);
        Assert.Equal(2, r.NewlyInstalled);
        Assert.Equal(0, r.Upgraded);
        Assert.Empty(r.Errors);
        Assert.Empty(r.Warnings);
    }

    [Fact]
    public void Succeeded_WithUpgrades_CountsCorrectly()
    {
        var installed = new InstalledPackageInfo[]
        {
            new() { PackageId = "a", Version = "2.0.0", WasUpgraded = true, PreviousVersion = "1.0.0" },
            new() { PackageId = "b", Version = "1.0.0" },
        };

        var r = DependencyInstallResult.Succeeded(installed, []);

        Assert.Equal(1, r.NewlyInstalled);
        Assert.Equal(1, r.Upgraded);
    }

    [Fact]
    public void PartialSuccess_TracksInstalledAndErrors()
    {
        var installed = new InstalledPackageInfo[] { new() { PackageId = "a", Version = "1.0.0" } };
        var errors = new[] { "b failed" };

        var r = DependencyInstallResult.PartialSuccess(installed, errors);

        Assert.False(r.IsSuccess);
        Assert.True(r.IsPartialSuccess);
        Assert.Single(r.InstalledPackages);
        Assert.Single(r.Errors, "b failed");
    }

    [Fact]
    public void Failed_OnlyHasErrors()
    {
        var errors = new[] { "nope-1", "nope-2" };

        var r = DependencyInstallResult.Failed(errors);

        Assert.False(r.IsSuccess);
        Assert.False(r.IsPartialSuccess);
        Assert.Empty(r.InstalledPackages);
        Assert.Equal(errors, r.Errors);
    }

    [Fact]
    public void InstalledPackageInfo_RecordEquality_ByValue()
    {
        var a = new InstalledPackageInfo { PackageId = "x", Version = "1.0.0" };
        var b = new InstalledPackageInfo { PackageId = "x", Version = "1.0.0" };

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void PackagePublishResult_Succeeded_PopulatesFields()
    {
        var r = PackagePublishResult.Succeeded("x", "1.0.0", "/registry/x");

        Assert.True(r.Success);
        Assert.Equal("x", r.PackageId);
        Assert.Equal("1.0.0", r.Version);
        Assert.Equal("/registry/x", r.RegistryPath);
        Assert.Null(r.Error);
        Assert.Empty(r.Warnings);
    }

    [Fact]
    public void PackagePublishResult_Failed_CarriesError()
    {
        var r = PackagePublishResult.Failed("registry rejected");

        Assert.False(r.Success);
        Assert.Equal("registry rejected", r.Error);
        Assert.Null(r.PackageId);
    }

    [Fact]
    public void PackagePublishResult_SucceededWithWarnings_PreservesThem()
    {
        var warnings = new[] { "license missing" };

        var r = PackagePublishResult.Succeeded("x", "1.0.0", "/registry/x", warnings);

        Assert.True(r.Success);
        Assert.Equal(warnings, r.Warnings);
    }
}
