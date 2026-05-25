using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

public sealed class ConnectorPackageInfoTests
{
    private static ConnectorPackageInfo Minimal() => new()
    {
        PackageId = "x",
        Version = "1.0.0",
        Name = "X",
    };

    [Fact]
    public void HasDependencies_NoDependencies_False()
    {
        var info = Minimal();
        Assert.False(info.HasDependencies);
    }

    [Fact]
    public void HasDependencies_WithDependency_True()
    {
        var info = Minimal() with
        {
            Dependencies = [new ConnectorDependencyInfo { PackageId = "y" }],
        };
        Assert.True(info.HasDependencies);
    }

    [Fact]
    public void DefaultLists_AreEmpty()
    {
        var info = Minimal();
        Assert.Empty(info.Tags);
        Assert.Empty(info.AvailableVersions);
        Assert.Empty(info.ConnectorTypes);
        Assert.Empty(info.Dependencies);
    }

    [Fact]
    public void DefaultFlags_AreFalseAndZero()
    {
        var info = Minimal();
        Assert.False(info.IsInstalled);
        Assert.False(info.IsSigned);
        Assert.Equal(0, info.DownloadCount);
        Assert.Null(info.InstalledVersion);
        Assert.Null(info.SignerIdentity);
        Assert.Null(info.SignerProvider);
    }

    [Fact]
    public void RecordEquality_SameValues_AreEqual()
    {
        var a = Minimal();
        var b = Minimal();
        Assert.Equal(a, b);
    }

    [Fact]
    public void RecordEquality_DifferentVersion_NotEqual()
    {
        var a = Minimal();
        var b = a with { Version = "2.0.0" };
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void ConnectorDependencyInfo_DefaultsToWildcardAndRequired()
    {
        var dep = new ConnectorDependencyInfo { PackageId = "x" };
        Assert.Equal("*", dep.VersionConstraint);
        Assert.False(dep.Optional);
    }
}
