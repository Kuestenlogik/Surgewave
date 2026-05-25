using Kuestenlogik.Surgewave.Clustering.Upgrades;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Upgrades;

public class VersionCompatibilityCheckerTests
{
    private readonly VersionCompatibilityChecker _checker = new();

    [Fact]
    public void Check_EmptyCluster_IsCompatible()
    {
        var local = BrokerVersion.Parse("1.0.0");

        var result = _checker.Check(local, []);

        Assert.True(result.IsCompatible);
        Assert.Null(result.Reason);
        Assert.Empty(result.Warnings);
    }

    [Fact]
    public void Check_AllSameVersion_IsCompatible()
    {
        var local = BrokerVersion.Parse("1.2.3");
        var cluster = new List<BrokerVersion>
        {
            BrokerVersion.Parse("1.2.3"),
            BrokerVersion.Parse("1.2.3"),
        };

        var result = _checker.Check(local, cluster);

        Assert.True(result.IsCompatible);
        Assert.Null(result.Reason);
        Assert.Empty(result.Warnings);
    }

    [Fact]
    public void Check_MixedMinorVersions_CompatibleWithWarnings()
    {
        var local = BrokerVersion.Parse("1.3.0");
        var cluster = new List<BrokerVersion>
        {
            BrokerVersion.Parse("1.2.0"),
            BrokerVersion.Parse("1.2.0"),
        };

        var result = _checker.Check(local, cluster);

        Assert.True(result.IsCompatible);
        Assert.Null(result.Reason);
        Assert.NotEmpty(result.Warnings);
        Assert.All(result.Warnings, w => Assert.Contains("Minor version mismatch", w));
    }

    [Fact]
    public void Check_MixedPatchVersions_CompatibleWithWarnings()
    {
        var local = BrokerVersion.Parse("1.2.4");
        var cluster = new List<BrokerVersion>
        {
            BrokerVersion.Parse("1.2.3"),
        };

        var result = _checker.Check(local, cluster);

        Assert.True(result.IsCompatible);
        Assert.Null(result.Reason);
        Assert.Contains(result.Warnings, w => w.Contains("Patch version mismatch"));
    }

    [Fact]
    public void Check_DifferentMajorVersion_Incompatible()
    {
        var local = BrokerVersion.Parse("2.0.0");
        var cluster = new List<BrokerVersion>
        {
            BrokerVersion.Parse("1.5.0"),
            BrokerVersion.Parse("1.5.0"),
        };

        var result = _checker.Check(local, cluster);

        Assert.False(result.IsCompatible);
        Assert.NotNull(result.Reason);
        Assert.Contains("incompatible", result.Reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Check_MixedIncompatibleVersions_ReportsAll()
    {
        var local = BrokerVersion.Parse("2.0.0");
        var cluster = new List<BrokerVersion>
        {
            BrokerVersion.Parse("1.0.0"),
            BrokerVersion.Parse("3.0.0"),
        };

        var result = _checker.Check(local, cluster);

        Assert.False(result.IsCompatible);
        Assert.Contains("1.0.0", result.Reason!);
        Assert.Contains("3.0.0", result.Reason);
    }

    [Fact]
    public void Check_MoreThanTwoDistinctMinorVersions_WarnsAboutMultiHop()
    {
        var local = BrokerVersion.Parse("1.3.0");
        var cluster = new List<BrokerVersion>
        {
            BrokerVersion.Parse("1.1.0"),
            BrokerVersion.Parse("1.2.0"),
        };

        var result = _checker.Check(local, cluster);

        Assert.True(result.IsCompatible);
        Assert.Contains(result.Warnings, w =>
            w.Contains("More than 2 distinct minor versions", StringComparison.OrdinalIgnoreCase));
    }
}
