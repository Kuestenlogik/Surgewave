using Kuestenlogik.Surgewave.Clustering.Upgrades;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Upgrades;

public class BrokerVersionTests
{
    [Fact]
    public void Parse_ValidVersion_ReturnsCorrectComponents()
    {
        var version = BrokerVersion.Parse("1.2.3");

        Assert.Equal(1, version.Major);
        Assert.Equal(2, version.Minor);
        Assert.Equal(3, version.Patch);
        Assert.Null(version.PreRelease);
    }

    [Fact]
    public void Parse_VersionWithPreRelease_ReturnsCorrectComponents()
    {
        var version = BrokerVersion.Parse("1.0.0-beta.1");

        Assert.Equal(1, version.Major);
        Assert.Equal(0, version.Minor);
        Assert.Equal(0, version.Patch);
        Assert.Equal("beta.1", version.PreRelease);
    }

    [Fact]
    public void Parse_VersionWithLeadingV_StripsPrefix()
    {
        var version = BrokerVersion.Parse("v2.3.4");

        Assert.Equal(2, version.Major);
        Assert.Equal(3, version.Minor);
        Assert.Equal(4, version.Patch);
    }

    [Fact]
    public void Parse_TwoPartVersion_DefaultsPatchToZero()
    {
        var version = BrokerVersion.Parse("1.5");

        Assert.Equal(1, version.Major);
        Assert.Equal(5, version.Minor);
        Assert.Equal(0, version.Patch);
    }

    [Theory]
    [InlineData("")]
    [InlineData("abc")]
    [InlineData("1")]
    [InlineData("1.2.3.4")]
    [InlineData("-1.0.0")]
    public void Parse_InvalidVersion_ThrowsFormatException(string input)
    {
        Assert.Throws<FormatException>(() => BrokerVersion.Parse(input));
    }

    [Fact]
    public void Parse_NullVersion_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => BrokerVersion.Parse(null!));
    }

    [Fact]
    public void TryParse_ValidVersion_ReturnsTrueAndResult()
    {
        var success = BrokerVersion.TryParse("1.2.3", out var result);

        Assert.True(success);
        Assert.NotNull(result);
        Assert.Equal(1, result.Major);
    }

    [Fact]
    public void TryParse_InvalidVersion_ReturnsFalse()
    {
        var success = BrokerVersion.TryParse("invalid", out var result);

        Assert.False(success);
        Assert.Null(result);
    }

    [Fact]
    public void TryParse_NullVersion_ReturnsFalse()
    {
        var success = BrokerVersion.TryParse(null, out var result);

        Assert.False(success);
        Assert.Null(result);
    }

    [Fact]
    public void IsCompatibleWith_SameMajorVersion_ReturnsTrue()
    {
        var v1 = BrokerVersion.Parse("1.0.0");
        var v2 = BrokerVersion.Parse("1.5.3");

        Assert.True(v1.IsCompatibleWith(v2));
        Assert.True(v2.IsCompatibleWith(v1));
    }

    [Fact]
    public void IsCompatibleWith_DifferentMajorVersion_ReturnsFalse()
    {
        var v1 = BrokerVersion.Parse("1.0.0");
        var v2 = BrokerVersion.Parse("2.0.0");

        Assert.False(v1.IsCompatibleWith(v2));
        Assert.False(v2.IsCompatibleWith(v1));
    }

    [Fact]
    public void IsCompatibleWith_SameVersion_ReturnsTrue()
    {
        var v1 = BrokerVersion.Parse("1.2.3");
        var v2 = BrokerVersion.Parse("1.2.3");

        Assert.True(v1.IsCompatibleWith(v2));
    }

    [Fact]
    public void IsCompatibleWith_MajorZero_Compatible()
    {
        var v1 = BrokerVersion.Parse("0.1.0");
        var v2 = BrokerVersion.Parse("0.9.5");

        Assert.True(v1.IsCompatibleWith(v2));
    }

    [Fact]
    public void IsNewerThan_HigherMajor_ReturnsTrue()
    {
        var v1 = BrokerVersion.Parse("2.0.0");
        var v2 = BrokerVersion.Parse("1.9.9");

        Assert.True(v1.IsNewerThan(v2));
        Assert.False(v2.IsNewerThan(v1));
    }

    [Fact]
    public void IsNewerThan_HigherMinor_ReturnsTrue()
    {
        var v1 = BrokerVersion.Parse("1.3.0");
        var v2 = BrokerVersion.Parse("1.2.9");

        Assert.True(v1.IsNewerThan(v2));
    }

    [Fact]
    public void IsNewerThan_HigherPatch_ReturnsTrue()
    {
        var v1 = BrokerVersion.Parse("1.2.4");
        var v2 = BrokerVersion.Parse("1.2.3");

        Assert.True(v1.IsNewerThan(v2));
    }

    [Fact]
    public void IsNewerThan_ReleaseNewerThanPreRelease()
    {
        var release = BrokerVersion.Parse("1.0.0");
        var preRelease = BrokerVersion.Parse("1.0.0-beta");

        Assert.True(release.IsNewerThan(preRelease));
        Assert.False(preRelease.IsNewerThan(release));
    }

    [Fact]
    public void IsNewerThan_SameVersion_ReturnsFalse()
    {
        var v1 = BrokerVersion.Parse("1.2.3");
        var v2 = BrokerVersion.Parse("1.2.3");

        Assert.False(v1.IsNewerThan(v2));
    }

    [Fact]
    public void ToString_WithoutPreRelease_ReturnsSemanticVersion()
    {
        var version = BrokerVersion.Parse("1.2.3");

        Assert.Equal("1.2.3", version.ToString());
    }

    [Fact]
    public void ToString_WithPreRelease_IncludesPreRelease()
    {
        var version = BrokerVersion.Parse("1.0.0-rc.1");

        Assert.Equal("1.0.0-rc.1", version.ToString());
    }

    [Fact]
    public void Current_ReturnsNonNullVersion()
    {
        var current = BrokerVersion.Current;

        Assert.NotNull(current);
        Assert.True(current.Major >= 0);
        Assert.True(current.Minor >= 0);
        Assert.True(current.Patch >= 0);
    }

    [Fact]
    public void Equality_SameVersions_AreEqual()
    {
        var v1 = BrokerVersion.Parse("1.2.3");
        var v2 = BrokerVersion.Parse("1.2.3");

        Assert.Equal(v1, v2);
        Assert.True(v1 == v2);
    }

    [Fact]
    public void Equality_DifferentVersions_AreNotEqual()
    {
        var v1 = BrokerVersion.Parse("1.2.3");
        var v2 = BrokerVersion.Parse("1.2.4");

        Assert.NotEqual(v1, v2);
        Assert.True(v1 != v2);
    }

    [Fact]
    public void Equality_PreReleaseCaseInsensitive()
    {
        var v1 = BrokerVersion.Parse("1.0.0-BETA");
        var v2 = BrokerVersion.Parse("1.0.0-beta");

        Assert.Equal(v1, v2);
    }
}
