using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// Coverage for the version-range resolver in <see cref="PluginDependency.IsSatisfiedBy"/> —
/// the operator surface (`>=`, `&gt;`, `&lt;=`, `&lt;`, `^`, `~`, exact, `*`) is the most
/// error-prone part of the package dependency story; broken matchers can hide a missing
/// upgrade or accept an incompatible one.
/// </summary>
public sealed class PluginDependencyTests
{
    [Theory]
    [InlineData("1.0.0")]
    [InlineData("0.0.1")]
    [InlineData("999.999.999")]
    public void Wildcard_AcceptsAnyVersion(string candidate)
    {
        var dep = new PluginDependency { Id = "x", Version = "*" };
        Assert.True(dep.IsSatisfiedBy(candidate));
    }

    [Fact]
    public void EmptyVersion_AcceptsAnyVersion()
    {
        var dep = new PluginDependency { Id = "x", Version = "" };
        Assert.True(dep.IsSatisfiedBy("1.0.0"));
    }

    [Theory]
    [InlineData("1.0.0", "1.0.0", true)]
    [InlineData("1.0.0", "1.0.1", false)]
    [InlineData("1.0.0", "0.9.9", false)]
    [InlineData("2.5.3", "2.5.3", true)]
    public void ExactMatch(string constraint, string candidate, bool expected)
    {
        var dep = new PluginDependency { Id = "x", Version = constraint };
        Assert.Equal(expected, dep.IsSatisfiedBy(candidate));
    }

    [Theory]
    [InlineData(">=1.0.0", "1.0.0", true)]
    [InlineData(">=1.0.0", "1.5.0", true)]
    [InlineData(">=1.0.0", "0.9.9", false)]
    [InlineData(">=1.0.0", "2.0.0", true)]
    public void GreaterOrEqual(string constraint, string candidate, bool expected)
    {
        var dep = new PluginDependency { Id = "x", Version = constraint };
        Assert.Equal(expected, dep.IsSatisfiedBy(candidate));
    }

    [Theory]
    [InlineData(">1.0.0", "1.0.0", false)]
    [InlineData(">1.0.0", "1.0.1", true)]
    [InlineData(">1.0.0", "0.9.9", false)]
    public void GreaterThan(string constraint, string candidate, bool expected)
    {
        var dep = new PluginDependency { Id = "x", Version = constraint };
        Assert.Equal(expected, dep.IsSatisfiedBy(candidate));
    }

    [Theory]
    [InlineData("<=2.0.0", "2.0.0", true)]
    [InlineData("<=2.0.0", "1.0.0", true)]
    [InlineData("<=2.0.0", "2.0.1", false)]
    public void LessOrEqual(string constraint, string candidate, bool expected)
    {
        var dep = new PluginDependency { Id = "x", Version = constraint };
        Assert.Equal(expected, dep.IsSatisfiedBy(candidate));
    }

    [Theory]
    [InlineData("<2.0.0", "1.9.9", true)]
    [InlineData("<2.0.0", "2.0.0", false)]
    [InlineData("<2.0.0", "0.0.1", true)]
    public void LessThan(string constraint, string candidate, bool expected)
    {
        var dep = new PluginDependency { Id = "x", Version = constraint };
        Assert.Equal(expected, dep.IsSatisfiedBy(candidate));
    }

    [Theory]
    // Caret: same major, >= minor — npm-style "compatible-with"
    [InlineData("^1.2.0", "1.2.0", true)]
    [InlineData("^1.2.0", "1.5.0", true)]
    [InlineData("^1.2.0", "1.99.0", true)]
    [InlineData("^1.2.0", "2.0.0", false)] // major bump rejected
    [InlineData("^1.2.0", "1.1.0", false)] // below the floor
    public void Caret_SameMajorAndAtLeastMinor(string constraint, string candidate, bool expected)
    {
        var dep = new PluginDependency { Id = "x", Version = constraint };
        Assert.Equal(expected, dep.IsSatisfiedBy(candidate));
    }

    [Theory]
    // Tilde: same major.minor, >= patch
    [InlineData("~1.2.3", "1.2.3", true)]
    [InlineData("~1.2.3", "1.2.9", true)]
    [InlineData("~1.2.3", "1.3.0", false)] // minor bump rejected
    [InlineData("~1.2.3", "2.2.3", false)] // major bump rejected
    [InlineData("~1.2.3", "1.2.2", false)] // below the floor
    public void Tilde_SameMajorMinorAndAtLeastPatch(string constraint, string candidate, bool expected)
    {
        var dep = new PluginDependency { Id = "x", Version = constraint };
        Assert.Equal(expected, dep.IsSatisfiedBy(candidate));
    }

    [Theory]
    [InlineData("not-a-version")]
    [InlineData("1.2.3.4.5")]
    [InlineData("v1.0.0")]
    public void InvalidConstraintFormat_NotSatisfied(string constraint)
    {
        var dep = new PluginDependency { Id = "x", Version = constraint };
        Assert.False(dep.IsSatisfiedBy("1.0.0"));
    }

    [Fact]
    public void InvalidCandidate_NotSatisfied()
    {
        var dep = new PluginDependency { Id = "x", Version = ">=1.0.0" };
        Assert.False(dep.IsSatisfiedBy("not-a-version"));
    }

    [Fact]
    public void Optional_DefaultsToFalse()
    {
        var dep = new PluginDependency { Id = "x", Version = "1.0.0" };
        Assert.False(dep.Optional);
    }

    [Fact]
    public void Optional_RespectsExplicitValue()
    {
        var dep = new PluginDependency { Id = "x", Version = "1.0.0", Optional = true };
        Assert.True(dep.Optional);
    }
}
