using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

public sealed class InstalledConnectorTests
{
    private static InstalledConnector Minimal() => new()
    {
        PackageId = "x",
        Version = "1.0.0",
        InstallDirectory = "/plugins/x",
    };

    [Fact]
    public void Defaults_ScalarFieldsEmpty_TagsEmpty_InstalledAtZero()
    {
        var c = Minimal();

        Assert.Equal(string.Empty, c.Name);
        Assert.Equal(string.Empty, c.Author);
        Assert.Equal(string.Empty, c.License);
        Assert.Equal(string.Empty, c.Description);
        Assert.Empty(c.Tags);
        Assert.Equal(default, c.InstalledAt);
    }

    [Fact]
    public void RecordEquality_SameValues_AreEqual()
    {
        var ts = DateTimeOffset.UtcNow;
        var a = Minimal() with { InstalledAt = ts };
        var b = Minimal() with { InstalledAt = ts };

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void RecordEquality_DifferentVersion_NotEqual()
    {
        var a = Minimal();
        var b = a with { Version = "2.0.0" };

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void With_PreservesOtherFields()
    {
        var source = Minimal() with
        {
            Name = "Akka",
            Author = "kl",
            License = "Apache-2.0",
            Description = "akka connector",
            Tags = ["akka", "streaming"],
            InstalledAt = DateTimeOffset.UtcNow,
        };

        var renamed = source with { Name = "Akka.NET" };

        Assert.Equal("Akka.NET", renamed.Name);
        Assert.Equal(source.Author, renamed.Author);
        Assert.Equal(source.License, renamed.License);
        Assert.Equal(source.Tags, renamed.Tags);
    }
}
