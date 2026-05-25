using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

/// <summary>
/// Full property-coverage Tests fuer <see cref="ConnectorPackageInfo"/>. Die Klasse hat
/// viele init-only-Properties, die in den existierenden Smoke-Tests nicht geschrieben
/// wurden. Hier wird einmal ein vollstaendig populiertes Objekt gebaut und alle Felder
/// werden ausgelesen.
/// </summary>
public sealed class ConnectorPackageInfoFullPropertiesTests
{
    [Fact]
    public void FullPopulation_AllPropertiesReadback()
    {
        var info = new ConnectorPackageInfo
        {
            PackageId = "Kuestenlogik.Surgewave.Connector.Hue",
            Version = "1.0.0",
            Name = "Hue Connector",
            Description = "Philips Hue source",
            Author = "Kuestenlogik",
            IconUrl = "https://cdn.example.com/hue.png",
            ProjectUrl = "https://github.com/Kuestenlogik/Surgewave.Connectors",
            License = "Apache-2.0",
            Tags = ["iot", "hue"],
            AvailableVersions = ["1.0.0", "0.9.0"],
            IsInstalled = true,
            InstalledVersion = "0.9.0",
            DownloadCount = 12345,
            Published = DateTimeOffset.Parse("2026-05-01T12:00:00Z"),
            ConnectorTypes = ["source", "sink"],
            Sha256 = new string('a', 64),
            Dependencies =
            [
                new ConnectorDependencyInfo
                {
                    PackageId = "Kuestenlogik.Surgewave.Connector.Base",
                    VersionConstraint = ">=1.0.0",
                    Optional = false,
                },
            ],
            IsSigned = true,
            SignerIdentity = "kuestenlogik",
            SignerProvider = "builtin-ecdsa",
        };

        Assert.Equal("Kuestenlogik.Surgewave.Connector.Hue", info.PackageId);
        Assert.Equal("1.0.0", info.Version);
        Assert.Equal("Hue Connector", info.Name);
        Assert.Equal("Philips Hue source", info.Description);
        Assert.Equal("Kuestenlogik", info.Author);
        Assert.Equal("https://cdn.example.com/hue.png", info.IconUrl);
        Assert.Equal("https://github.com/Kuestenlogik/Surgewave.Connectors", info.ProjectUrl);
        Assert.Equal("Apache-2.0", info.License);
        Assert.Equal(["iot", "hue"], info.Tags);
        Assert.Equal(["1.0.0", "0.9.0"], info.AvailableVersions);
        Assert.True(info.IsInstalled);
        Assert.Equal("0.9.0", info.InstalledVersion);
        Assert.Equal(12345L, info.DownloadCount);
        Assert.Equal(DateTimeOffset.Parse("2026-05-01T12:00:00Z"), info.Published);
        Assert.Equal(["source", "sink"], info.ConnectorTypes);
        Assert.Equal(new string('a', 64), info.Sha256);
        Assert.True(info.HasDependencies);
        Assert.Single(info.Dependencies);
        Assert.True(info.IsSigned);
        Assert.Equal("kuestenlogik", info.SignerIdentity);
        Assert.Equal("builtin-ecdsa", info.SignerProvider);
    }

    [Fact]
    public void Published_DefaultsToNull()
    {
        var info = new ConnectorPackageInfo
        {
            PackageId = "x",
            Version = "1.0.0",
            Name = "X",
        };

        Assert.Null(info.Published);
    }

    [Fact]
    public void ConnectorDependencyInfo_OptionalTrueRoundtrip()
    {
        var dep = new ConnectorDependencyInfo
        {
            PackageId = "optional.pkg",
            VersionConstraint = "^2.0.0",
            Optional = true,
        };

        Assert.Equal("optional.pkg", dep.PackageId);
        Assert.Equal("^2.0.0", dep.VersionConstraint);
        Assert.True(dep.Optional);
    }
}
