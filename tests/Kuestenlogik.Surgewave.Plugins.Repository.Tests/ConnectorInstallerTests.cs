using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

/// <summary>
/// Coverage fuer <see cref="ConnectorInstaller"/>. Wir testen die fs-only Pfade direkt
/// (Constructor erzeugt Install-Dir, LoadInstalledConnectors discovered connector.json
/// + plugin.json, Uninstall raeumt Files weg, IsInstalled/GetInstalledVersion-Lookups).
/// Die Install/LoadConnector-Pfade brauchen ein echtes ZIP + ALC und sind im
/// <c>Kuestenlogik.Surgewave.Plugins.Connectors.Tests</c>-Projekt (E2E).
/// </summary>
public sealed class ConnectorInstallerTests : IDisposable
{
    private readonly string _root;

    public ConnectorInstallerTests()
    {
        _root = Path.Combine(Path.GetTempPath(), $"sw-connector-installer-tests-{Guid.NewGuid():N}");
    }

    [Fact]
    public void Constructor_NonExistentDir_CreatesIt()
    {
        var dir = Path.Combine(_root, "fresh");
        Assert.False(Directory.Exists(dir));

        using var installer = new ConnectorInstaller(dir);

        Assert.True(Directory.Exists(dir));
        Assert.Empty(installer.InstalledConnectors);
    }

    [Fact]
    public void Constructor_EmptyDir_NoInstalledConnectors()
    {
        Directory.CreateDirectory(_root);

        using var installer = new ConnectorInstaller(_root);

        Assert.Empty(installer.InstalledConnectors);
    }

    [Fact]
    public void Constructor_DiscoversConnectorJsonMetadata()
    {
        var packageDir = Path.Combine(_root, "akka.plugin.1.0.0");
        Directory.CreateDirectory(packageDir);
        var connector = new InstalledConnector
        {
            PackageId = "akka.plugin",
            Version = "1.0.0",
            InstallDirectory = packageDir,
            InstalledAt = DateTimeOffset.UtcNow,
        };
        File.WriteAllText(Path.Combine(packageDir, "connector.json"), JsonSerializer.Serialize(connector));

        using var installer = new ConnectorInstaller(_root);

        Assert.True(installer.IsInstalled("akka.plugin"));
        Assert.Equal("1.0.0", installer.GetInstalledVersion("akka.plugin"));
    }

    [Fact]
    public void Constructor_FallbackToPluginJson_WhenNoConnectorJson()
    {
        var packageDir = Path.Combine(_root, "mqtt.plugin.2.1.0");
        Directory.CreateDirectory(packageDir);
        var manifest = new
        {
            id = "mqtt.plugin",
            version = "2.1.0",
            name = "MQTT",
            authors = new[] { "Kuestenlogik" },
            license = "Apache-2.0",
            description = "MQTT connector",
            tags = new[] { "iot", "mqtt" },
        };
        File.WriteAllText(Path.Combine(packageDir, "plugin.json"), JsonSerializer.Serialize(manifest));

        using var installer = new ConnectorInstaller(_root);

        var c = installer.InstalledConnectors["mqtt.plugin"];
        Assert.Equal("2.1.0", c.Version);
        Assert.Equal("MQTT", c.Name);
        Assert.Equal("Kuestenlogik", c.Author);
        Assert.Equal("Apache-2.0", c.License);
        Assert.Equal("MQTT connector", c.Description);
        Assert.Equal(["iot", "mqtt"], c.Tags);
    }

    [Fact]
    public void Constructor_PluginJsonWithoutNameOrAuthor_UsesFallbacks()
    {
        var packageDir = Path.Combine(_root, "minimal");
        Directory.CreateDirectory(packageDir);
        var manifest = new { id = "minimal.plugin", version = "1.0.0" };
        File.WriteAllText(Path.Combine(packageDir, "plugin.json"), JsonSerializer.Serialize(manifest));

        using var installer = new ConnectorInstaller(_root);

        var c = installer.InstalledConnectors["minimal.plugin"];
        Assert.Equal("minimal.plugin", c.Name);
        Assert.Equal(string.Empty, c.Author);
        Assert.Equal(string.Empty, c.License);
        Assert.Empty(c.Tags);
    }

    [Fact]
    public void Constructor_MalformedConnectorJson_FallsThroughOrSkips()
    {
        var packageDir = Path.Combine(_root, "broken");
        Directory.CreateDirectory(packageDir);
        File.WriteAllText(Path.Combine(packageDir, "connector.json"), "{ this is not json");

        using var installer = new ConnectorInstaller(_root);

        Assert.Empty(installer.InstalledConnectors);
    }

    [Fact]
    public void Constructor_MalformedPluginJson_Skipped()
    {
        var packageDir = Path.Combine(_root, "bad-manifest");
        Directory.CreateDirectory(packageDir);
        File.WriteAllText(Path.Combine(packageDir, "plugin.json"), "{ broken");

        using var installer = new ConnectorInstaller(_root);

        Assert.Empty(installer.InstalledConnectors);
    }

    [Fact]
    public void Constructor_PluginJsonMissingIdOrVersion_Skipped()
    {
        var packageDir = Path.Combine(_root, "no-id");
        Directory.CreateDirectory(packageDir);
        File.WriteAllText(Path.Combine(packageDir, "plugin.json"), """{"name":"x"}""");

        using var installer = new ConnectorInstaller(_root);

        Assert.Empty(installer.InstalledConnectors);
    }

    [Fact]
    public void IsInstalled_UnknownPackage_False()
    {
        Directory.CreateDirectory(_root);
        using var installer = new ConnectorInstaller(_root);

        Assert.False(installer.IsInstalled("nothing.here"));
        Assert.Null(installer.GetInstalledVersion("nothing.here"));
    }

    [Fact]
    public void Uninstall_KnownPackage_RemovesFromDiskAndState()
    {
        var packageDir = Path.Combine(_root, "uninst.plugin.1.0.0");
        Directory.CreateDirectory(packageDir);
        var connector = new InstalledConnector
        {
            PackageId = "uninst.plugin",
            Version = "1.0.0",
            InstallDirectory = packageDir,
        };
        File.WriteAllText(Path.Combine(packageDir, "connector.json"), JsonSerializer.Serialize(connector));

        using var installer = new ConnectorInstaller(_root);
        Assert.True(installer.IsInstalled("uninst.plugin"));

        installer.Uninstall("uninst.plugin");

        Assert.False(installer.IsInstalled("uninst.plugin"));
        Assert.False(Directory.Exists(packageDir));
    }

    [Fact]
    public void Uninstall_UnknownPackage_NoOp()
    {
        Directory.CreateDirectory(_root);
        using var installer = new ConnectorInstaller(_root);

        // should silently no-op, not throw
        installer.Uninstall("never.installed");

        Assert.Empty(installer.InstalledConnectors);
    }

    [Fact]
    public void LoadConnector_NotInstalled_Throws()
    {
        Directory.CreateDirectory(_root);
        using var installer = new ConnectorInstaller(_root);

        Assert.Throws<InvalidOperationException>(() => installer.LoadConnector("nothing"));
    }

    [Fact]
    public void UnloadConnector_NotLoaded_NoOp()
    {
        Directory.CreateDirectory(_root);
        using var installer = new ConnectorInstaller(_root);

        // No exception, no state change
        installer.UnloadConnector("nothing");
    }

    [Fact]
    public void LoadConnector_NoLibDir_ReturnsEmptyAssemblyList()
    {
        var packageDir = Path.Combine(_root, "lib-less.plugin.1.0.0");
        Directory.CreateDirectory(packageDir);
        File.WriteAllText(
            Path.Combine(packageDir, "connector.json"),
            JsonSerializer.Serialize(new InstalledConnector
            {
                PackageId = "lib-less.plugin",
                Version = "1.0.0",
                InstallDirectory = packageDir,
            }));

        using var installer = new ConnectorInstaller(_root);
        var assemblies = installer.LoadConnector("lib-less.plugin");

        Assert.Empty(assemblies);
    }

    [Fact]
    public void LoadConnector_TwiceForSamePackage_ReturnsCachedAssemblies()
    {
        var packageDir = Path.Combine(_root, "cached.plugin.1.0.0");
        Directory.CreateDirectory(packageDir);
        File.WriteAllText(
            Path.Combine(packageDir, "connector.json"),
            JsonSerializer.Serialize(new InstalledConnector
            {
                PackageId = "cached.plugin",
                Version = "1.0.0",
                InstallDirectory = packageDir,
            }));

        using var installer = new ConnectorInstaller(_root);
        var first = installer.LoadConnector("cached.plugin");
        var second = installer.LoadConnector("cached.plugin");

        Assert.Equal(first.Count, second.Count);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            try { Directory.Delete(_root, recursive: true); }
            catch { /* best effort */ }
        }
    }
}
