using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// Locks down the on-wire shape of <c>plugin.json</c>. Marketplace, broker, packager and CLI
/// all consume this format from independent processes — a silent property-name drift would
/// break installs in production. Each <see cref="JsonPropertyName"/> on
/// <see cref="PluginManifest"/> needs explicit coverage so the contract doesn't shift by
/// accident.
/// </summary>
public sealed class PluginManifestSerializationTests
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    [Fact]
    public void Deserialize_MinimalManifest_PopulatesRequiredFields()
    {
        const string json = """
        {
          "id": "Kuestenlogik.Surgewave.Connector.Mqtt",
          "name": "MQTT Connector",
          "version": "1.2.3",
          "assemblies": ["Kuestenlogik.Surgewave.Connector.Mqtt.dll"]
        }
        """;

        var manifest = JsonSerializer.Deserialize<PluginManifest>(json, Options)!;

        Assert.Equal("Kuestenlogik.Surgewave.Connector.Mqtt", manifest.Id);
        Assert.Equal("MQTT Connector", manifest.Name);
        Assert.Equal("1.2.3", manifest.Version);
        Assert.Single(manifest.Assemblies, "Kuestenlogik.Surgewave.Connector.Mqtt.dll");
        Assert.Null(manifest.Description);
        Assert.Null(manifest.PluginSettings);
        Assert.Null(manifest.SurgewaveDependencies);
    }

    [Fact]
    public void Deserialize_FullManifest_PopulatesAllOptionalFields()
    {
        const string json = """
        {
          "id": "Kuestenlogik.Surgewave.Connector.Mqtt",
          "name": "MQTT Connector",
          "version": "1.2.3",
          "description": "Brokers MQTT 5 in and out of Surgewave topics.",
          "authors": ["Kuestenlogik", "Community"],
          "license": "Apache-2.0",
          "projectUrl": "https://surgewave.io",
          "tags": ["mqtt", "iot"],
          "icon": "icon.png",
          "minRuntimeVersion": "0.1.0",
          "dependencies": { "MQTTnet": "5.0.0" },
          "surgewaveDependencies": [
            { "id": "Kuestenlogik.Surgewave.Connect", "version": ">=0.1.0" }
          ],
          "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
          "assemblies": ["a.dll", "b.dll"],
          "pluginSettings": "mqtt-defaults.json",
          "$schema": "https://surgewave.io/schemas/plugin.json"
        }
        """;

        var manifest = JsonSerializer.Deserialize<PluginManifest>(json, Options)!;

        Assert.Equal("Brokers MQTT 5 in and out of Surgewave topics.", manifest.Description);
        Assert.NotNull(manifest.Authors);
        Assert.Equal(["Kuestenlogik", "Community"], manifest.Authors!);
        Assert.Equal("Apache-2.0", manifest.License);
        Assert.Equal("https://surgewave.io", manifest.ProjectUrl);
        Assert.NotNull(manifest.Tags);
        Assert.Equal(["mqtt", "iot"], manifest.Tags!);
        Assert.Equal("icon.png", manifest.Icon);
        Assert.Equal("0.1.0", manifest.MinRuntimeVersion);
        Assert.Equal("5.0.0", manifest.Dependencies!["MQTTnet"]);
        Assert.Single(manifest.SurgewaveDependencies!);
        Assert.Equal("Kuestenlogik.Surgewave.Connect", manifest.SurgewaveDependencies![0].Id);
        Assert.Equal(">=0.1.0", manifest.SurgewaveDependencies![0].Version);
        Assert.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", manifest.Sha256);
        Assert.Equal("mqtt-defaults.json", manifest.PluginSettings);
        Assert.Equal("https://surgewave.io/schemas/plugin.json", manifest.Schema);
    }

    [Fact]
    public void Roundtrip_PreservesAllFields()
    {
        var original = new PluginManifest
        {
            Id = "x",
            Name = "X",
            Version = "1.0.0",
            Assemblies = ["x.dll"],
            Description = "test",
            License = "MIT",
            Tags = ["a", "b"],
            PluginSettings = "x-defaults.json",
        };

        var json = JsonSerializer.Serialize(original, Options);
        var roundtripped = JsonSerializer.Deserialize<PluginManifest>(json, Options)!;

        Assert.Equal(original.Id, roundtripped.Id);
        Assert.Equal(original.Name, roundtripped.Name);
        Assert.Equal(original.Version, roundtripped.Version);
        Assert.Equal(original.Description, roundtripped.Description);
        Assert.Equal(original.License, roundtripped.License);
        Assert.Equal(original.Tags!, roundtripped.Tags!);
        Assert.Equal(original.PluginSettings, roundtripped.PluginSettings);
        Assert.Equal(original.Assemblies, roundtripped.Assemblies);
    }

    [Fact]
    public void PropertyNames_AreCanonical()
    {
        // Snake / kebab / pascal drift in JSON would break ecosystem consumers
        // (CLI, marketplace, broker, packager all deserialise this file
        // independently). Lock the wire-name surface.
        var manifest = new PluginManifest
        {
            Id = "id",
            Name = "name",
            Version = "1.0.0",
            Assemblies = ["x.dll"],
        };
        var json = JsonSerializer.Serialize(manifest);

        Assert.Contains("\"id\":", json);
        Assert.Contains("\"name\":", json);
        Assert.Contains("\"version\":", json);
        Assert.Contains("\"assemblies\":", json);
    }
}
