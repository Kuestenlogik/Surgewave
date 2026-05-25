using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Distributed;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Connect.Tests.Plugins;

public class ConnectorPluginSystemTests
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    [Fact]
    public void ManifestDeserialization_CoreFields()
    {
        // Arrange
        var json = """
        {
            "id": "kl.test.connector",
            "name": "Test Connector Plugin",
            "version": "1.0.0",
            "assemblies": ["Test.dll"]
        }
        """;

        // Act
        var manifest = JsonSerializer.Deserialize<PluginManifest>(json, JsonOptions);

        // Assert
        Assert.NotNull(manifest);
        Assert.Equal("kl.test.connector", manifest.Id);
        Assert.Equal("Test Connector Plugin", manifest.Name);
        Assert.Equal("1.0.0", manifest.Version);
        Assert.Single(manifest.Assemblies);
        Assert.Equal("Test.dll", manifest.Assemblies[0]);
    }

    [Fact]
    public void ManifestDeserialization_IgnoresLegacyConnectorsArray()
    {
        // Arrange -- legacy manifest that still has a connectors array should deserialize without error
        var json = """
        {
            "id": "kl.legacy.connector",
            "name": "Legacy Plugin",
            "version": "0.9.0",
            "assemblies": ["Legacy.dll"],
            "connectors": [
                {
                    "class": "Kuestenlogik.Legacy.LegacySource",
                    "type": "source"
                }
            ]
        }
        """;

        // Act
        var manifest = JsonSerializer.Deserialize<PluginManifest>(json, JsonOptions);

        // Assert -- manifest deserializes successfully, connectors array is ignored
        Assert.NotNull(manifest);
        Assert.Equal("kl.legacy.connector", manifest.Id);
        Assert.Equal("Legacy Plugin", manifest.Name);
        Assert.Equal("0.9.0", manifest.Version);
    }

    [Fact]
    public void HeartbeatWithCapabilities_SerializesCorrectly()
    {
        // Arrange
        var heartbeat = new WorkerHeartbeat
        {
            WorkerId = "worker-1",
            RestUrl = "http://localhost:8083",
            Timestamp = 1710500000000,
            Generation = 3,
            AssignedConnectors = ["my-source", "my-sink"],
            AvailableTypes =
            [
                new ConnectorCapability("Kuestenlogik.Test.MySource", "source", "My Source", "1.0.0"),
                new ConnectorCapability("Kuestenlogik.Test.MySink", "sink", "My Sink", "1.0.0")
            ]
        };

        // Act
        var json = JsonSerializer.Serialize(heartbeat);
        var deserialized = JsonSerializer.Deserialize<WorkerHeartbeat>(json, JsonOptions);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal("worker-1", deserialized.WorkerId);
        Assert.Equal(2, deserialized.AvailableTypes.Count);
        Assert.Equal("Kuestenlogik.Test.MySource", deserialized.AvailableTypes[0].ClassName);
        Assert.Equal("source", deserialized.AvailableTypes[0].Type);
        Assert.Equal("My Source", deserialized.AvailableTypes[0].DisplayName);
        Assert.Equal("1.0.0", deserialized.AvailableTypes[0].Version);
        Assert.Equal("Kuestenlogik.Test.MySink", deserialized.AvailableTypes[1].ClassName);
        Assert.Equal("sink", deserialized.AvailableTypes[1].Type);
    }

    [Fact]
    public void HeartbeatWithCapabilities_BackwardCompatible_WithoutAvailableTypes()
    {
        // Arrange -- legacy heartbeat without AvailableTypes
        var json = """
        {
            "WorkerId": "worker-old",
            "RestUrl": "http://localhost:8083",
            "Timestamp": 1710500000000,
            "Generation": 1,
            "AssignedConnectors": ["legacy-conn"]
        }
        """;

        // Act
        var deserialized = JsonSerializer.Deserialize<WorkerHeartbeat>(json, JsonOptions);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal("worker-old", deserialized.WorkerId);
        Assert.Empty(deserialized.AvailableTypes);
        Assert.Single(deserialized.AssignedConnectors);
    }

    [Fact]
    public void AggregatedRegistry_CombinesLocalAndRemote()
    {
        // Arrange
        var registry = new AggregatedConnectorRegistry();

        var localPlugins = new List<PluginInfo>
        {
            new()
            {
                Class = "Kuestenlogik.Local.FileSource",
                Type = "source",
                Version = "1.0.0",
                DisplayName = "File Source",
                Icon = "FileOpen",
                Category = "IO",
                Description = "Reads from files"
            }
        };

        var remoteCapabilities = new List<ConnectorCapability>
        {
            new("Kuestenlogik.Remote.DisSource", "source", "DIS Source", "2.0.0")
        };

        // Act
        registry.UpdateFromLocalPlugins(localPlugins);
        registry.UpdateFromHeartbeat("worker-remote-1", remoteCapabilities);

        var allTypes = registry.GetAllTypes();

        // Assert
        Assert.Equal(2, allTypes.Count);

        var local = allTypes.First(t => t.ClassName == "Kuestenlogik.Local.FileSource");
        Assert.True(local.IsLocal);
        Assert.Equal("File Source", local.DisplayName);
        Assert.Equal("FileOpen", local.Icon);
        Assert.Equal("IO", local.Category);
        Assert.Equal("Reads from files", local.Description);

        var remote = allTypes.First(t => t.ClassName == "Kuestenlogik.Remote.DisSource");
        Assert.False(remote.IsLocal);
        Assert.Equal("DIS Source", remote.DisplayName);
        Assert.Equal("2.0.0", remote.Version);
        Assert.Contains("worker-remote-1", remote.AvailableOnWorkers);
    }

    [Fact]
    public void AggregatedRegistry_DeduplicatesByClassName()
    {
        // Arrange
        var registry = new AggregatedConnectorRegistry();

        // Same class name available both locally and remotely
        var localPlugins = new List<PluginInfo>
        {
            new()
            {
                Class = "Kuestenlogik.Common.HttpSource",
                Type = "source",
                Version = "1.0.0",
                DisplayName = "HTTP Source (Local)",
                Icon = "Http",
                Category = "Web"
            }
        };

        var remoteCapabilities = new List<ConnectorCapability>
        {
            new("Kuestenlogik.Common.HttpSource", "source", "HTTP Source (Remote)", "1.0.0")
        };

        // Act
        registry.UpdateFromLocalPlugins(localPlugins);
        registry.UpdateFromHeartbeat("worker-2", remoteCapabilities);

        var allTypes = registry.GetAllTypes();

        // Assert -- should be deduplicated, local takes priority
        Assert.Single(allTypes);
        var entry = allTypes[0];
        Assert.Equal("Kuestenlogik.Common.HttpSource", entry.ClassName);
        Assert.Equal("HTTP Source (Local)", entry.DisplayName); // local metadata wins
        Assert.True(entry.IsLocal);
        Assert.Contains("worker-2", entry.AvailableOnWorkers);
    }

    [Fact]
    public void AggregatedRegistry_TracksWorkerAvailability()
    {
        // Arrange
        var registry = new AggregatedConnectorRegistry();

        var capsWorker1 = new List<ConnectorCapability>
        {
            new("Kuestenlogik.Shared.PostgresqlSink", "sink", "PostgreSQL Sink", "1.0.0"),
            new("Kuestenlogik.Shared.S3Sink", "sink", "S3 Sink", "1.0.0")
        };

        var capsWorker2 = new List<ConnectorCapability>
        {
            new("Kuestenlogik.Shared.PostgresqlSink", "sink", "PostgreSQL Sink", "1.0.0"),
            new("Kuestenlogik.Shared.RedisSink", "sink", "Redis Sink", "1.0.0")
        };

        // Act
        registry.UpdateFromHeartbeat("worker-A", capsWorker1);
        registry.UpdateFromHeartbeat("worker-B", capsWorker2);

        // Assert -- PostgresqlSink should be available on both workers
        var pgWorkers = registry.GetWorkersForType("Kuestenlogik.Shared.PostgresqlSink");
        Assert.Equal(2, pgWorkers.Count);
        Assert.Contains("worker-A", pgWorkers);
        Assert.Contains("worker-B", pgWorkers);

        // S3 only on worker-A
        var s3Workers = registry.GetWorkersForType("Kuestenlogik.Shared.S3Sink");
        Assert.Single(s3Workers);
        Assert.Contains("worker-A", s3Workers);

        // Redis only on worker-B
        var redisWorkers = registry.GetWorkersForType("Kuestenlogik.Shared.RedisSink");
        Assert.Single(redisWorkers);
        Assert.Contains("worker-B", redisWorkers);

        // Unknown type
        var unknownWorkers = registry.GetWorkersForType("Kuestenlogik.Unknown.Type");
        Assert.Empty(unknownWorkers);
    }

    [Fact]
    public void AggregatedRegistry_RemoveWorker_CleansUpCapabilities()
    {
        // Arrange
        var registry = new AggregatedConnectorRegistry();

        registry.UpdateFromHeartbeat("worker-1", [
            new ConnectorCapability("Kuestenlogik.Test.Source", "source", "Test Source", "1.0.0")
        ]);
        registry.UpdateFromHeartbeat("worker-2", [
            new ConnectorCapability("Kuestenlogik.Test.Source", "source", "Test Source", "1.0.0")
        ]);

        // Verify both workers are tracked
        Assert.Equal(2, registry.GetWorkersForType("Kuestenlogik.Test.Source").Count);

        // Act -- remove worker-1
        registry.RemoveWorker("worker-1");

        // Assert
        var workers = registry.GetWorkersForType("Kuestenlogik.Test.Source");
        Assert.Single(workers);
        Assert.Contains("worker-2", workers);
    }

    [Fact]
    public void ConnectorPlugin_ExtendedFieldsAreOptional()
    {
        // Arrange & Act -- create a PluginInfo without extended fields
        var plugin = new PluginInfo
        {
            Class = "Kuestenlogik.Test.BasicConnector",
            Type = "source",
            Version = "1.0.0"
        };

        // Assert
        Assert.Null(plugin.DisplayName);
        Assert.Null(plugin.Icon);
        Assert.Null(plugin.Category);
        Assert.Null(plugin.Description);
    }

    [Fact]
    public void ConnectorPlugin_ExtendedFieldsPopulated()
    {
        // Arrange & Act
        var plugin = new PluginInfo
        {
            Class = "Kuestenlogik.Test.RichConnector",
            Type = "sink",
            Version = "2.0.0",
            DisplayName = "Rich Sink",
            Icon = "Database",
            Category = "Storage",
            Description = "Writes to a rich storage system"
        };

        // Assert
        Assert.Equal("Rich Sink", plugin.DisplayName);
        Assert.Equal("Database", plugin.Icon);
        Assert.Equal("Storage", plugin.Category);
        Assert.Equal("Writes to a rich storage system", plugin.Description);
    }
}
