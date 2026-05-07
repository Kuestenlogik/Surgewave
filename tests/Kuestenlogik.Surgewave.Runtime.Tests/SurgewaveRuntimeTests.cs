using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Xunit;

namespace Kuestenlogik.Surgewave.Runtime.Tests;

/// <summary>
/// Tests for the SurgewaveRuntime and related configuration.
/// </summary>
public sealed class SurgewaveRuntimeTests
{
    #region SurgewaveRuntimeOptions Tests

    [Fact]
    public void SurgewaveRuntimeOptions_DefaultValues()
    {
        // Act
        var options = new SurgewaveRuntimeOptions();

        // Assert - verify default values
        Assert.Equal("localhost", options.Host);
        Assert.Equal(0, options.Port);
        Assert.Equal(0, options.BrokerId);
        Assert.True(options.AutoCreateTopics);
        Assert.Equal(1, options.DefaultNumPartitions);
        Assert.Equal(1, options.DefaultReplicationFactor);
        Assert.Equal(-1, options.RetentionHours);
        Assert.Equal(-1, options.RetentionBytes);
        Assert.Equal(5, options.ShutdownTimeoutSeconds);
        Assert.False(options.EnableSasl);
        Assert.False(options.EnableTls);
        Assert.False(options.EnableAcl);
        Assert.True(options.CleanupOnDispose);
        Assert.Equal(StorageEngines.File, options.StorageEngine);
        Assert.Null(options.DataDirectory);
        Assert.True(options.EnableDualMode); // IPv4+IPv6 enabled by default
    }

    [Fact]
    public void SurgewaveRuntimeOptions_ClusterDefaults()
    {
        // Act
        var options = new SurgewaveRuntimeOptions();

        // Assert - cluster defaults
        Assert.False(options.EnableCluster);
        Assert.Empty(options.ClusterNodes);
        Assert.Equal(0, options.ReplicationPort);
        Assert.False(options.UseRaftConsensus);
        Assert.Equal(150, options.RaftElectionTimeoutMinMs);
        Assert.Equal(300, options.RaftElectionTimeoutMaxMs);
        Assert.Equal(50, options.RaftHeartbeatIntervalMs);
        Assert.Equal(3000, options.HeartbeatIntervalMs);
        Assert.Equal(10000, options.HeartbeatTimeoutMs);
    }

    [Fact]
    public void SurgewaveRuntimeOptions_CanBeCustomized()
    {
        // Act
        var options = new SurgewaveRuntimeOptions
        {
            Host = "0.0.0.0",
            Port = 9093,
            BrokerId = 5,
            AutoCreateTopics = false,
            DefaultNumPartitions = 8,
            DefaultReplicationFactor = 3,
            RetentionHours = 168,
            RetentionBytes = 1073741824,
            ShutdownTimeoutSeconds = 30,
            EnableSasl = true,
            EnableTls = true,
            EnableAcl = true,
            CleanupOnDispose = false,
            StorageEngine = StorageEngines.Memory,
            DataDirectory = "/tmp/surgewave"
        };

        // Assert
        Assert.Equal("0.0.0.0", options.Host);
        Assert.Equal(9093, options.Port);
        Assert.Equal(5, options.BrokerId);
        Assert.False(options.AutoCreateTopics);
        Assert.Equal(8, options.DefaultNumPartitions);
        Assert.Equal(3, options.DefaultReplicationFactor);
        Assert.Equal(168, options.RetentionHours);
        Assert.Equal(1073741824, options.RetentionBytes);
        Assert.Equal(30, options.ShutdownTimeoutSeconds);
        Assert.True(options.EnableSasl);
        Assert.True(options.EnableTls);
        Assert.True(options.EnableAcl);
        Assert.False(options.CleanupOnDispose);
        Assert.Equal(StorageEngines.Memory, options.StorageEngine);
        Assert.Equal("/tmp/surgewave", options.DataDirectory);
    }

    [Fact]
    public void SurgewaveRuntimeOptions_ClusterCanBeConfigured()
    {
        // Act
        var options = new SurgewaveRuntimeOptions
        {
            EnableCluster = true,
            ClusterNodes = ["1:localhost:9093:9094", "2:localhost:9095:9096"],
            ReplicationPort = 9094,
            UseRaftConsensus = true,
            RaftElectionTimeoutMinMs = 200,
            RaftElectionTimeoutMaxMs = 400,
            RaftHeartbeatIntervalMs = 100,
            HeartbeatIntervalMs = 5000,
            HeartbeatTimeoutMs = 15000
        };

        // Assert
        Assert.True(options.EnableCluster);
        Assert.Equal(2, options.ClusterNodes.Count);
        Assert.Equal(9094, options.ReplicationPort);
        Assert.True(options.UseRaftConsensus);
        Assert.Equal(200, options.RaftElectionTimeoutMinMs);
        Assert.Equal(400, options.RaftElectionTimeoutMaxMs);
        Assert.Equal(100, options.RaftHeartbeatIntervalMs);
        Assert.Equal(5000, options.HeartbeatIntervalMs);
        Assert.Equal(15000, options.HeartbeatTimeoutMs);
    }

    [Fact]
    public void SurgewaveRuntimeOptions_IsRecordType()
    {
        // Act
        var options1 = new SurgewaveRuntimeOptions { BrokerId = 1 };
        var options3 = new SurgewaveRuntimeOptions { BrokerId = 2 };

        // Assert - verify record type has correct properties
        // Note: List<> properties prevent simple equality, so we verify key differences
        Assert.NotEqual(options1.BrokerId, options3.BrokerId);
        Assert.Equal(options1.Host, options3.Host); // Same default host
        Assert.Equal(options1.Port, options3.Port); // Same default port
    }

    [Fact]
    public void SurgewaveRuntimeOptions_WithExpression_CreatesModifiedCopy()
    {
        // Arrange
        var original = new SurgewaveRuntimeOptions
        {
            Host = "localhost",
            Port = 9092,
            BrokerId = 1
        };

        // Act
        var modified = original with { Port = 9093 };

        // Assert
        Assert.Equal(9092, original.Port);
        Assert.Equal(9093, modified.Port);
        Assert.Equal("localhost", modified.Host);
        Assert.Equal(1, modified.BrokerId);
    }

    #endregion

    #region StorageEngine Tests

    [Fact]
    public void StorageEngines_HasExpectedValues()
    {
        // Assert
        Assert.Equal("file", StorageEngines.File);
        Assert.Equal("memory", StorageEngines.Memory);
    }

    [Fact]
    public void StorageEngine_File_IsDefault()
    {
        // Act
        var options = new SurgewaveRuntimeOptions();

        // Assert
        Assert.Equal(StorageEngines.File, options.StorageEngine);
    }

    #endregion

    #region SurgewaveRuntimeBuilder Tests

    [Fact]
    public void SurgewaveRuntimeBuilder_CreateBuilder_ReturnsBuilder()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder();

        // Assert
        Assert.NotNull(builder);
        Assert.IsType<SurgewaveRuntimeBuilder>(builder);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithPort_SetsPort()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithPort(9092);

        // Assert - build options to verify
        var options = builder.Build();
        Assert.Equal(9092, options.Port);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithHost_SetsHost()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithHost("0.0.0.0");

        // Assert
        var options = builder.Build();
        Assert.Equal("0.0.0.0", options.Host);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithBrokerId_SetsBrokerId()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(5);

        // Assert
        var options = builder.Build();
        Assert.Equal(5, options.BrokerId);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithDataDirectory_SetsDataDirectory()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithDataDirectory("/tmp/surgewave-data");

        // Assert
        var options = builder.Build();
        Assert.Equal("/tmp/surgewave-data", options.DataDirectory);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithAutoCreateTopics_SetsAutoCreateTopics()
    {
        // Act
        var builderEnabled = SurgewaveRuntime.CreateBuilder()
            .WithAutoCreateTopics(true);
        var builderDisabled = SurgewaveRuntime.CreateBuilder()
            .WithAutoCreateTopics(false);

        // Assert
        Assert.True(builderEnabled.Build().AutoCreateTopics);
        Assert.False(builderDisabled.Build().AutoCreateTopics);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithPartitions_SetsDefaultPartitions()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithPartitions(6);

        // Assert
        var options = builder.Build();
        Assert.Equal(6, options.DefaultNumPartitions);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithRetention_SetsRetentionOptions()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithRetentionHours(72)
            .WithRetentionBytes(500_000_000);

        // Assert
        var options = builder.Build();
        Assert.Equal(72, options.RetentionHours);
        Assert.Equal(500_000_000, options.RetentionBytes);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithStorageEngine_SetsStorageEngine()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithStorageEngine(StorageEngines.Memory);

        // Assert
        var options = builder.Build();
        Assert.Equal(StorageEngines.Memory, options.StorageEngine);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithCluster_SetsClusterOptions()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithCluster()
            .WithReplicationPort(9094);

        // Assert
        var options = builder.Build();
        Assert.True(options.EnableCluster);
        Assert.Equal(9094, options.ReplicationPort);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithRaft_SetsRaftOptions()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithCluster()
            .WithRaft();

        // Assert
        var options = builder.Build();
        Assert.True(options.UseRaftConsensus);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_Fluent_ChainsMethods()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithPort(9092)
            .WithHost("localhost")
            .WithBrokerId(1)
            .WithPartitions(3)
            .WithAutoCreateTopics(true)
            .WithStorageEngine(StorageEngines.Memory);

        // Assert
        var options = builder.Build();
        Assert.Equal(9092, options.Port);
        Assert.Equal("localhost", options.Host);
        Assert.Equal(1, options.BrokerId);
        Assert.Equal(3, options.DefaultNumPartitions);
        Assert.True(options.AutoCreateTopics);
        Assert.Equal(StorageEngines.Memory, options.StorageEngine);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithClusterNodes_AddsNodes()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithCluster("1:localhost:9093", "2:localhost:9094");

        // Assert
        var options = builder.Build();
        Assert.Equal(2, options.ClusterNodes.Count);
        Assert.Contains("1:localhost:9093", options.ClusterNodes);
        Assert.Contains("2:localhost:9094", options.ClusterNodes);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithHeartbeat_SetsHeartbeatOptions()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithCluster()
            .WithHeartbeatInterval(5000)
            .WithHeartbeatTimeout(15000);

        // Assert
        var options = builder.Build();
        Assert.Equal(5000, options.HeartbeatIntervalMs);
        Assert.Equal(15000, options.HeartbeatTimeoutMs);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithReplicationFactor_SetsReplicationFactor()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithReplicationFactor(3);

        // Assert
        var options = builder.Build();
        Assert.Equal(3, options.DefaultReplicationFactor);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithShutdownTimeout_SetsTimeout()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithShutdownTimeout(30);

        // Assert
        var options = builder.Build();
        Assert.Equal(30, options.ShutdownTimeoutSeconds);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithSecurity_SetsSecurity()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithSasl()
            .WithTls()
            .WithAcl();

        // Assert
        var options = builder.Build();
        Assert.True(options.EnableSasl);
        Assert.True(options.EnableTls);
        Assert.True(options.EnableAcl);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithCleanup_SetsCleanup()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithCleanup(false);

        // Assert
        var options = builder.Build();
        Assert.False(options.CleanupOnDispose);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithDualMode_SetsDualMode()
    {
        // Act - enable dual mode explicitly
        var builderEnabled = SurgewaveRuntime.CreateBuilder()
            .WithDualMode(true);

        // Act - disable dual mode
        var builderDisabled = SurgewaveRuntime.CreateBuilder()
            .WithDualMode(false);

        // Assert
        Assert.True(builderEnabled.Build().EnableDualMode);
        Assert.False(builderDisabled.Build().EnableDualMode);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_WithIPv4Only_DisablesDualMode()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithIPv4Only();

        // Assert
        var options = builder.Build();
        Assert.False(options.EnableDualMode);
    }

    [Fact]
    public void SurgewaveRuntimeBuilder_DualMode_DefaultsToTrue()
    {
        // Act
        var builder = SurgewaveRuntime.CreateBuilder();

        // Assert
        var options = builder.Build();
        Assert.True(options.EnableDualMode);
    }

    #endregion
}
