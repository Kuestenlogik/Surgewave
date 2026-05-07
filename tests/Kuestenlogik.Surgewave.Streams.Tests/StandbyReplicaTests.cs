using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class StandbyReplicaTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public StandbyReplicaTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddFilter(level => level >= LogLevel.Debug);
        });
    }

    public async ValueTask DisposeAsync()
    {
        _loggerFactory.Dispose();
        await Task.CompletedTask;
    }

    [Fact]
    public void StandbyTask_Initialize_SetsRestoringState()
    {
        var store = new InMemoryKeyValueStore<string, string>("standby-store");
        var logger = _loggerFactory.CreateLogger<StandbyReplicaTests>();
        var config = new StreamsConfig
        {
            ApplicationId = "standby-test",
            BootstrapServers = "localhost:9092"
        };
        var context = new ProcessorContext(config, new StreamsMetrics(), logger);

        var standby = new StandbyTask("standby-0", "standby-store", store, logger);

        Assert.Equal(StandbyTaskState.Created, standby.State);

        standby.Initialize(context);

        Assert.Equal(StandbyTaskState.Restoring, standby.State);
    }

    [Fact]
    public void StandbyTask_UpdateReplica_TracksOffset()
    {
        var store = new InMemoryKeyValueStore<string, string>("standby-store");
        var logger = _loggerFactory.CreateLogger<StandbyReplicaTests>();
        var config = new StreamsConfig
        {
            ApplicationId = "standby-test",
            BootstrapServers = "localhost:9092"
        };
        var context = new ProcessorContext(config, new StreamsMetrics(), logger);

        var standby = new StandbyTask("standby-0", "standby-store", store, logger);
        standby.Initialize(context);

        standby.UpdateReplica([1, 2], [3, 4], 42);
        Assert.Equal(42, standby.ReplicatedOffset);
        Assert.Equal(StandbyTaskState.Running, standby.State);

        standby.UpdateReplica([5, 6], [7, 8], 100);
        Assert.Equal(100, standby.ReplicatedOffset);
    }

    [Fact]
    public void StandbyTask_Promote_ReturnsStore()
    {
        var store = new InMemoryKeyValueStore<string, string>("standby-store");
        var logger = _loggerFactory.CreateLogger<StandbyReplicaTests>();
        var config = new StreamsConfig
        {
            ApplicationId = "standby-test",
            BootstrapServers = "localhost:9092"
        };
        var context = new ProcessorContext(config, new StreamsMetrics(), logger);

        var standby = new StandbyTask("standby-0", "standby-store", store, logger);
        standby.Initialize(context);
        standby.UpdateReplica([1], [2], 50);

        var promoted = standby.Promote();

        Assert.Same(store, promoted);
        Assert.Equal(StandbyTaskState.Promoted, standby.State);
    }

    [Fact]
    public void StandbyTask_Close_ClosesStore()
    {
        var store = new InMemoryKeyValueStore<string, string>("standby-store");
        var logger = _loggerFactory.CreateLogger<StandbyReplicaTests>();
        var config = new StreamsConfig
        {
            ApplicationId = "standby-test",
            BootstrapServers = "localhost:9092"
        };
        var context = new ProcessorContext(config, new StreamsMetrics(), logger);

        var standby = new StandbyTask("standby-0", "standby-store", store, logger);
        standby.Initialize(context);

        standby.Close();
        Assert.Equal(StandbyTaskState.Closed, standby.State);

        // Double close should not throw
        standby.Close();
    }

    [Fact]
    public void StandbyConfig_Defaults()
    {
        var config = StandbyConfig.Disabled;

        Assert.Equal(0, config.NumStandbyReplicas);
        Assert.True(config.WarmupEnabled);
        Assert.Equal(1000, config.MaxReplicationBatchSize);
    }

    [Fact]
    public void StreamsConfig_StandbyDefaults()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "test",
            BootstrapServers = "localhost:9092"
        };

        Assert.Equal(0, config.Standby.NumStandbyReplicas);

        // With custom standby config
        var customConfig = new StreamsConfig
        {
            ApplicationId = "test",
            BootstrapServers = "localhost:9092",
            Standby = new StandbyConfig { NumStandbyReplicas = 2 }
        };

        Assert.Equal(2, customConfig.Standby.NumStandbyReplicas);
    }
}
