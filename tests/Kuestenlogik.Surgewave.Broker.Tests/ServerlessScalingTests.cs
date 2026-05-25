using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kuestenlogik.Surgewave.Broker.Serverless;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Unit tests for the serverless scaling subsystem.
/// Covers lifecycle state management, drain coordination, cold start optimization,
/// scale decision engine, metrics, and top-level coordinator.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ServerlessScalingTests : IDisposable
{
    private readonly ServerlessConfig _config;
    private readonly ServerlessMetrics _metrics;

    public ServerlessScalingTests()
    {
        _config = new ServerlessConfig { Enabled = true };
        _metrics = new ServerlessMetrics();
    }

    public void Dispose()
    {
        _metrics.Dispose();
    }

    // --- Helpers ---

    private static DrainCoordinator CreateDrainCoordinator(
        ServerlessConfig? config = null,
        Func<CancellationToken, Task>? flushCallback = null)
    {
        return new DrainCoordinator(
            NullLogger<DrainCoordinator>.Instance,
            config ?? new ServerlessConfig(),
            flushCallback ?? (_ => Task.CompletedTask));
    }

    private static ColdStartOptimizer CreateColdStartOptimizer(ServerlessConfig? config = null)
    {
        return new ColdStartOptimizer(
            NullLogger<ColdStartOptimizer>.Instance,
            config ?? new ServerlessConfig());
    }

    private static ScaleDecisionEngine CreateScaleDecisionEngine(ServerlessConfig? config = null)
    {
        return new ScaleDecisionEngine(
            NullLogger<ScaleDecisionEngine>.Instance,
            config ?? new ServerlessConfig());
    }

    private static ClusterState CreateClusterStateWithPartitions(int partitionCount, int localBrokerId = 0)
    {
        var state = new ClusterState { LocalBrokerId = localBrokerId };
        state.RegisterBroker(localBrokerId, "localhost", 9092);

        for (int i = 0; i < partitionCount; i++)
        {
            var topicName = $"topic-{i / 4}";
            var partitionId = i % 4;

            if (state.GetTopic(topicName) == null)
            {
                state.AddTopic(new TopicMetadata
                {
                    Name = topicName,
                    TopicId = Guid.NewGuid(),
                    PartitionCount = 4,
                    ReplicationFactor = 1,
                    Config = new Dictionary<string, string>(),
                    CreatedAt = DateTime.UtcNow
                });
            }

            var tp = new TopicPartition { Topic = topicName, Partition = partitionId };
            state.AssignReplicas(tp, [localBrokerId]);
        }

        return state;
    }

    private static ScaleMetrics CreateMetrics(
        double cpuPercent = 50,
        int connections = 10,
        double produceRate = 100,
        double fetchRate = 50,
        long unflushedBytes = 0,
        int brokerCount = 2,
        DateTimeOffset? timestamp = null)
    {
        return new ScaleMetrics
        {
            CpuUsagePercent = cpuPercent,
            ActiveConnections = connections,
            ProduceRatePerSecond = produceRate,
            FetchRatePerSecond = fetchRate,
            UnflushedBytes = unflushedBytes,
            CurrentBrokerCount = brokerCount,
            Timestamp = timestamp ?? DateTimeOffset.UtcNow
        };
    }

    // =========================================================================
    // 1. ServerlessLifecycleState defaults
    // =========================================================================

    [Fact]
    public void ServerlessLifecycleState_InitialState_IsColdStarting()
    {
        var coordinator = new ServerlessBrokerCoordinator(
            NullLogger<ServerlessBrokerCoordinator>.Instance,
            _config,
            CreateDrainCoordinator(),
            CreateColdStartOptimizer(),
            CreateScaleDecisionEngine(),
            _metrics);

        Assert.Equal(ServerlessLifecycleState.ColdStarting, coordinator.State);

        coordinator.Dispose();
    }

    // =========================================================================
    // 2. DrainCoordinator transitions to Draining
    // =========================================================================

    [Fact]
    public async Task DrainCoordinator_TransitionsToDraining()
    {
        var drain = CreateDrainCoordinator();

        await drain.StartDrainAsync();

        Assert.Equal(ServerlessLifecycleState.Draining, drain.CurrentState);
    }

    // =========================================================================
    // 3. DrainCoordinator waits for flush then transitions to ReadyToTerminate
    // =========================================================================

    [Fact]
    public async Task DrainCoordinator_WaitsForFlush_ThenReadyToTerminate()
    {
        var flushed = false;
        var drain = CreateDrainCoordinator(
            flushCallback: _ =>
            {
                flushed = true;
                return Task.CompletedTask;
            });

        await drain.StartDrainAsync();
        await drain.WaitForDrainCompleteAsync();

        Assert.True(flushed);
        Assert.Equal(ServerlessLifecycleState.ReadyToTerminate, drain.CurrentState);
    }

    // =========================================================================
    // 4. DrainCoordinator timeout forces ReadyToTerminate
    // =========================================================================

    [Fact]
    public async Task DrainCoordinator_Timeout_ForcesReadyToTerminate()
    {
        var config = new ServerlessConfig
        {
            DrainTimeout = TimeSpan.FromMilliseconds(50)
        };

        var drain = CreateDrainCoordinator(
            config: config,
            flushCallback: async ct =>
            {
                // Simulate a long-running flush that exceeds the timeout
                await Task.Delay(TimeSpan.FromSeconds(10), ct);
            });

        await drain.StartDrainAsync();
        await drain.WaitForDrainCompleteAsync();

        Assert.Equal(ServerlessLifecycleState.ReadyToTerminate, drain.CurrentState);
    }

    // =========================================================================
    // 5. Concurrent drain calls only drain once
    // =========================================================================

    [Fact]
    public async Task DrainCoordinator_ConcurrentDrainCalls_OnlyDrainsOnce()
    {
        var flushCount = 0;
        var drain = CreateDrainCoordinator(
            flushCallback: _ =>
            {
                Interlocked.Increment(ref flushCount);
                return Task.CompletedTask;
            });

        // Fire multiple concurrent start drain calls
        var tasks = Enumerable.Range(0, 10)
            .Select(_ => drain.StartDrainAsync())
            .ToArray();

        await Task.WhenAll(tasks);
        await drain.WaitForDrainCompleteAsync();

        // Flush should only be called once (from WaitForDrainCompleteAsync)
        Assert.Equal(1, flushCount);
        Assert.Equal(ServerlessLifecycleState.ReadyToTerminate, drain.CurrentState);
    }

    // =========================================================================
    // 6. ColdStartOptimizer returns report
    // =========================================================================

    [Fact]
    public async Task ColdStartOptimizer_ReturnsReport()
    {
        var optimizer = CreateColdStartOptimizer();
        var clusterState = CreateClusterStateWithPartitions(8);

        var report = await optimizer.OptimizeColdStartAsync(clusterState);

        Assert.Equal(8, report.PartitionsLoaded);
        Assert.True(report.PartitionsPreWarmed > 0);
        Assert.Equal(ServerlessLifecycleState.Active, report.FinalState);
        Assert.True(report.TotalDuration >= TimeSpan.Zero);
    }

    // =========================================================================
    // 7. ColdStartOptimizer with empty cluster completes quickly
    // =========================================================================

    [Fact]
    public async Task ColdStartOptimizer_EmptyCluster_QuickStart()
    {
        var optimizer = CreateColdStartOptimizer();
        var clusterState = new ClusterState();

        var report = await optimizer.OptimizeColdStartAsync(clusterState);

        Assert.Equal(0, report.PartitionsLoaded);
        Assert.Equal(0, report.PartitionsPreWarmed);
        Assert.Equal(0, report.BytesPreFetched);
        Assert.Equal(ServerlessLifecycleState.Active, report.FinalState);
    }

    // =========================================================================
    // 8. ScaleDecisionEngine - high load scales up
    // =========================================================================

    [Fact]
    public void ScaleDecisionEngine_HighLoad_ScalesUp()
    {
        var engine = CreateScaleDecisionEngine();
        var metrics = CreateMetrics(cpuPercent: 85, brokerCount: 2);

        var decision = engine.Evaluate(metrics);

        Assert.Equal(ScaleAction.ScaleUp, decision.Action);
        Assert.Equal(3, decision.TargetBrokerCount);
        Assert.Contains("CPU", decision.Reason);
    }

    // =========================================================================
    // 9. ScaleDecisionEngine - low load scales down
    // =========================================================================

    [Fact]
    public void ScaleDecisionEngine_LowLoad_ScalesDown()
    {
        var engine = CreateScaleDecisionEngine();
        var metrics = CreateMetrics(cpuPercent: 10, brokerCount: 3);

        var decision = engine.Evaluate(metrics);

        Assert.Equal(ScaleAction.ScaleDown, decision.Action);
        Assert.Equal(2, decision.TargetBrokerCount);
    }

    // =========================================================================
    // 10. ScaleDecisionEngine - stabilization window prevents flapping
    // =========================================================================

    [Fact]
    public void ScaleDecisionEngine_StabilizationWindow_PreventsFlapping()
    {
        var config = new ServerlessConfig
        {
            StabilizationWindow = TimeSpan.FromMinutes(5)
        };
        var engine = CreateScaleDecisionEngine(config);

        var now = DateTimeOffset.UtcNow;

        // First evaluation: scale up
        var metrics1 = CreateMetrics(cpuPercent: 85, brokerCount: 2, timestamp: now);
        var decision1 = engine.Evaluate(metrics1);
        Assert.Equal(ScaleAction.ScaleUp, decision1.Action);

        // Second evaluation shortly after: should be NoChange due to stabilization
        var metrics2 = CreateMetrics(cpuPercent: 10, brokerCount: 3, timestamp: now.AddSeconds(30));
        var decision2 = engine.Evaluate(metrics2);
        Assert.Equal(ScaleAction.NoChange, decision2.Action);
        Assert.Contains("stabilization", decision2.Reason, StringComparison.OrdinalIgnoreCase);
    }

    // =========================================================================
    // 11. ScaleDecisionEngine - at min brokers, no scale down
    // =========================================================================

    [Fact]
    public void ScaleDecisionEngine_AtMinBrokers_NoScaleDown()
    {
        var config = new ServerlessConfig { MinBrokers = 2 };
        var engine = CreateScaleDecisionEngine(config);
        var metrics = CreateMetrics(cpuPercent: 10, brokerCount: 2);

        var decision = engine.Evaluate(metrics);

        Assert.Equal(ScaleAction.NoChange, decision.Action);
        Assert.Equal(2, decision.TargetBrokerCount);
    }

    // =========================================================================
    // 12. ScaleDecisionEngine - at max brokers, no scale up
    // =========================================================================

    [Fact]
    public void ScaleDecisionEngine_AtMaxBrokers_NoScaleUp()
    {
        var config = new ServerlessConfig { MaxBrokers = 3 };
        var engine = CreateScaleDecisionEngine(config);
        var metrics = CreateMetrics(cpuPercent: 95, brokerCount: 3);

        var decision = engine.Evaluate(metrics);

        Assert.Equal(ScaleAction.NoChange, decision.Action);
        Assert.Contains("max brokers", decision.Reason, StringComparison.OrdinalIgnoreCase);
    }

    // =========================================================================
    // 13. ScaleDecisionEngine - idle cluster scales to zero
    // =========================================================================

    [Fact]
    public void ScaleDecisionEngine_IdleCluster_ScalesToZero()
    {
        var config = new ServerlessConfig { MinBrokers = 0 };
        var engine = CreateScaleDecisionEngine(config);
        var metrics = CreateMetrics(
            cpuPercent: 0,
            connections: 0,
            produceRate: 0,
            fetchRate: 0,
            unflushedBytes: 0,
            brokerCount: 1);

        var decision = engine.Evaluate(metrics);

        Assert.Equal(ScaleAction.ScaleDown, decision.Action);
        Assert.Equal(0, decision.TargetBrokerCount);
        Assert.Contains("zero", decision.Reason, StringComparison.OrdinalIgnoreCase);
    }

    // =========================================================================
    // 14. ServerlessConfig default values
    // =========================================================================

    [Fact]
    public void ServerlessConfig_DefaultValues_Correct()
    {
        var config = new ServerlessConfig();

        Assert.False(config.Enabled);
        Assert.Equal(TimeSpan.FromMinutes(5), config.IdleTimeout);
        Assert.Equal(TimeSpan.FromSeconds(60), config.DrainTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), config.ColdStartTimeout);
        Assert.Equal(0, config.MinBrokers);
        Assert.Equal(10, config.MaxBrokers);
        Assert.Equal(70.0, config.ScaleUpThresholdPercent);
        Assert.Equal(20.0, config.ScaleDownThresholdPercent);
        Assert.Equal(TimeSpan.FromMinutes(2), config.StabilizationWindow);
        Assert.Equal(10, config.WarmupPartitions);
        Assert.Empty(config.PreferredObjectStoreProviders);
    }

    // =========================================================================
    // 15. ServerlessMetrics records drain and cold start
    // =========================================================================

    [Fact]
    public void ServerlessMetrics_RecordsDrainAndColdStart()
    {
        using var metrics = new ServerlessMetrics();
        var recorded = new Dictionary<string, List<(object Value, TagList Tags)>>();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == ServerlessMetrics.MeterName)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            if (!recorded.TryGetValue(instrument.Name, out var list))
            {
                list = [];
                recorded[instrument.Name] = list;
            }
            list.Add((value, new TagList()));
        });
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
        {
            if (!recorded.TryGetValue(instrument.Name, out var list))
            {
                list = [];
                recorded[instrument.Name] = list;
            }
            list.Add((value, new TagList()));
        });
        listener.Start();

        // Record events
        metrics.RecordDrain();
        metrics.RecordDrainDuration(150.0);
        metrics.RecordColdStart();
        metrics.RecordColdStartDuration(500.0);
        metrics.RecordScaleUp();
        metrics.RecordScaleDown();

        listener.RecordObservableInstruments();

        Assert.True(recorded.ContainsKey("surgewave_serverless_drain_total"));
        Assert.True(recorded.ContainsKey("surgewave_serverless_drain_duration_ms"));
        Assert.True(recorded.ContainsKey("surgewave_serverless_cold_starts_total"));
        Assert.True(recorded.ContainsKey("surgewave_serverless_cold_start_duration_ms"));
        Assert.True(recorded.ContainsKey("surgewave_serverless_scale_up_total"));
        Assert.True(recorded.ContainsKey("surgewave_serverless_scale_down_total"));
    }

    // =========================================================================
    // 16. ServerlessBrokerCoordinator initialize transitions to Active
    // =========================================================================

    [Fact]
    public async Task ServerlessBrokerCoordinator_Initialize_TransitionsToActive()
    {
        using var metrics = new ServerlessMetrics();
        var coordinator = new ServerlessBrokerCoordinator(
            NullLogger<ServerlessBrokerCoordinator>.Instance,
            _config,
            CreateDrainCoordinator(),
            CreateColdStartOptimizer(),
            CreateScaleDecisionEngine(),
            metrics);

        Assert.Equal(ServerlessLifecycleState.ColdStarting, coordinator.State);

        var clusterState = CreateClusterStateWithPartitions(4);
        await coordinator.InitializeAsync(clusterState);

        Assert.Equal(ServerlessLifecycleState.Active, coordinator.State);

        coordinator.Dispose();
    }

    // =========================================================================
    // 17. ServerlessBrokerCoordinator drain transitions correctly
    // =========================================================================

    [Fact]
    public async Task ServerlessBrokerCoordinator_Drain_TransitionsCorrectly()
    {
        using var metrics = new ServerlessMetrics();
        var coordinator = new ServerlessBrokerCoordinator(
            NullLogger<ServerlessBrokerCoordinator>.Instance,
            _config,
            CreateDrainCoordinator(),
            CreateColdStartOptimizer(),
            CreateScaleDecisionEngine(),
            metrics);

        // First initialize to Active
        var clusterState = CreateClusterStateWithPartitions(4);
        await coordinator.InitializeAsync(clusterState);
        Assert.Equal(ServerlessLifecycleState.Active, coordinator.State);

        // Then drain
        await coordinator.InitiateDrainAsync();

        Assert.Equal(ServerlessLifecycleState.ReadyToTerminate, coordinator.State);

        coordinator.Dispose();
    }
}
