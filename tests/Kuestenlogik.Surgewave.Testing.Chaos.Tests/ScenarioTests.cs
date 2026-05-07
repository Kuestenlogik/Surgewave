using Kuestenlogik.Surgewave.Testing.Chaos;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Testing.Chaos.Tests;

/// <summary>
/// Unit tests for chaos scenario helpers (NetworkPartition, BrokerCrash, DiskFailure, LatencyInjection).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ScenarioTests
{
    [Fact]
    public void NetworkPartition_ActivatesFaults_ForTargetedPeers()
    {
        // Arrange
        var engine = new ChaosEngine();

        // Act - partition broker 1 from brokers 2 and 3
        using var scenario = NetworkPartitionScenario.Create(engine, isolatedBrokerId: 1, otherBrokerIds: [2, 3]);

        // Assert - bidirectional partition faults should be active
        Assert.True(engine.IsFaultActive(FaultType.NetworkPartition, brokerId: 1, peerId: 2));
        Assert.True(engine.IsFaultActive(FaultType.NetworkPartition, brokerId: 1, peerId: 3));
        Assert.True(engine.IsFaultActive(FaultType.NetworkPartition, brokerId: 2, peerId: 1));
        Assert.True(engine.IsFaultActive(FaultType.NetworkPartition, brokerId: 3, peerId: 1));
    }

    [Fact]
    public void NetworkPartition_Heal_DeactivatesFaults()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scenario = NetworkPartitionScenario.Create(engine, isolatedBrokerId: 1, otherBrokerIds: [2, 3]);

        // Act
        scenario.Heal();

        // Assert
        Assert.Empty(engine.ActiveFaults);
    }

    [Fact]
    public void BrokerCrash_ActivatesNodeCrashFault()
    {
        // Arrange
        var engine = new ChaosEngine();

        // Act
        using var scenario = BrokerCrashScenario.Create(engine, brokerId: 1);

        // Assert
        Assert.True(engine.IsFaultActive(FaultType.NodeCrash, brokerId: 1));
    }

    [Fact]
    public void BrokerCrash_Recover_DeactivatesFault()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scenario = BrokerCrashScenario.Create(engine, brokerId: 1);

        // Act
        scenario.Recover();

        // Assert
        Assert.Empty(engine.ActiveFaults);
    }

    [Fact]
    public void DiskFailure_ActivatesDiskIoError()
    {
        // Arrange
        var engine = new ChaosEngine();

        // Act
        using var scenario = DiskFailureScenario.Create(engine, brokerId: 1);

        // Assert
        Assert.True(engine.IsFaultActive(FaultType.DiskIoError, brokerId: 1));
    }

    [Fact]
    public void LatencyInjection_SetsConfiguredLatency()
    {
        // Arrange
        var engine = new ChaosEngine();
        var latency = TimeSpan.FromMilliseconds(250);

        // Act
        using var scenario = LatencyInjectionScenario.Create(engine, brokerId: 1, latency: latency);

        // Assert
        var injected = engine.GetInjectedLatency(FaultType.SlowNetwork, brokerId: 1);
        Assert.NotNull(injected);
        Assert.Equal(latency, injected.Value);
    }
}
