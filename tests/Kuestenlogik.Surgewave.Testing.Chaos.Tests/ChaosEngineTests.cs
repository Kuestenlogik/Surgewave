using Kuestenlogik.Surgewave.Testing.Chaos;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Testing.Chaos.Tests;

/// <summary>
/// Unit tests for the ChaosEngine fault activation and deactivation lifecycle.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ChaosEngineTests
{
    [Fact]
    public void ActivateFault_ReturnsUniqueId()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scope = new FaultScope { BrokerId = 1 };

        // Act
        var id1 = engine.ActivateFault(FaultType.NetworkPartition, scope);
        var id2 = engine.ActivateFault(FaultType.DiskIoError, scope);

        // Assert
        Assert.NotNull(id1);
        Assert.NotNull(id2);
        Assert.NotEqual(id1, id2);
    }

    [Fact]
    public void DeactivateFault_RemovesFault()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scope = new FaultScope { BrokerId = 1 };
        var id = engine.ActivateFault(FaultType.NetworkPartition, scope);

        // Act
        engine.DeactivateFault(id);

        // Assert - fault should no longer be active (using wildcard brokerId=-1 to match any)
        Assert.Empty(engine.ActiveFaults);
    }

    [Fact]
    public void DeactivateAll_ClearsAllFaults()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scope = new FaultScope { BrokerId = 1 };
        engine.ActivateFault(FaultType.NetworkPartition, scope);
        engine.ActivateFault(FaultType.DiskIoError, scope);
        engine.ActivateFault(FaultType.SlowNetwork, scope);

        // Act
        engine.DeactivateAll();

        // Assert
        Assert.Empty(engine.ActiveFaults);
    }

    [Fact]
    public void IsFaultActive_MatchesByType()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scope = new FaultScope { BrokerId = 1 };
        engine.ActivateFault(FaultType.DiskIoError, scope);

        // Act & Assert
        Assert.True(engine.IsFaultActive(FaultType.DiskIoError));
        Assert.False(engine.IsFaultActive(FaultType.NetworkPartition));
    }

    [Fact]
    public void IsFaultActive_MatchesByBrokerId()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scope = new FaultScope { BrokerId = 1 };
        engine.ActivateFault(FaultType.NetworkPartition, scope);

        // Act & Assert
        Assert.True(engine.IsFaultActive(FaultType.NetworkPartition, brokerId: 1));
        Assert.False(engine.IsFaultActive(FaultType.NetworkPartition, brokerId: 2));
    }

    [Fact]
    public void IsFaultActive_WithProbability_RespectsProbability()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scope = new FaultScope { BrokerId = 1, Probability = 0.0 }; // 0% probability = never active
        engine.ActivateFault(FaultType.NetworkPartition, scope);

        // Act - With 0% probability, fault should never be considered active for injection
        var activeCount = 0;
        for (var i = 0; i < 100; i++)
        {
            if (engine.IsFaultActive(FaultType.NetworkPartition, brokerId: 1))
            {
                activeCount++;
            }
        }

        // Assert - With 0% probability, should never trigger
        Assert.Equal(0, activeCount);
    }

    [Fact]
    public void GetInjectedLatency_ReturnsConfiguredLatency()
    {
        // Arrange
        var engine = new ChaosEngine();
        var latency = TimeSpan.FromMilliseconds(200);
        var scope = new FaultScope { BrokerId = 1 };
        engine.ActivateFault(FaultType.SlowNetwork, scope, latency);

        // Act
        var injectedLatency = engine.GetInjectedLatency(FaultType.SlowNetwork, brokerId: 1);

        // Assert
        Assert.NotNull(injectedLatency);
        Assert.Equal(latency, injectedLatency.Value);
    }

    [Fact]
    public void Timeline_RecordsFaultEvents()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scope = new FaultScope { BrokerId = 1 };

        // Act
        var id = engine.ActivateFault(FaultType.NetworkPartition, scope);
        engine.DeactivateFault(id);

        // Assert
        var events = engine.Timeline.GetEvents();
        Assert.True(events.Count >= 2, "Should have at least activation and deactivation events");

        // Verify we have both activation and deactivation events
        Assert.Contains(events, e => e.EventType == ChaosEventType.Activated);
        Assert.Contains(events, e => e.EventType == ChaosEventType.Deactivated);
    }
}
