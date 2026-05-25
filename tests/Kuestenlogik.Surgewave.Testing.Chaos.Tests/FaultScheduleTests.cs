using Kuestenlogik.Surgewave.Testing.Chaos;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Testing.Chaos.Tests;

/// <summary>
/// Unit tests for FaultSchedule timed fault activation and deactivation.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class FaultScheduleTests
{
    [Fact]
    public async Task Schedule_ActivatesAfterDelay()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scope = new FaultScope { BrokerId = 1 };

        // Act - schedule activation after 50ms
        using var schedule = FaultSchedule.Create(engine, FaultType.NetworkPartition, scope,
            activateAfter: TimeSpan.FromMilliseconds(50));

        // Should not be active immediately
        Assert.Empty(engine.ActiveFaults);

        // Wait for activation
        await Task.Delay(TimeSpan.FromMilliseconds(200));

        // Assert - should now be active
        Assert.NotEmpty(engine.ActiveFaults);
        Assert.True(engine.IsFaultActive(FaultType.NetworkPartition, brokerId: 1));
    }

    [Fact]
    public async Task Schedule_DeactivatesAfterDuration()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scope = new FaultScope { BrokerId = 1 };

        // Act - activate immediately with 500ms duration (generous for CI)
        using var schedule = FaultSchedule.Create(engine, FaultType.DiskIoError, scope,
            activateAfter: TimeSpan.Zero,
            duration: TimeSpan.FromMilliseconds(500));

        // Wait briefly for activation — well within the 500ms window
        await Task.Delay(TimeSpan.FromMilliseconds(100));

        // Should be active during duration
        Assert.NotEmpty(engine.ActiveFaults);

        // Wait for deactivation (500ms duration + generous margin)
        await Task.Delay(TimeSpan.FromMilliseconds(800));

        // Assert - should now be inactive
        Assert.Empty(engine.ActiveFaults);
    }

    [Fact]
    public async Task Dispose_CancelsPendingSchedule()
    {
        // Arrange
        var engine = new ChaosEngine();
        var scope = new FaultScope { BrokerId = 1 };

        // Schedule activation after 200ms
        var schedule = FaultSchedule.Create(engine, FaultType.NetworkPartition, scope,
            activateAfter: TimeSpan.FromMilliseconds(200));

        // Act - dispose before activation fires
        schedule.Dispose();

        // Wait past the scheduled time
        await Task.Delay(TimeSpan.FromMilliseconds(300));

        // Assert - should never have activated
        Assert.Empty(engine.ActiveFaults);
    }
}
