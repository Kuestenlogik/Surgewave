using Kuestenlogik.Surgewave.Testing.Chaos;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Testing.Chaos.Tests;

/// <summary>
/// Unit tests for ChaosTimeline event recording and retrieval.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ChaosTimelineTests
{
    private readonly ITestOutputHelper _output;

    public ChaosTimelineTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void Record_AddsEvent()
    {
        // Arrange
        var timeline = new ChaosTimeline();
        var scope = new FaultScope { BrokerId = 1 };

        // Act
        timeline.Record(new ChaosEvent(
            EventType: ChaosEventType.Activated,
            FaultType: FaultType.NetworkPartition,
            Scope: scope,
            Timestamp: DateTimeOffset.UtcNow,
            Description: "Test partition activated"));

        // Assert
        var events = timeline.GetEvents();
        Assert.Single(events);
        Assert.Equal(FaultType.NetworkPartition, events[0].FaultType);
        Assert.Equal(ChaosEventType.Activated, events[0].EventType);
        Assert.Contains("Test partition", events[0].Description);
    }

    [Fact]
    public void GetEvents_ReturnsInOrder()
    {
        // Arrange
        var timeline = new ChaosTimeline();
        var now = DateTimeOffset.UtcNow;

        timeline.Record(new ChaosEvent(
            EventType: ChaosEventType.Activated,
            FaultType: FaultType.NetworkPartition,
            Scope: new FaultScope { BrokerId = 1 },
            Timestamp: now,
            Description: "First"));

        timeline.Record(new ChaosEvent(
            EventType: ChaosEventType.Activated,
            FaultType: FaultType.DiskIoError,
            Scope: new FaultScope { BrokerId = 2 },
            Timestamp: now.AddMilliseconds(10),
            Description: "Second"));

        timeline.Record(new ChaosEvent(
            EventType: ChaosEventType.Deactivated,
            FaultType: FaultType.NetworkPartition,
            Scope: new FaultScope { BrokerId = 1 },
            Timestamp: now.AddMilliseconds(20),
            Description: "Third"));

        // Act
        var events = timeline.GetEvents();

        // Assert
        Assert.Equal(3, events.Count);
        Assert.Contains("First", events[0].Description);
        Assert.Contains("Second", events[1].Description);
        Assert.Contains("Third", events[2].Description);
    }

    [Fact]
    public void DumpToOutput_FormatsCorrectly()
    {
        // Arrange
        var timeline = new ChaosTimeline();

        timeline.Record(ChaosEventType.Activated, FaultType.NetworkPartition,
            new FaultScope { BrokerId = 1 },
            "Partition between broker 1 and 2");

        timeline.Record(ChaosEventType.Deactivated, FaultType.NetworkPartition,
            new FaultScope { BrokerId = 1 },
            "Partition healed");

        // Act - dump to captured output (should not throw)
        var lines = new List<string>();
        timeline.DumpToOutput(line => lines.Add(line));

        // Assert - header + 2 events + footer = at least 4 lines
        Assert.True(lines.Count >= 4, $"Expected at least 4 lines but got {lines.Count}");

        // Verify output contains expected information
        var allOutput = string.Join("\n", lines);
        Assert.Contains("NetworkPartition", allOutput);
        Assert.Contains("Activated", allOutput);
        Assert.Contains("Deactivated", allOutput);

        // Also dump to xunit output for visual verification
        foreach (var line in lines)
        {
            _output.WriteLine(line);
        }
    }
}
