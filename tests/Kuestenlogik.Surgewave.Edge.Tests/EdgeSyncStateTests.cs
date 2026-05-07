using Xunit;

namespace Kuestenlogik.Surgewave.Edge.Tests;

/// <summary>
/// Tests for EdgeSyncState - offset tracking, persistence, and sync/failure recording.
/// </summary>
public sealed class EdgeSyncStateTests
{
    #region Default State Tests

    [Fact]
    public void NewState_HasDefaultValues()
    {
        var state = new EdgeSyncState();

        Assert.Equal("", state.EdgeId);
        Assert.Empty(state.SyncedOffsets);
        Assert.Equal(0, state.TotalMessagesSynced);
        Assert.False(state.IsOnline);
        Assert.Equal(0, state.ConsecutiveFailures);
        Assert.Equal(DateTimeOffset.MinValue, state.LastSyncAt);
    }

    #endregion

    #region SetSyncedOffset / GetSyncedOffset Tests

    [Fact]
    public void GetSyncedOffset_UnknownTopic_ReturnsMinusOne()
    {
        var state = new EdgeSyncState();
        Assert.Equal(-1, state.GetSyncedOffset("nonexistent", 0));
    }

    [Fact]
    public void GetSyncedOffset_UnknownPartition_ReturnsMinusOne()
    {
        var state = new EdgeSyncState();
        state.SetSyncedOffset("topic", 0, 100);

        Assert.Equal(-1, state.GetSyncedOffset("topic", 1));
    }

    [Fact]
    public void SetSyncedOffset_NewTopicAndPartition_CreatesEntry()
    {
        var state = new EdgeSyncState();

        state.SetSyncedOffset("sensor-data", 0, 42);

        Assert.Equal(42, state.GetSyncedOffset("sensor-data", 0));
    }

    [Fact]
    public void SetSyncedOffset_Update_OverwritesExisting()
    {
        var state = new EdgeSyncState();

        state.SetSyncedOffset("topic", 0, 100);
        state.SetSyncedOffset("topic", 0, 200);

        Assert.Equal(200, state.GetSyncedOffset("topic", 0));
    }

    [Fact]
    public void SetSyncedOffset_MultipleTopicsAndPartitions_TracksIndependently()
    {
        var state = new EdgeSyncState();

        state.SetSyncedOffset("topic-a", 0, 10);
        state.SetSyncedOffset("topic-a", 1, 20);
        state.SetSyncedOffset("topic-b", 0, 30);

        Assert.Equal(10, state.GetSyncedOffset("topic-a", 0));
        Assert.Equal(20, state.GetSyncedOffset("topic-a", 1));
        Assert.Equal(30, state.GetSyncedOffset("topic-b", 0));
        Assert.Equal(-1, state.GetSyncedOffset("topic-b", 1));
    }

    #endregion

    #region RecordSync Tests

    [Fact]
    public void RecordSync_IncrementsTotal()
    {
        var state = new EdgeSyncState();

        state.RecordSync(100);
        Assert.Equal(100, state.TotalMessagesSynced);

        state.RecordSync(50);
        Assert.Equal(150, state.TotalMessagesSynced);
    }

    [Fact]
    public void RecordSync_SetsLastSyncAt()
    {
        var state = new EdgeSyncState();
        var before = DateTimeOffset.UtcNow;

        state.RecordSync(1);

        Assert.True(state.LastSyncAt >= before);
        Assert.True(state.LastSyncAt <= DateTimeOffset.UtcNow);
    }

    [Fact]
    public void RecordSync_ResetsConsecutiveFailures()
    {
        var state = new EdgeSyncState();

        state.RecordFailure();
        state.RecordFailure();
        Assert.Equal(2, state.ConsecutiveFailures);

        state.RecordSync(1);
        Assert.Equal(0, state.ConsecutiveFailures);
    }

    [Fact]
    public void RecordSync_ZeroMessages_StillResets()
    {
        var state = new EdgeSyncState();
        state.RecordFailure();

        state.RecordSync(0);

        Assert.Equal(0, state.ConsecutiveFailures);
        Assert.Equal(0, state.TotalMessagesSynced);
    }

    #endregion

    #region RecordFailure Tests

    [Fact]
    public void RecordFailure_IncrementsConsecutiveFailures()
    {
        var state = new EdgeSyncState();

        state.RecordFailure();
        Assert.Equal(1, state.ConsecutiveFailures);

        state.RecordFailure();
        Assert.Equal(2, state.ConsecutiveFailures);

        state.RecordFailure();
        Assert.Equal(3, state.ConsecutiveFailures);
    }

    [Fact]
    public void RecordFailure_DoesNotAffectTotalSynced()
    {
        var state = new EdgeSyncState();
        state.RecordSync(50);

        state.RecordFailure();

        Assert.Equal(50, state.TotalMessagesSynced);
    }

    #endregion

    #region SaveToFile / LoadFromFile Tests

    [Fact]
    public void SaveAndLoad_RoundTrip_PreservesAllFields()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"edge-state-{Guid.NewGuid():N}.json");
        try
        {
            var state = new EdgeSyncState
            {
                EdgeId = "edge-42",
                TotalMessagesSynced = 1000,
                IsOnline = true,
                ConsecutiveFailures = 2,
                LastSyncAt = DateTimeOffset.UtcNow
            };
            state.SetSyncedOffset("topic-a", 0, 500);
            state.SetSyncedOffset("topic-a", 1, 600);
            state.SetSyncedOffset("topic-b", 0, 300);

            state.SaveToFile(tempFile);
            var loaded = EdgeSyncState.LoadFromFile(tempFile);

            Assert.Equal("edge-42", loaded.EdgeId);
            Assert.Equal(1000, loaded.TotalMessagesSynced);
            Assert.True(loaded.IsOnline);
            Assert.Equal(2, loaded.ConsecutiveFailures);
            Assert.Equal(500, loaded.GetSyncedOffset("topic-a", 0));
            Assert.Equal(600, loaded.GetSyncedOffset("topic-a", 1));
            Assert.Equal(300, loaded.GetSyncedOffset("topic-b", 0));
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void LoadFromFile_NonexistentFile_ReturnsNewState()
    {
        var path = Path.Combine(Path.GetTempPath(), $"nonexistent-{Guid.NewGuid():N}.json");

        var state = EdgeSyncState.LoadFromFile(path);

        Assert.NotNull(state);
        Assert.Equal("", state.EdgeId);
        Assert.Empty(state.SyncedOffsets);
        Assert.Equal(0, state.TotalMessagesSynced);
    }

    [Fact]
    public void SaveToFile_OverwritesExisting()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"edge-overwrite-{Guid.NewGuid():N}.json");
        try
        {
            var state1 = new EdgeSyncState { EdgeId = "v1" };
            state1.SaveToFile(tempFile);

            var state2 = new EdgeSyncState { EdgeId = "v2" };
            state2.SaveToFile(tempFile);

            var loaded = EdgeSyncState.LoadFromFile(tempFile);
            Assert.Equal("v2", loaded.EdgeId);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void SaveToFile_UsesAtomicWrite()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"edge-atomic-{Guid.NewGuid():N}.json");
        try
        {
            var state = new EdgeSyncState { EdgeId = "atomic-test" };
            state.SaveToFile(tempFile);

            // The temp file should not exist after save
            Assert.False(File.Exists(tempFile + ".tmp"));
            // The final file should exist
            Assert.True(File.Exists(tempFile));
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    #endregion

    #region Thread Safety Tests

    [Fact]
    public void ConcurrentOperations_DoNotThrow()
    {
        var state = new EdgeSyncState();
        var exceptions = new List<Exception>();

        Parallel.For(0, 100, i =>
        {
            try
            {
                state.SetSyncedOffset($"topic-{i % 5}", i % 3, i);
                state.GetSyncedOffset($"topic-{i % 5}", i % 3);
                if (i % 3 == 0) state.RecordSync(1);
                if (i % 4 == 0) state.RecordFailure();
            }
            catch (Exception ex)
            {
                lock (exceptions) exceptions.Add(ex);
            }
        });

        Assert.Empty(exceptions);
    }

    #endregion
}
