using Xunit;

namespace Kuestenlogik.Surgewave.Edge.Tests;

public sealed class EdgeSyncTests
{
    [Fact]
    public void EdgeSyncState_SaveAndLoad()
    {
        // Arrange
        var tempFile = Path.Combine(Path.GetTempPath(), $"edge-sync-test-{Guid.NewGuid():N}.json");
        try
        {
            var state = new EdgeSyncState
            {
                EdgeId = "test-edge-01",
                TotalMessagesSynced = 42,
                IsOnline = true,
                LastSyncAt = DateTimeOffset.UtcNow
            };
            state.SetSyncedOffset("sensor-data", 0, 100);
            state.SetSyncedOffset("sensor-data", 1, 200);
            state.SetSyncedOffset("alerts", 0, 50);

            // Act
            state.SaveToFile(tempFile);
            var loaded = EdgeSyncState.LoadFromFile(tempFile);

            // Assert
            Assert.Equal("test-edge-01", loaded.EdgeId);
            Assert.Equal(42, loaded.TotalMessagesSynced);
            Assert.True(loaded.IsOnline);
            Assert.Equal(100, loaded.GetSyncedOffset("sensor-data", 0));
            Assert.Equal(200, loaded.GetSyncedOffset("sensor-data", 1));
            Assert.Equal(50, loaded.GetSyncedOffset("alerts", 0));
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void EdgeSyncState_TracksOffsets()
    {
        // Arrange
        var state = new EdgeSyncState();

        // Act & Assert — initial offset is -1
        Assert.Equal(-1, state.GetSyncedOffset("my-topic", 0));

        // Set offset
        state.SetSyncedOffset("my-topic", 0, 42);
        Assert.Equal(42, state.GetSyncedOffset("my-topic", 0));

        // Update offset
        state.SetSyncedOffset("my-topic", 0, 100);
        Assert.Equal(100, state.GetSyncedOffset("my-topic", 0));

        // Different partition
        state.SetSyncedOffset("my-topic", 1, 77);
        Assert.Equal(77, state.GetSyncedOffset("my-topic", 1));
        Assert.Equal(100, state.GetSyncedOffset("my-topic", 0));
    }

    [Fact]
    public void EdgeSyncConfig_Defaults()
    {
        // Act
        var config = new EdgeSyncConfig();

        // Assert
        Assert.Equal(Environment.MachineName, config.EdgeId);
        Assert.Equal("localhost:9092", config.CloudBrokerAddress);
        Assert.Single(config.SyncTopics);
        Assert.Equal("*", config.SyncTopics[0]);
        Assert.Equal(SyncDirection.EdgeToCloud, config.Direction);
        Assert.Equal(30, config.SyncIntervalSeconds);
        Assert.Equal(1000, config.MaxBatchSize);
        Assert.Equal(100, config.OfflineBufferMaxMb);
        Assert.True(config.CompressSync);
        Assert.Equal("edge-sync-state.json", config.OfflineStateFile);
        Assert.Equal(5000, config.ConnectivityTimeoutMs);
        Assert.Equal(5, config.MaxConsecutiveFailures);
    }

    [Fact]
    public void EdgeBrokerBuilder_CreateWithSqlite()
    {
        // Act
        var builder = EdgeBrokerBuilder
            .Create("factory-01")
            .WithSqliteStorage("edge.db")
            .WithPort(0)
            .WithHost("localhost")
            .WithDataDirectory("/tmp/edge-test");

        // Assert — builder is non-null and chainable
        Assert.NotNull(builder);
    }

    [Fact]
    public void EdgeBrokerBuilder_CreateWithMemory()
    {
        // Act
        var builder = EdgeBrokerBuilder
            .Create("vehicle-01")
            .WithMemoryStorage()
            .WithPort(0)
            .WithCloudSync("cloud.example.com:9092", cfg =>
            {
                cfg.SyncIntervalSeconds = 5;
                cfg.MaxBatchSize = 500;
                cfg.Direction = SyncDirection.Bidirectional;
            })
            .WithTopics("sensor-data", "gps", "diagnostics");

        // Assert — builder is non-null and chainable
        Assert.NotNull(builder);
    }

    [Fact]
    public void SyncDirection_AllValues()
    {
        // Assert — verify all enum values exist
        var values = Enum.GetValues<SyncDirection>();
        Assert.Equal(3, values.Length);
        Assert.Contains(SyncDirection.EdgeToCloud, values);
        Assert.Contains(SyncDirection.CloudToEdge, values);
        Assert.Contains(SyncDirection.Bidirectional, values);
    }

    [Fact]
    public void EdgeSyncState_RecordsSyncAndFailures()
    {
        // Arrange
        var state = new EdgeSyncState();

        // Act — record some syncs
        state.RecordSync(100);
        Assert.Equal(100, state.TotalMessagesSynced);
        Assert.Equal(0, state.ConsecutiveFailures);
        Assert.True(state.LastSyncAt > DateTimeOffset.MinValue);

        state.RecordSync(50);
        Assert.Equal(150, state.TotalMessagesSynced);

        // Act — record failures
        state.RecordFailure();
        state.RecordFailure();
        Assert.Equal(2, state.ConsecutiveFailures);

        // Act — successful sync resets failures
        state.RecordSync(10);
        Assert.Equal(0, state.ConsecutiveFailures);
        Assert.Equal(160, state.TotalMessagesSynced);
    }

    [Fact]
    public void ConnectivityChecker_ParsesAddress()
    {
        // Valid addresses
        var (host, port) = ConnectivityChecker.ParseAddress("cloud.example.com:9092");
        Assert.Equal("cloud.example.com", host);
        Assert.Equal(9092, port);

        (host, port) = ConnectivityChecker.ParseAddress("localhost:19092");
        Assert.Equal("localhost", host);
        Assert.Equal(19092, port);

        (host, port) = ConnectivityChecker.ParseAddress("192.168.1.100:8080");
        Assert.Equal("192.168.1.100", host);
        Assert.Equal(8080, port);

        // Invalid addresses throw
        Assert.Throws<ArgumentException>(() => ConnectivityChecker.ParseAddress("no-port"));
        Assert.Throws<ArgumentException>(() => ConnectivityChecker.ParseAddress("host:0"));
        Assert.Throws<ArgumentException>(() => ConnectivityChecker.ParseAddress("host:99999"));
        Assert.Throws<ArgumentException>(() => ConnectivityChecker.ParseAddress("host:abc"));
    }

    [Fact]
    public void EdgeSyncState_LoadFromFile_ReturnsNewStateWhenFileDoesNotExist()
    {
        // Arrange
        var nonExistentFile = Path.Combine(Path.GetTempPath(), $"nonexistent-{Guid.NewGuid():N}.json");

        // Act
        var state = EdgeSyncState.LoadFromFile(nonExistentFile);

        // Assert
        Assert.NotNull(state);
        Assert.Equal("", state.EdgeId);
        Assert.Empty(state.SyncedOffsets);
        Assert.Equal(0, state.TotalMessagesSynced);
        Assert.False(state.IsOnline);
    }
}
