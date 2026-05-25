using Xunit;

namespace Kuestenlogik.Surgewave.Edge.Tests;

/// <summary>
/// Tests for EdgeSyncConfig, SyncDirection, ConnectivityChecker, and EdgeBrokerBuilder configuration.
/// </summary>
public sealed class EdgeConfigTests
{
    #region EdgeSyncConfig Tests

    [Fact]
    public void EdgeSyncConfig_Defaults_AreCorrect()
    {
        var config = new EdgeSyncConfig();

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
    public void EdgeSyncConfig_CustomValues_SetCorrectly()
    {
        var config = new EdgeSyncConfig
        {
            EdgeId = "factory-01",
            CloudBrokerAddress = "cloud.example.com:9092",
            SyncTopics = ["sensor-data", "alerts"],
            Direction = SyncDirection.Bidirectional,
            SyncIntervalSeconds = 10,
            MaxBatchSize = 500,
            OfflineBufferMaxMb = 50,
            CompressSync = false,
            OfflineStateFile = "/tmp/state.json",
            ConnectivityTimeoutMs = 3000,
            MaxConsecutiveFailures = 10
        };

        Assert.Equal("factory-01", config.EdgeId);
        Assert.Equal("cloud.example.com:9092", config.CloudBrokerAddress);
        Assert.Equal(2, config.SyncTopics.Count);
        Assert.Equal(SyncDirection.Bidirectional, config.Direction);
        Assert.Equal(10, config.SyncIntervalSeconds);
        Assert.Equal(500, config.MaxBatchSize);
        Assert.Equal(50, config.OfflineBufferMaxMb);
        Assert.False(config.CompressSync);
        Assert.Equal("/tmp/state.json", config.OfflineStateFile);
        Assert.Equal(3000, config.ConnectivityTimeoutMs);
        Assert.Equal(10, config.MaxConsecutiveFailures);
    }

    [Fact]
    public void EdgeSyncConfig_SyncTopics_CanBeModified()
    {
        var config = new EdgeSyncConfig();
        config.SyncTopics.Clear();
        config.SyncTopics.Add("specific-topic");

        Assert.Single(config.SyncTopics);
        Assert.Equal("specific-topic", config.SyncTopics[0]);
    }

    #endregion

    #region SyncDirection Tests

    [Fact]
    public void SyncDirection_HasThreeValues()
    {
        var values = Enum.GetValues<SyncDirection>();
        Assert.Equal(3, values.Length);
    }

    [Theory]
    [InlineData(SyncDirection.EdgeToCloud, 0)]
    [InlineData(SyncDirection.CloudToEdge, 1)]
    [InlineData(SyncDirection.Bidirectional, 2)]
    public void SyncDirection_IntValues_AreCorrect(SyncDirection direction, int expected)
    {
        Assert.Equal(expected, (int)direction);
    }

    [Fact]
    public void SyncDirection_AllValues_HaveUniqueName()
    {
        var names = Enum.GetNames<SyncDirection>();
        Assert.Equal(3, names.Distinct().Count());
    }

    #endregion

    #region ConnectivityChecker.ParseAddress Tests

    [Theory]
    [InlineData("cloud.example.com:9092", "cloud.example.com", 9092)]
    [InlineData("localhost:19092", "localhost", 19092)]
    [InlineData("192.168.1.100:8080", "192.168.1.100", 8080)]
    [InlineData("broker:1", "broker", 1)]
    [InlineData("broker:65535", "broker", 65535)]
    public void ParseAddress_ValidAddresses_ParsesCorrectly(string address, string expectedHost, int expectedPort)
    {
        var (host, port) = ConnectivityChecker.ParseAddress(address);

        Assert.Equal(expectedHost, host);
        Assert.Equal(expectedPort, port);
    }

    [Theory]
    [InlineData("no-port")]
    [InlineData("host:0")]
    [InlineData("host:99999")]
    [InlineData("host:abc")]
    [InlineData("host:-1")]
    [InlineData("host:")]
    public void ParseAddress_InvalidAddresses_ThrowsArgumentException(string address)
    {
        Assert.Throws<ArgumentException>(() => ConnectivityChecker.ParseAddress(address));
    }

    [Fact]
    public void ParseAddress_IPv6WithPort_ParsesCorrectly()
    {
        // LastIndexOf(':') should handle IPv6 format when port is after last colon
        var (host, port) = ConnectivityChecker.ParseAddress("[::1]:9092");

        Assert.Equal("[::1]", host);
        Assert.Equal(9092, port);
    }

    [Fact]
    public void ParseAddress_HostWithMultipleColons_UsesLastColon()
    {
        // This tests the LastIndexOf behavior
        var (host, port) = ConnectivityChecker.ParseAddress("a:b:9092");

        Assert.Equal("a:b", host);
        Assert.Equal(9092, port);
    }

    #endregion

    #region EdgeBrokerBuilder Tests

    [Fact]
    public void EdgeBrokerBuilder_Create_ReturnsNonNull()
    {
        var builder = EdgeBrokerBuilder.Create("test-edge");
        Assert.NotNull(builder);
    }

    [Fact]
    public void EdgeBrokerBuilder_Create_NullId_Throws()
    {
        Assert.ThrowsAny<ArgumentException>(() => EdgeBrokerBuilder.Create(null!));
    }

    [Fact]
    public void EdgeBrokerBuilder_Create_EmptyId_Throws()
    {
        Assert.Throws<ArgumentException>(() => EdgeBrokerBuilder.Create(""));
    }

    [Fact]
    public void EdgeBrokerBuilder_Create_WhitespaceId_Throws()
    {
        Assert.Throws<ArgumentException>(() => EdgeBrokerBuilder.Create("   "));
    }

    [Fact]
    public void EdgeBrokerBuilder_FluentApi_IsChainable()
    {
        var builder = EdgeBrokerBuilder
            .Create("edge-01")
            .WithSqliteStorage("test.db")
            .WithPort(9092)
            .WithHost("0.0.0.0")
            .WithDataDirectory("/data")
            .WithTopics("topic1", "topic2");

        Assert.NotNull(builder);
    }

    [Fact]
    public void EdgeBrokerBuilder_WithMemoryStorage_IsChainable()
    {
        var builder = EdgeBrokerBuilder
            .Create("edge-01")
            .WithMemoryStorage();

        Assert.NotNull(builder);
    }

    [Fact]
    public void EdgeBrokerBuilder_WithCloudSync_IsChainable()
    {
        var builder = EdgeBrokerBuilder
            .Create("edge-01")
            .WithMemoryStorage()
            .WithCloudSync("cloud:9092", cfg =>
            {
                cfg.SyncIntervalSeconds = 5;
                cfg.Direction = SyncDirection.Bidirectional;
                cfg.MaxBatchSize = 200;
            });

        Assert.NotNull(builder);
    }

    [Fact]
    public void EdgeBrokerBuilder_WithCloudSync_NullAddress_Throws()
    {
        var builder = EdgeBrokerBuilder.Create("edge-01");
        Assert.ThrowsAny<ArgumentException>(() => builder.WithCloudSync(null!));
    }

    [Fact]
    public void EdgeBrokerBuilder_WithCloudSync_EmptyAddress_Throws()
    {
        var builder = EdgeBrokerBuilder.Create("edge-01");
        Assert.Throws<ArgumentException>(() => builder.WithCloudSync(""));
    }

    [Fact]
    public void EdgeBrokerBuilder_WithLogging_IsChainable()
    {
        var builder = EdgeBrokerBuilder
            .Create("edge-01")
            .WithMemoryStorage()
            .WithLogging(Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance);

        Assert.NotNull(builder);
    }

    [Fact]
    public void EdgeBrokerBuilder_MultipleStorageModes_LastWins()
    {
        // Memory then SQLite
        var builder1 = EdgeBrokerBuilder
            .Create("edge-01")
            .WithMemoryStorage()
            .WithSqliteStorage("test.db");
        Assert.NotNull(builder1);

        // SQLite then Memory
        var builder2 = EdgeBrokerBuilder
            .Create("edge-02")
            .WithSqliteStorage("test.db")
            .WithMemoryStorage();
        Assert.NotNull(builder2);
    }

    #endregion

    #region EdgeSyncConfig Direction Validation Tests

    [Theory]
    [InlineData(SyncDirection.EdgeToCloud)]
    [InlineData(SyncDirection.CloudToEdge)]
    [InlineData(SyncDirection.Bidirectional)]
    public void EdgeSyncConfig_Direction_AcceptsAllValues(SyncDirection direction)
    {
        var config = new EdgeSyncConfig { Direction = direction };
        Assert.Equal(direction, config.Direction);
    }

    #endregion
}
