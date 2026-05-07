using System.Text;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect.Distributed;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Tests for the Surgewave Connect framework.
/// </summary>
[Collection("Broker")]
[Trait("Category", TestCategories.Integration)]
public sealed class ConnectTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public ConnectTests(BrokerFixture fixture, ITestOutputHelper output)
    {
        _ = fixture;
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddFilter(level => level >= LogLevel.Debug);
            builder.AddProvider(new XUnitLoggerProvider(output));
        });
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        _loggerFactory.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void ConfigDef_DefinesAndValidatesConfigs()
    {
        // Arrange
        var configDef = new ConfigDef()
            .Define("string.config", ConfigType.String, Importance.High, "A string config")
            .Define("int.config", ConfigType.Int, 42, Importance.Medium, "An int config")
            .Define("bool.config", ConfigType.Boolean, true, Importance.Low, "A bool config");

        // Assert
        Assert.Equal(3, configDef.Keys.Count);
        Assert.Equal("string.config", configDef.Keys[0].Name);
        Assert.Equal(ConfigType.String, configDef.Keys[0].Type);
        Assert.Equal(Importance.High, configDef.Keys[0].Importance);
        Assert.Null(configDef.Keys[0].DefaultValue);

        Assert.Equal("int.config", configDef.Keys[1].Name);
        Assert.Equal(ConfigType.Int, configDef.Keys[1].Type);
        Assert.Equal(42, configDef.Keys[1].DefaultValue);

        Assert.Equal("bool.config", configDef.Keys[2].Name);
        Assert.Equal(true, configDef.Keys[2].DefaultValue);
    }

    [Fact]
    public async Task ConnectWorker_CreatesAndStopsConnector()
    {
        // Arrange
        var config = new ConnectWorkerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = "test-connect-" + Guid.NewGuid().ToString("N")[..8]
        };
        var logger = _loggerFactory.CreateLogger<ConnectWorker>();
        await using var surgewaveClient = await SurgewaveClient.Create(BrokerFixture.BootstrapServers).UseSurgewaveProtocol().BuildAsync();

        await using var worker = new ConnectWorker(config, surgewaveClient, logger);
        await worker.StartAsync();

        // Create a temp file for the source connector
        var tempFile = Path.GetTempFileName();
        await File.WriteAllTextAsync(tempFile, "test line\n");

        try
        {
            // Topics are auto-created by the broker

            // Act - Create connector
            var connectorConfig = new Dictionary<string, string>
            {
                ["connector.class"] = typeof(TestSourceConnector).AssemblyQualifiedName!,
                ["file"] = tempFile,
                ["topic"] = "connect-test-topic"
            };

            var connectorName = await worker.CreateConnectorAsync(
                "test-file-source",
                typeof(TestSourceConnector).AssemblyQualifiedName!,
                connectorConfig);

            // Assert
            Assert.Equal("test-file-source", connectorName);

            var connectors = worker.ListConnectors();
            Assert.Contains("test-file-source", connectors);

            var status = worker.GetConnectorStatus("test-file-source");
            Assert.NotNull(status);
            Assert.Equal("test-file-source", status.Name);
            Assert.Equal("source", status.Type);
            Assert.Equal("Running", status.State);

            // Stop connector
            await worker.StopConnectorAsync("test-file-source");

            connectors = worker.ListConnectors();
            Assert.DoesNotContain("test-file-source", connectors);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task ConnectWorker_RejectsInvalidConnectorClass()
    {
        // Arrange
        var config = new ConnectWorkerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = "test-connect-" + Guid.NewGuid().ToString("N")[..8]
        };
        var logger = _loggerFactory.CreateLogger<ConnectWorker>();
        await using var surgewaveClient = await SurgewaveClient.Create(BrokerFixture.BootstrapServers).UseSurgewaveProtocol().BuildAsync();

        await using var worker = new ConnectWorker(config, surgewaveClient, logger);
        await worker.StartAsync();

        var connectorConfig = new Dictionary<string, string>
        {
            ["file"] = "test.txt",
            ["topic"] = "test-topic"
        };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() =>
            worker.CreateConnectorAsync("test", "NonExistent.Connector.Class", connectorConfig));
    }

    [Fact]
    public async Task ConnectWorker_RejectsDuplicateConnectorName()
    {
        // Arrange
        var config = new ConnectWorkerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = "test-connect-" + Guid.NewGuid().ToString("N")[..8]
        };
        var logger = _loggerFactory.CreateLogger<ConnectWorker>();
        await using var surgewaveClient = await SurgewaveClient.Create(BrokerFixture.BootstrapServers).UseSurgewaveProtocol().BuildAsync();

        await using var worker = new ConnectWorker(config, surgewaveClient, logger);
        await worker.StartAsync();

        var tempFile = Path.GetTempFileName();

        try
        {
            // Topics are auto-created by the broker

            var connectorConfig = new Dictionary<string, string>
            {
                ["connector.class"] = typeof(TestSourceConnector).AssemblyQualifiedName!,
                ["file"] = tempFile,
                ["topic"] = "connect-dup-test-topic"
            };

            await worker.CreateConnectorAsync(
                "duplicate-test",
                typeof(TestSourceConnector).AssemblyQualifiedName!,
                connectorConfig);

            // Act & Assert - Second creation should fail
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                worker.CreateConnectorAsync(
                    "duplicate-test",
                    typeof(TestSourceConnector).AssemblyQualifiedName!,
                    connectorConfig));
        }
        finally
        {
            await worker.StopConnectorAsync("duplicate-test");
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void SourceRecord_HasRequiredProperties()
    {
        // Arrange & Act
        var record = new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["key"] = "value" },
            SourceOffset = new Dictionary<string, object> { ["offset"] = 123L },
            Topic = "test-topic",
            Partition = 0,
            Key = Encoding.UTF8.GetBytes("key"),
            Value = Encoding.UTF8.GetBytes("value"),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]> { ["header"] = new byte[] { 1, 2, 3 } }
        };

        // Assert
        Assert.Equal("test-topic", record.Topic);
        Assert.Equal(0, record.Partition);
        Assert.Equal("key", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("value", Encoding.UTF8.GetString(record.Value));
        Assert.NotNull(record.Timestamp);
        Assert.Single(record.Headers!);
    }

    [Fact]
    public void SinkRecord_HasRequiredProperties()
    {
        // Arrange & Act
        var record = new SinkRecord
        {
            Topic = "test-topic",
            Partition = 2,
            Offset = 100,
            Key = Encoding.UTF8.GetBytes("key"),
            Value = Encoding.UTF8.GetBytes("value"),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]> { ["header"] = new byte[] { 1, 2, 3 } }
        };

        // Assert
        Assert.Equal("test-topic", record.Topic);
        Assert.Equal(2, record.Partition);
        Assert.Equal(100, record.Offset);
        Assert.Equal("key", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("value", Encoding.UTF8.GetString(record.Value));
    }

    // ========================
    // Distributed Coordination Tests
    // ========================

    [Fact]
    public void ConnectWorkerConfig_HasDistributedModeSettings()
    {
        // Arrange & Act
        var config = new ConnectWorkerConfig
        {
            DistributedMode = true,
            HeartbeatIntervalMs = 5000,
            SessionTimeoutMs = 60000,
            RebalanceDelayMs = 2000
        };

        // Assert
        Assert.True(config.DistributedMode);
        Assert.Equal(5000, config.HeartbeatIntervalMs);
        Assert.Equal(60000, config.SessionTimeoutMs);
        Assert.Equal(2000, config.RebalanceDelayMs);
    }

    [Fact]
    public void ConnectorTaskAssignment_HasRequiredProperties()
    {
        // Arrange & Act
        var assignment = new ConnectorTaskAssignment
        {
            ConnectorName = "my-connector",
            WorkerId = "worker-1",
            TaskIds = [0, 1, 2],
            Generation = 5
        };

        // Assert
        Assert.Equal("my-connector", assignment.ConnectorName);
        Assert.Equal("worker-1", assignment.WorkerId);
        Assert.Equal(3, assignment.TaskIds.Count);
        Assert.Equal(5, assignment.Generation);
        Assert.True(assignment.Timestamp > 0);
    }

    [Fact]
    public void ConnectorTaskStatus_TracksTaskState()
    {
        // Arrange & Act
        var status = new ConnectorTaskStatus
        {
            ConnectorName = "my-connector",
            TaskId = 2,
            WorkerId = "worker-1",
            State = TaskState.Running,
            LastActive = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        // Assert
        Assert.Equal("my-connector", status.ConnectorName);
        Assert.Equal(2, status.TaskId);
        Assert.Equal(TaskState.Running, status.State);
        Assert.Null(status.ErrorMessage);
    }

    [Fact]
    public void ConnectorTaskStatus_TracksFailedState()
    {
        // Arrange & Act
        var status = new ConnectorTaskStatus
        {
            ConnectorName = "my-connector",
            TaskId = 0,
            WorkerId = "worker-1",
            State = TaskState.Failed,
            ErrorMessage = "Connection refused",
            ErrorTrace = "at MyConnector.Connect()"
        };

        // Assert
        Assert.Equal(TaskState.Failed, status.State);
        Assert.Equal("Connection refused", status.ErrorMessage);
        Assert.NotNull(status.ErrorTrace);
    }

    [Fact]
    public void TaskState_HasAllExpectedValues()
    {
        // Assert - all states are defined
        Assert.Equal(0, (int)TaskState.Unassigned);
        Assert.Equal(1, (int)TaskState.Running);
        Assert.Equal(2, (int)TaskState.Paused);
        Assert.Equal(3, (int)TaskState.Failed);
        Assert.Equal(4, (int)TaskState.Restarting);
    }

    [Fact]
    public void WorkerInfo_TracksWorkerState()
    {
        // Arrange & Act
        var workerInfo = new WorkerInfo
        {
            WorkerId = "worker-test-123",
            LastHeartbeat = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            AssignedConnectors = ["connector-a", "connector-b"]
        };

        // Assert
        Assert.Equal("worker-test-123", workerInfo.WorkerId);
        Assert.True(workerInfo.LastHeartbeat > 0);
        Assert.Equal(2, workerInfo.AssignedConnectors.Count);
        Assert.Contains("connector-a", workerInfo.AssignedConnectors);
    }

    [Fact]
    public void WorkerHeartbeat_HasRequiredFields()
    {
        // Arrange & Act
        var heartbeat = new WorkerHeartbeat
        {
            WorkerId = "worker-abc",
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Generation = 3,
            AssignedConnectors = ["conn-1"],
            RestUrl = "http://localhost:8083"
        };

        // Assert
        Assert.Equal("worker-abc", heartbeat.WorkerId);
        Assert.Equal(3, heartbeat.Generation);
        Assert.Single(heartbeat.AssignedConnectors);
    }

    [Fact]
    public void TasksAssignedEventArgs_ContainsAssignment()
    {
        // Arrange
        var assignment = new ConnectorTaskAssignment
        {
            ConnectorName = "test-connector",
            WorkerId = "worker-1",
            TaskIds = [0, 1],
            Generation = 1
        };

        // Act
        var eventArgs = new TasksAssignedEventArgs(assignment);

        // Assert
        Assert.Equal("test-connector", eventArgs.Assignment.ConnectorName);
        Assert.Equal(2, eventArgs.Assignment.TaskIds.Count);
    }

    [Fact]
    public void TasksRevokedEventArgs_ContainsTaskIds()
    {
        // Arrange
        var taskIds = new[] { "connector-0", "connector-1", "connector-2" };

        // Act
        var eventArgs = new TasksRevokedEventArgs(taskIds);

        // Assert
        Assert.Equal(3, eventArgs.TaskIds.Count);
        Assert.Contains("connector-1", eventArgs.TaskIds);
    }

    [Fact]
    public async Task WorkerCoordinator_InitializesWithUniqueWorkerId()
    {
        // Arrange
        var config = new ConnectWorkerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = "test-coord-" + Guid.NewGuid().ToString("N")[..8],
            DistributedMode = true
        };
        var logger = _loggerFactory.CreateLogger<WorkerCoordinator>();

        // Act
        await using var coordinator = new WorkerCoordinator(config, "http://localhost:8083", logger);

        // Assert
        Assert.NotNull(coordinator.WorkerId);
        Assert.StartsWith("worker-", coordinator.WorkerId);
        Assert.False(coordinator.IsLeader); // Not leader until started
        Assert.Equal(0, coordinator.Generation);
    }
}

/// <summary>
/// Logger provider that writes to xunit test output.
/// </summary>
internal sealed class XUnitLoggerProvider : ILoggerProvider
{
    private readonly ITestOutputHelper _output;

    public XUnitLoggerProvider(ITestOutputHelper output) => _output = output;

    public ILogger CreateLogger(string categoryName) => new XUnitLogger(_output, categoryName);

    public void Dispose() { }
}

internal sealed class XUnitLogger : ILogger
{
    private readonly ITestOutputHelper _output;
    private readonly string _categoryName;

    public XUnitLogger(ITestOutputHelper output, string categoryName)
    {
        _output = output;
        _categoryName = categoryName;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        try
        {
            _output.WriteLine($"[{logLevel}] {_categoryName}: {formatter(state, exception)}");
            if (exception != null)
            {
                _output.WriteLine(exception.ToString());
            }
        }
        catch
        {
            // Ignore errors writing to output
        }
    }
}

/// <summary>
/// Minimal source connector for framework integration tests.
/// </summary>
internal sealed class TestSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(TestSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("file", ConfigType.String, Importance.High, "Input file")
        .Define("topic", ConfigType.String, Importance.High, "Output topic");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey("file"))
            throw new ArgumentException("Missing required config: file");
        foreach (var kvp in config) _config[kvp.Key] = kvp.Value;
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
        => [new Dictionary<string, string>(_config)];
}

/// <summary>
/// Minimal source task for framework integration tests.
/// </summary>
internal sealed class TestSourceTask : SourceTask
{
    public override string Version => "1.0.0";
    private string _topic = "";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config.TryGetValue("topic", out var t) ? t : "test";
    }

    public override void Stop() { }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        IReadOnlyList<SourceRecord> records =
        [
            new SourceRecord
            {
                Topic = _topic,
                SourcePartition = new Dictionary<string, object>(),
                SourceOffset = new Dictionary<string, object> { ["offset"] = 0L },
                Value = Encoding.UTF8.GetBytes("test-record")
            }
        ];
        return Task.FromResult(records);
    }
}
