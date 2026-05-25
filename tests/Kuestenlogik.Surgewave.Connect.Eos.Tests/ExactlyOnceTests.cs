using System.Text;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect.Eos;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Runtime;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Connect.Eos.Tests;

/// <summary>
/// Integration tests for Exactly-Once Semantics (EOS) in Surgewave Connect.
/// </summary>
public sealed class ExactlyOnceTests : IAsyncLifetime, IDisposable
{
    private bool _disposed;
    private readonly ITestOutputHelper _output;
    private SurgewaveRuntime? _surgewave;
    private SurgewaveNativeClient? _nativeClient;

    public ExactlyOnceTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public async ValueTask InitializeAsync()
    {
        _surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(3)
            .Build()
            .StartAsync();

        var (host, port) = ParseBootstrapServers(_surgewave.BootstrapServers);
        _nativeClient = new SurgewaveNativeClient(host, port);
        await _nativeClient.ConnectAsync();
        _output.WriteLine($"Broker started on {_surgewave.BootstrapServers}");
    }

    public async ValueTask DisposeAsync()
    {
        if (_nativeClient != null)
        {
            await _nativeClient.DisposeAsync();
        }
        if (_surgewave != null)
        {
            await _surgewave.DisposeAsync();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        DisposeAsync().GetAwaiter().GetResult();
    }

    private static (string host, int port) ParseBootstrapServers(string servers)
    {
        var parts = servers.Split(':');
        return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 9092);
    }

    [Fact]
    public async Task TransactionBuilder_InitCommit_Succeeds()
    {
        // Arrange
        Assert.NotNull(_nativeClient);
        var txnId = $"test-txn-{Guid.NewGuid():N}";

        // Act
        var txnBuilder = await _nativeClient.Transactions
            .BeginTransaction(txnId)
            .WithTimeout(TimeSpan.FromSeconds(30))
            .InitAsync();

        var commitResult = await txnBuilder.CommitAsync();

        // Assert
        Assert.Equal(SurgewaveErrorCode.None, commitResult);
        _output.WriteLine($"Transaction {txnId} committed successfully");
    }

    [Fact]
    public async Task TransactionBuilder_InitAbort_Succeeds()
    {
        // Arrange
        Assert.NotNull(_nativeClient);
        var txnId = $"test-txn-abort-{Guid.NewGuid():N}";

        // Act
        var txnBuilder = await _nativeClient.Transactions
            .BeginTransaction(txnId)
            .WithTimeout(TimeSpan.FromSeconds(30))
            .InitAsync();

        var abortResult = await txnBuilder.AbortAsync();

        // Assert
        Assert.Equal(SurgewaveErrorCode.None, abortResult);
        _output.WriteLine($"Transaction {txnId} aborted successfully");
    }

    [Fact]
    public async Task TransactionBuilder_AddPartitionsAndCommit_Succeeds()
    {
        // Arrange
        Assert.NotNull(_nativeClient);
        var txnId = $"test-txn-partitions-{Guid.NewGuid():N}";
        var topicName = $"eos-test-{Guid.NewGuid():N}";

        // Topic will be auto-created by broker with AutoCreateTopics=true

        // Act
        var txnBuilder = await _nativeClient.Transactions
            .BeginTransaction(txnId)
            .WithTimeout(TimeSpan.FromSeconds(30))
            .InitAsync();

        var addResult = await txnBuilder.AddPartitionsAsync(
            new Dictionary<string, List<int>>
            {
                [topicName] = [0, 1, 2]
            });

        // Assert - AddPartitions should succeed
        Assert.NotNull(addResult);
        Assert.True(addResult.ContainsKey(topicName));
        foreach (var partResult in addResult[topicName])
        {
            Assert.Equal(SurgewaveErrorCode.None, partResult.ErrorCode);
        }

        var commitResult = await txnBuilder.CommitAsync();
        Assert.Equal(SurgewaveErrorCode.None, commitResult);
        _output.WriteLine($"Transaction {txnId} with partitions committed successfully");
    }

    [Fact]
    public async Task TransactionBuilder_SendOffsetsToTransaction_Succeeds()
    {
        // Arrange
        Assert.NotNull(_nativeClient);
        var txnId = $"test-txn-offsets-{Guid.NewGuid():N}";
        var groupId = $"test-group-{Guid.NewGuid():N}";
        var topicName = $"eos-offset-test-{Guid.NewGuid():N}";

        // Topic will be auto-created by broker with AutoCreateTopics=true

        // Act
        var txnBuilder = await _nativeClient.Transactions
            .BeginTransaction(txnId)
            .WithTimeout(TimeSpan.FromSeconds(30))
            .InitAsync();

        var offsets = new Dictionary<Core.Models.TopicPartition, long>
        {
            [new() { Topic = topicName, Partition = 0 }] = 100,
            [new() { Topic = topicName, Partition = 1 }] = 200,
            [new() { Topic = topicName, Partition = 2 }] = 300
        };

        var offsetResult = await txnBuilder.SendOffsetsToTransactionAsync(groupId, offsets);

        // Assert
        Assert.NotNull(offsetResult);
        Assert.True(offsetResult.ContainsKey(topicName));
        foreach (var partResult in offsetResult[topicName])
        {
            Assert.Equal(SurgewaveErrorCode.None, partResult.ErrorCode);
        }

        var commitResult = await txnBuilder.CommitAsync();
        Assert.Equal(SurgewaveErrorCode.None, commitResult);
        _output.WriteLine($"Transaction {txnId} with offsets committed successfully");
    }

    [Fact]
    public async Task SinkOffsetCommitter_CommitOffsets_Succeeds()
    {
        // Arrange
        Assert.NotNull(_nativeClient);
        var txnId = $"test-sink-{Guid.NewGuid():N}";
        var groupId = $"test-sink-group-{Guid.NewGuid():N}";
        var topicName = $"eos-sink-test-{Guid.NewGuid():N}";

        // Topic will be auto-created by broker with AutoCreateTopics=true

        var loggerFactory = LoggerFactory.Create(builder => builder.AddXunit(_output));
        var logger = loggerFactory.CreateLogger<SinkOffsetCommitter>();

        var offsetCommitter = new SinkOffsetCommitter(_nativeClient, groupId, logger);

        // Create sink records to simulate processing
        var sinkRecords = new List<SinkRecord>
        {
            new() { Topic = topicName, Partition = 0, Offset = 50, Key = null, Value = [] },
            new() { Topic = topicName, Partition = 0, Offset = 51, Key = null, Value = [] },
            new() { Topic = topicName, Partition = 1, Offset = 100, Key = null, Value = [] }
        };

        // Act
        var txnBuilder = await _nativeClient.Transactions
            .BeginTransaction(txnId)
            .WithTimeout(TimeSpan.FromSeconds(30))
            .InitAsync();

        await offsetCommitter.CommitAsync(txnBuilder, sinkRecords);

        var commitResult = await txnBuilder.CommitAsync();

        // Assert
        Assert.Equal(SurgewaveErrorCode.None, commitResult);
        _output.WriteLine($"SinkOffsetCommitter committed offsets successfully");
    }

    [Fact]
    public void ITaskRunner_Interface_ExistsAndHasExpectedMembers()
    {
        // Verify ITaskRunner interface has expected members
        var interfaceType = typeof(ITaskRunner);

        Assert.True(interfaceType.IsInterface);
        Assert.NotNull(interfaceType.GetProperty("TaskId"));
        Assert.NotNull(interfaceType.GetProperty("State"));
        Assert.NotNull(interfaceType.GetMethod("StartAsync"));
        Assert.NotNull(interfaceType.GetMethod("StopAsync"));
        Assert.NotNull(interfaceType.GetMethod("PauseAsync"));
        Assert.NotNull(interfaceType.GetMethod("ResumeAsync"));
    }

    [Fact]
    public void TaskRunnerState_HasExpectedValues()
    {
        // Verify TaskRunnerState enum has expected values
        Assert.True(Enum.IsDefined(TaskRunnerState.Unassigned));
        Assert.True(Enum.IsDefined(TaskRunnerState.Running));
        Assert.True(Enum.IsDefined(TaskRunnerState.Paused));
        Assert.True(Enum.IsDefined(TaskRunnerState.Failed));
    }

    [Fact]
    public void ConnectWorkerConfig_HasEosSettings()
    {
        // Verify ConnectWorkerConfig has EOS settings
        var config = new ConnectWorkerConfig();

        Assert.False(config.ExactlyOnceSupport); // Default should be false
        Assert.Equal("connect", config.TransactionIdPrefix);
        Assert.Equal(60000, config.TransactionTimeoutMs);
    }

    [Fact]
    public async Task ConnectWorker_WithEosEnabled_CreatesConnector()
    {
        // Arrange
        Assert.NotNull(_surgewave);
        var loggerFactory = LoggerFactory.Create(builder => builder.AddXunit(_output));
        var workerConfig = new ConnectWorkerConfig
        {
            BootstrapServers = _surgewave.BootstrapServers,
            GroupId = $"eos-test-group-{Guid.NewGuid():N}",
            ExactlyOnceSupport = true,
            TransactionIdPrefix = "eos-test"
        };

        // Create Surgewave client for EOS support
        await using var surgewaveClient = await SurgewaveClient.Create(_surgewave.BootstrapServers)
            .UseSurgewaveProtocol()
            .BuildAsync();

        await using var connectWorker = new ConnectWorker(workerConfig, surgewaveClient, loggerFactory.CreateLogger<ConnectWorker>());
        await connectWorker.StartAsync();

        var topicName = $"eos-generator-test-{Guid.NewGuid():N}";

        // Act - Create generator source connector with EOS enabled
        var connectorConfig = new Dictionary<string, string>
        {
            ["generator.topic"] = topicName,
            ["generator.message.count"] = "5",
            ["generator.interval.ms"] = "100",
            ["generator.batch.size"] = "1",
            ["tasks.max"] = "1",
            ["exactly.once"] = "true" // Per-connector EOS override
        };

        await connectWorker.CreateConnectorAsync(
            "eos-generator",
            typeof(EosTestSourceConnector).AssemblyQualifiedName!,
            connectorConfig);

        // Give it time to produce messages
        await Task.Delay(1000);

        // Assert - Connector should be running
        var status = connectWorker.GetConnectorStatus("eos-generator");
        Assert.NotNull(status);
        Assert.Equal("Running", status.State, ignoreCase: true);
        _output.WriteLine($"EOS connector status: {status.State}");

        // Cleanup
        await connectWorker.StopConnectorAsync("eos-generator");
        await connectWorker.StopAsync();
    }

    [Fact]
    public async Task ConnectWorker_EosSource_ProducesTransactionalMessages()
    {
        // Arrange
        Assert.NotNull(_surgewave);
        Assert.NotNull(_nativeClient);
        var loggerFactory = LoggerFactory.Create(builder => builder.AddXunit(_output));

        var topicName = $"eos-txn-test-{Guid.NewGuid():N}";

        var workerConfig = new ConnectWorkerConfig
        {
            BootstrapServers = _surgewave.BootstrapServers,
            GroupId = $"eos-txn-group-{Guid.NewGuid():N}",
            ExactlyOnceSupport = true,
            TransactionIdPrefix = "eos-txn-test"
        };

        // Create Surgewave client for EOS support
        await using var surgewaveClient = await SurgewaveClient.Create(_surgewave.BootstrapServers)
            .UseSurgewaveProtocol()
            .BuildAsync();

        await using var connectWorker = new ConnectWorker(workerConfig, surgewaveClient, loggerFactory.CreateLogger<ConnectWorker>());
        await connectWorker.StartAsync();

        // Act - Create generator source connector with EOS
        var connectorConfig = new Dictionary<string, string>
        {
            ["generator.topic"] = topicName,
            ["generator.message.count"] = "10",
            ["generator.interval.ms"] = "50",
            ["generator.batch.size"] = "2",
            ["tasks.max"] = "1"
        };

        await connectWorker.CreateConnectorAsync(
            "eos-txn-generator",
            typeof(EosTestSourceConnector).AssemblyQualifiedName!,
            connectorConfig);

        // Wait for messages to be produced
        await Task.Delay(2000);

        // Consume with READ_COMMITTED to verify transactional messages
        await using var consumer = surgewaveClient.CreateConsumer<string?, byte[]>(opts =>
        {
            opts.GroupId = $"eos-consumer-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.IsolationLevel = IsolationLevel.ReadCommitted;
        });

        consumer.Subscribe([topicName]);

        var messages = new List<ConsumeResult<string?, byte[]>>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var result = await consumer.ConsumeAsync(TimeSpan.FromMilliseconds(500), cts.Token);
                if (result != null)
                {
                    messages.Add(result);
                    _output.WriteLine($"Received message: offset={result.Offset}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Assert - Should have received messages (READ_COMMITTED sees committed txn messages)
        _output.WriteLine($"Total messages received with READ_COMMITTED: {messages.Count}");
        Assert.True(messages.Count > 0, "Expected to receive transactional messages");

        // Cleanup
        await connectWorker.StopConnectorAsync("eos-txn-generator");
        await connectWorker.StopAsync();
    }

    [Fact]
    public void TransactionalSourceTaskRunner_ImplementsITaskRunner()
    {
        // Verify TransactionalSourceTaskRunner implements ITaskRunner
        var runnerType = typeof(TransactionalSourceTaskRunner);

        Assert.True(typeof(ITaskRunner).IsAssignableFrom(runnerType));
        Assert.NotNull(runnerType.GetProperty("TaskId"));
        Assert.NotNull(runnerType.GetProperty("State"));
    }

    [Fact]
    public void TransactionalSinkTaskRunner_ImplementsITaskRunner()
    {
        // Verify TransactionalSinkTaskRunner implements ITaskRunner
        var runnerType = typeof(TransactionalSinkTaskRunner);

        Assert.True(typeof(ITaskRunner).IsAssignableFrom(runnerType));
        Assert.NotNull(runnerType.GetProperty("TaskId"));
        Assert.NotNull(runnerType.GetProperty("State"));
    }
}

/// <summary>
/// xUnit logger adapter for ILogger.
/// </summary>
internal static class XunitLoggerExtensions
{
    public static ILoggingBuilder AddXunit(this ILoggingBuilder builder, ITestOutputHelper output)
    {
        builder.AddProvider(new XunitLoggerProvider(output));
        return builder;
    }
}

internal sealed class XunitLoggerProvider : ILoggerProvider
{
    private readonly ITestOutputHelper _output;

    public XunitLoggerProvider(ITestOutputHelper output) => _output = output;

    public ILogger CreateLogger(string categoryName) => new XunitLogger(_output, categoryName);

    public void Dispose() { }
}

internal sealed class XunitLogger : ILogger
{
    private readonly ITestOutputHelper _output;
    private readonly string _categoryName;

    public XunitLogger(ITestOutputHelper output, string categoryName)
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
            // Ignore output errors (e.g., if test has already completed)
        }
    }
}

/// <summary>
/// Minimal source connector for EOS integration tests.
/// Accepts the same config keys as Generator connector.
/// </summary>
internal sealed class EosTestSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(EosTestSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("generator.topic", ConfigType.String, Importance.High, "Output topic")
        .Define("generator.message.count", ConfigType.Int, 100, Importance.Medium, "Messages to generate")
        .Define("generator.interval.ms", ConfigType.Int, 100, Importance.Low, "Interval between messages")
        .Define("generator.batch.size", ConfigType.Int, 1, Importance.Low, "Batch size");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        foreach (var kvp in config) _config[kvp.Key] = kvp.Value;
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
        => [new Dictionary<string, string>(_config)];
}

internal sealed class EosTestSourceTask : SourceTask
{
    public override string Version => "1.0.0";
    private string _topic = "";
    private int _count;
    private int _intervalMs = 100;
    private int _produced;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config.TryGetValue("generator.topic", out var t) ? t : "test";
        _count = config.TryGetValue("generator.message.count", out var c) && int.TryParse(c, out var cv) ? cv : 100;
        _intervalMs = config.TryGetValue("generator.interval.ms", out var i) && int.TryParse(i, out var iv) ? iv : 100;
    }

    public override void Stop() { }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_produced >= _count)
        {
            await Task.Delay(_intervalMs, cancellationToken);
            return [];
        }

        await Task.Delay(_intervalMs, cancellationToken);
        _produced++;

        return
        [
            new SourceRecord
            {
                Topic = _topic,
                SourcePartition = new Dictionary<string, object>(),
                SourceOffset = new Dictionary<string, object> { ["offset"] = (long)_produced },
                Value = Encoding.UTF8.GetBytes($"eos-test-message-{_produced}")
            }
        ];
    }
}
