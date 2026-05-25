using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Connect.Eos;

namespace Kuestenlogik.Surgewave.Connect.Tests.ExactlyOnce;

/// <summary>
/// Tests for exactly-once source connector infrastructure.
/// </summary>
public sealed class ExactlyOnceTests
{
    [Fact]
    public void ExactlyOnceSourceRecord_Properties()
    {
        var record = new ExactlyOnceSourceRecord
        {
            SourcePartition = "table-orders",
            SourceOffset = new Dictionary<string, string> { ["lsn"] = "42" },
            Topic = "orders-topic",
            Partition = 3,
            Key = [1, 2, 3],
            Value = [4, 5, 6],
            Timestamp = new DateTimeOffset(2026, 3, 18, 12, 0, 0, TimeSpan.Zero),
            Headers = new Dictionary<string, byte[]> { ["source"] = [7, 8] }
        };

        Assert.Equal("table-orders", record.SourcePartition);
        Assert.Equal("42", record.SourceOffset["lsn"]);
        Assert.Equal("orders-topic", record.Topic);
        Assert.Equal(3, record.Partition);
        Assert.Equal([1, 2, 3], record.Key);
        Assert.Equal([4, 5, 6], record.Value);
        Assert.NotNull(record.Timestamp);
        Assert.NotNull(record.Headers);
        Assert.Equal([7, 8], record.Headers["source"]);
    }

    [Fact]
    public void ExactlyOnceSourceRecord_PartitionIsNullable()
    {
        var record = new ExactlyOnceSourceRecord
        {
            SourcePartition = "file",
            SourceOffset = new Dictionary<string, string> { ["pos"] = "0" },
            Topic = "output",
            Value = [1]
        };

        Assert.Null(record.Partition);
        Assert.Null(record.Key);
        Assert.Null(record.Timestamp);
        Assert.Null(record.Headers);
    }

    [Fact]
    public void ExactlyOnceConfig_Defaults()
    {
        var config = new ExactlyOnceConfig();

        Assert.True(config.Enabled);
        Assert.Equal("__connect_offsets", config.OffsetTopic);
        Assert.Equal(1000, config.MaxBatchSize);
        Assert.Equal(TimeSpan.FromSeconds(60), config.TransactionTimeout);
        Assert.Equal(TimeSpan.FromSeconds(1), config.PollInterval);
    }

    [Fact]
    public void ExactlyOnceConfig_CustomValues()
    {
        var config = new ExactlyOnceConfig
        {
            Enabled = false,
            OffsetTopic = "custom-offsets",
            MaxBatchSize = 500,
            TransactionTimeout = TimeSpan.FromSeconds(30),
            PollInterval = TimeSpan.FromMilliseconds(500)
        };

        Assert.False(config.Enabled);
        Assert.Equal("custom-offsets", config.OffsetTopic);
        Assert.Equal(500, config.MaxBatchSize);
        Assert.Equal(TimeSpan.FromSeconds(30), config.TransactionTimeout);
        Assert.Equal(TimeSpan.FromMilliseconds(500), config.PollInterval);
    }

    [Fact]
    public async Task InMemoryOffsetStore_SetAndGet()
    {
        var store = new InMemorySourceOffsetStore();

        await store.CommitOffsetAsync("my-connector", "partition-0",
            new Dictionary<string, string> { ["offset"] = "100" });

        var offset = await store.GetOffsetAsync("my-connector", "partition-0");

        Assert.NotNull(offset);
        Assert.Equal("100", offset["offset"]);
    }

    [Fact]
    public async Task InMemoryOffsetStore_GetReturnsNullForUnknown()
    {
        var store = new InMemorySourceOffsetStore();

        var offset = await store.GetOffsetAsync("unknown", "partition-0");

        Assert.Null(offset);
    }

    [Fact]
    public async Task InMemoryOffsetStore_GetAllOffsets()
    {
        var store = new InMemorySourceOffsetStore();

        await store.CommitOffsetAsync("connector-a", "part-0",
            new Dictionary<string, string> { ["pos"] = "10" });
        await store.CommitOffsetAsync("connector-a", "part-1",
            new Dictionary<string, string> { ["pos"] = "20" });
        await store.CommitOffsetAsync("connector-b", "part-0",
            new Dictionary<string, string> { ["pos"] = "30" });

        var allOffsets = await store.GetAllOffsetsAsync("connector-a");

        Assert.Equal(2, allOffsets.Count);
        Assert.True(allOffsets.ContainsKey("part-0"));
        Assert.True(allOffsets.ContainsKey("part-1"));
        Assert.Equal("10", allOffsets["part-0"]["pos"]);
        Assert.Equal("20", allOffsets["part-1"]["pos"]);
    }

    [Fact]
    public async Task InMemoryOffsetStore_DeleteOffsets()
    {
        var store = new InMemorySourceOffsetStore();

        await store.CommitOffsetAsync("connector-a", "part-0",
            new Dictionary<string, string> { ["pos"] = "10" });
        await store.CommitOffsetAsync("connector-a", "part-1",
            new Dictionary<string, string> { ["pos"] = "20" });

        await store.DeleteOffsetsAsync("connector-a");

        var allOffsets = await store.GetAllOffsetsAsync("connector-a");
        Assert.Empty(allOffsets);

        var offset = await store.GetOffsetAsync("connector-a", "part-0");
        Assert.Null(offset);
    }

    [Fact]
    public async Task InMemoryOffsetStore_OverwritesExistingOffset()
    {
        var store = new InMemorySourceOffsetStore();

        await store.CommitOffsetAsync("conn", "p0",
            new Dictionary<string, string> { ["offset"] = "1" });
        await store.CommitOffsetAsync("conn", "p0",
            new Dictionary<string, string> { ["offset"] = "2" });

        var offset = await store.GetOffsetAsync("conn", "p0");

        Assert.NotNull(offset);
        Assert.Equal("2", offset["offset"]);
    }

    [Fact]
    public async Task ExactlyOnceSourceTask_CallsPollWithOffset()
    {
        var task = new TestExactlyOnceSourceTask();
        var store = new InMemorySourceOffsetStore();

        // Seed an offset
        await store.CommitOffsetAsync("test-connector", "default",
            new Dictionary<string, string> { ["cursor"] = "42" });

        task.OffsetStore = store;
        task.ConnectorName = "test-connector";
        task.SourcePartition = "default";

        // PollAsync should delegate to PollWithOffsetAsync with the stored offset
        var records = await task.PollAsync(CancellationToken.None);

        Assert.NotNull(task.LastReceivedOffset);
        Assert.Equal("42", task.LastReceivedOffset["cursor"]);
        Assert.Single(records);
    }

    [Fact]
    public async Task ExactlyOnceSourceTask_NoOffsetPassesNull()
    {
        var task = new TestExactlyOnceSourceTask();
        var store = new InMemorySourceOffsetStore();

        task.OffsetStore = store;
        task.ConnectorName = "new-connector";
        task.SourcePartition = "default";

        await task.PollAsync(CancellationToken.None);

        Assert.True(task.PollWasCalled);
        Assert.Null(task.LastReceivedOffset);
    }

    [Fact]
    public void ExactlyOnceSourceConnector_Defaults()
    {
        var connector = new TestExactlyOnceSourceConnector();

        Assert.True(connector.ExactlyOnceEnabled);
        Assert.NotNull(connector.ExactlyOnceConfig);
        Assert.Equal("__connect_offsets", connector.ExactlyOnceConfig.OffsetTopic);
    }

    [Fact]
    public async Task BackwardCompatible_StandardSourceStillWorks()
    {
        // Standard SourceTask should work normally without EOS
        var task = new TestStandardSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);
        task.Start(new Dictionary<string, string>());

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Single(records);
        Assert.Equal("standard-topic", records[0].Topic);
    }

    // ---- In-memory offset store for testing (no broker required) ----

    private sealed class InMemorySourceOffsetStore : ISourceOffsetStore
    {
        private readonly ConcurrentDictionary<string, Dictionary<string, string>> _offsets = new();

        public Task<Dictionary<string, string>?> GetOffsetAsync(
            string connectorName, string sourcePartition, CancellationToken ct = default)
        {
            var key = $"{connectorName}:{sourcePartition}";
            var result = _offsets.TryGetValue(key, out var offset) ? offset : null;
            return Task.FromResult(result);
        }

        public Task CommitOffsetAsync(
            string connectorName, string sourcePartition,
            Dictionary<string, string> offset, CancellationToken ct = default)
        {
            var key = $"{connectorName}:{sourcePartition}";
            _offsets[key] = new Dictionary<string, string>(offset);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyDictionary<string, Dictionary<string, string>>> GetAllOffsetsAsync(
            string connectorName, CancellationToken ct = default)
        {
            var prefix = $"{connectorName}:";
            var result = new Dictionary<string, Dictionary<string, string>>();
            foreach (var (key, value) in _offsets)
            {
                if (key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    result[key[prefix.Length..]] = new Dictionary<string, string>(value);
                }
            }
            return Task.FromResult<IReadOnlyDictionary<string, Dictionary<string, string>>>(result);
        }

        public Task DeleteOffsetsAsync(string connectorName, CancellationToken ct = default)
        {
            var prefix = $"{connectorName}:";
            foreach (var key in _offsets.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)).ToList())
            {
                _offsets.TryRemove(key, out _);
            }
            return Task.CompletedTask;
        }
    }

    // ---- Test fixtures ----

    private sealed class TestExactlyOnceSourceTask : ExactlyOnceSourceTask
    {
        public override string Version => "1.0.0";
        public Dictionary<string, string>? LastReceivedOffset { get; private set; }
        public bool PollWasCalled { get; private set; }

        public override void Start(IDictionary<string, string> config) { }
        public override void Stop() { }

        public override Task<IReadOnlyList<ExactlyOnceSourceRecord>> PollWithOffsetAsync(
            Dictionary<string, string>? lastOffset, CancellationToken ct)
        {
            PollWasCalled = true;
            LastReceivedOffset = lastOffset;

            var records = new List<ExactlyOnceSourceRecord>
            {
                new()
                {
                    SourcePartition = "default",
                    SourceOffset = new Dictionary<string, string> { ["cursor"] = "43" },
                    Topic = "output-topic",
                    Value = [1, 2, 3]
                }
            };

            return Task.FromResult<IReadOnlyList<ExactlyOnceSourceRecord>>(records);
        }
    }

    private sealed class TestExactlyOnceSourceConnector : ExactlyOnceSourceConnector
    {
        public override string Version => "1.0.0";
        public override Type TaskClass => typeof(TestExactlyOnceSourceTask);

        public override Kuestenlogik.Surgewave.Plugins.Configuration.ConfigDef Config => new();

        public override void Start(IDictionary<string, string> config) { }
        public override void Stop() { }

        public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
        {
            return [new Dictionary<string, string>()];
        }
    }

    private sealed class TestStandardSourceTask : SourceTask
    {
        public override string Version => "1.0.0";

        public override void Start(IDictionary<string, string> config) { }
        public override void Stop() { }

        public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
        {
            var records = new List<SourceRecord>
            {
                new()
                {
                    SourcePartition = new Dictionary<string, object> { ["file"] = "test.txt" },
                    SourceOffset = new Dictionary<string, object> { ["pos"] = 0 },
                    Topic = "standard-topic",
                    Value = [10, 20]
                }
            };
            return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
        }
    }
}
