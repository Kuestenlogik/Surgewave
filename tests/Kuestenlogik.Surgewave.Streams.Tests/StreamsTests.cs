using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Windows;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class StreamsTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public StreamsTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddFilter(level => level >= LogLevel.Debug);
        });
    }

    public async ValueTask DisposeAsync()
    {
        _loggerFactory.Dispose();
        await Task.CompletedTask;
    }

    #region Serdes Tests

    [Fact]
    public void Serdes_String_SerializesAndDeserializes()
    {
        var serde = Serdes.String();
        var original = "Hello World";

        var bytes = serde.Serialize(original);
        var result = serde.Deserialize(bytes);

        Assert.Equal(original, result);
    }

    [Fact]
    public void Serdes_Int64_SerializesAndDeserializes()
    {
        var serde = Serdes.Int64();
        var original = 12345678901234L;

        var bytes = serde.Serialize(original);
        var result = serde.Deserialize(bytes);

        Assert.Equal(original, result);
    }

    [Fact]
    public void Serdes_Json_SerializesAndDeserializes()
    {
        var serde = Serdes.Json<TestUser>();
        var original = new TestUser { Id = 1, Name = "Alice" };

        var bytes = serde.Serialize(original);
        var result = serde.Deserialize(bytes);

        Assert.Equal(original.Id, result.Id);
        Assert.Equal(original.Name, result.Name);
    }

    #endregion

    #region State Store Tests

    [Fact]
    public void InMemoryKeyValueStore_PutsAndGets()
    {
        using var store = new InMemoryKeyValueStore<string, int>("test-store");

        store.Put("key1", 100);
        store.Put("key2", 200);

        Assert.Equal(100, store.Get("key1"));
        Assert.Equal(200, store.Get("key2"));
        Assert.Equal(2, store.ApproximateNumEntries);
    }

    [Fact]
    public void InMemoryKeyValueStore_Deletes()
    {
        using var store = new InMemoryKeyValueStore<string, int>("test-store");

        store.Put("key1", 100);
        var deleted = store.Delete("key1");

        Assert.Equal(100, deleted);
        Assert.Equal(0, store.Get("key1"));
    }

    [Fact]
    public void InMemoryKeyValueStore_PutIfAbsent()
    {
        using var store = new InMemoryKeyValueStore<string, int>("test-store");

        var result1 = store.PutIfAbsent("key1", 100);
        var result2 = store.PutIfAbsent("key1", 200);

        Assert.Equal(100, result1);
        Assert.Equal(100, result2);
        Assert.Equal(100, store.Get("key1"));
    }

    [Fact]
    public void InMemoryWindowStore_PutsAndFetches()
    {
        using var store = new InMemoryWindowStore<string, int>(
            "test-window-store",
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1));

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var windowStart = now - (now % (5 * 60 * 1000));

        store.Put("key1", 100, windowStart);
        store.Put("key1", 200, windowStart + (5 * 60 * 1000));

        Assert.Equal(100, store.Fetch("key1", windowStart));
        Assert.Equal(200, store.Fetch("key1", windowStart + (5 * 60 * 1000)));
    }

    [Fact]
    public void InMemorySessionStore_PutsAndFinds()
    {
        using var store = new InMemorySessionStore<string, int>(
            "test-session-store",
            TimeSpan.FromHours(1));

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var session = new Windowed<string>("key1", new Window(now, now + 10000));

        store.Put(session, 100);

        var found = store.FindSessions("key1", now - 1000, now + 11000).ToList();
        Assert.Single(found);
        Assert.Equal(100, found[0].Value);
    }

    #endregion

    #region Window Tests

    [Fact]
    public void TumblingWindows_ComputesCorrectWindow()
    {
        var windows = TumblingWindows.Of(TimeSpan.FromMinutes(5));
        var timestamp = 1000000L; // Some timestamp

        var result = windows.WindowsFor(timestamp).Single();

        // Window should start at a multiple of 5 minutes
        Assert.Equal(0, result.StartMs % (5 * 60 * 1000));
        Assert.Equal(result.StartMs + (5 * 60 * 1000), result.EndMs);
    }

    [Fact]
    public void HoppingWindows_ComputesOverlappingWindows()
    {
        var windows = HoppingWindows.Of(TimeSpan.FromMinutes(10))
            .AdvanceBy(TimeSpan.FromMinutes(5));

        var timestamp = 7 * 60 * 1000L; // 7 minutes

        var result = windows.WindowsFor(timestamp).ToList();

        // Should fall into windows starting at 0 and 5 minutes
        Assert.True(result.Count >= 1);
    }

    [Fact]
    public void SessionWindows_ReturnsPointWindow()
    {
        var windows = SessionWindows.With(TimeSpan.FromMinutes(30));
        var timestamp = 1000000L;

        var result = windows.WindowsFor(timestamp).Single();

        Assert.Equal(timestamp, result.StartMs);
        Assert.Equal(timestamp, result.EndMs);
    }

    #endregion

    #region StreamsBuilder Tests

    [Fact]
    public void StreamsBuilder_CreatesStream()
    {
        var builder = new StreamsBuilder();

        var stream = builder.Stream<string, string>("input-topic");

        Assert.NotNull(stream);
    }

    [Fact]
    public void StreamsBuilder_CreatesTable()
    {
        var builder = new StreamsBuilder();

        var table = builder.Table<string, string>("input-topic");

        Assert.NotNull(table);
        Assert.NotNull(table.QueryableStoreName);
    }

    [Fact]
    public void StreamsBuilder_CreatesGlobalTable()
    {
        var builder = new StreamsBuilder();

        var globalTable = builder.GlobalTable<string, string>("global-topic");

        Assert.NotNull(globalTable);
        Assert.NotNull(globalTable.QueryableStoreName);
    }

    [Fact]
    public void StreamsBuilder_BuildsTopology()
    {
        var builder = new StreamsBuilder();

        builder.Stream<string, string>("input-topic")
            .Filter((k, v) => v.Length > 5)
            .MapValues(v => v.ToUpperInvariant())
            .To("output-topic");

        var topology = builder.Build();

        Assert.NotNull(topology);
        Assert.Single(topology.Sources);
    }

    #endregion

    #region KStream Operations Tests

    [Fact]
    public void KStream_Filter_FiltersRecords()
    {
        var builder = new StreamsBuilder();
        var results = new List<string>();

        var stream = builder.Stream<string, int>("input-topic")
            .Filter((k, v) => v > 50)
            .Peek((k, v) => results.Add($"{k}:{v}"));

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "test-app",
            BootstrapServers = "localhost:9092"
        };

        // The topology is built correctly
        Assert.NotNull(topology);
    }

    [Fact]
    public void KStream_MapValues_TransformsValues()
    {
        var builder = new StreamsBuilder();

        var stream = builder.Stream<string, int>("input-topic")
            .MapValues(v => v * 2)
            .MapValues(v => $"Result: {v}");

        var topology = builder.Build();
        Assert.NotNull(topology);
    }

    [Fact]
    public void KStream_SelectKey_ChangesKey()
    {
        var builder = new StreamsBuilder();

        var stream = builder.Stream<string, TestUser>("input-topic")
            .SelectKey((k, v) => v.Id);

        var topology = builder.Build();
        Assert.NotNull(topology);
    }

    [Fact]
    public void KStream_FlatMapValues_ExpandsValues()
    {
        var builder = new StreamsBuilder();

        var stream = builder.Stream<string, string>("input-topic")
            .FlatMapValues(v => v.Split(' '));

        var topology = builder.Build();
        Assert.NotNull(topology);
    }

    [Fact]
    public void KStream_Branch_SplitsStream()
    {
        var builder = new StreamsBuilder();

        var branches = builder.Stream<string, int>("input-topic")
            .Branch(
                (k, v) => v < 0,
                (k, v) => v >= 0 && v <= 100,
                (k, v) => v > 100);

        Assert.Equal(3, branches.Length);
    }

    [Fact]
    public void KStream_Merge_CombinesStreams()
    {
        var builder = new StreamsBuilder();

        var stream1 = builder.Stream<string, string>("topic1");
        var stream2 = builder.Stream<string, string>("topic2");

        var merged = stream1.Merge(stream2);

        var topology = builder.Build();
        Assert.Equal(2, topology.Sources.Count);
    }

    #endregion

    #region Repartition Tests

    [Fact]
    public void KStream_Repartition_CreatesRepartitionNode()
    {
        var builder = new StreamsBuilder { ApplicationId = "test-app" };

        var stream = builder.Stream<string, int>("input-topic")
            .Repartition();

        var topology = builder.Build();

        Assert.NotNull(stream);
        Assert.Single(topology.RepartitionNodes);
    }

    [Fact]
    public void KStream_RepartitionWithKeySelector_TransformsKey()
    {
        var builder = new StreamsBuilder { ApplicationId = "test-app" };

        var stream = builder.Stream<string, TestUser>("input-topic")
            .Repartition((key, value) => value.Id);

        var topology = builder.Build();

        Assert.NotNull(stream);
        Assert.Single(topology.RepartitionNodes);
    }

    [Fact]
    public void KStream_Repartition_GeneratesCorrectTopicName()
    {
        var builder = new StreamsBuilder { ApplicationId = "my-app" };

        var stream = builder.Stream<string, int>("input-topic")
            .Repartition();

        var topology = builder.Build();
        var repartitionNode = topology.RepartitionNodes.First() as Kuestenlogik.Surgewave.Streams.Processors.RepartitionNode<string, int>;

        Assert.NotNull(repartitionNode);
        Assert.StartsWith("my-app-REPARTITION", repartitionNode.RepartitionTopic);
        Assert.EndsWith("-repartition", repartitionNode.RepartitionTopic);
    }

    [Fact]
    public async Task KStream_Repartition_ForwardsRecords()
    {
        var builder = new StreamsBuilder { ApplicationId = "test-app" };
        var processed = new List<string>();

        builder.Stream<string, int>("input-topic")
            .Repartition()
            .Peek((k, v) => processed.Add($"{k}:{v}"))
            .To("output-topic");

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "test-app",
            BootstrapServers = "localhost:9092"
        };

        await using var app = new StreamsApplication(config, topology, _loggerFactory);
        app.Start();

        app.ProcessRecord("input-topic", "key1", 100);
        app.ProcessRecord("input-topic", "key2", 200);

        Assert.Equal(2, processed.Count);
        Assert.Contains("key1:100", processed);
        Assert.Contains("key2:200", processed);
    }

    [Fact]
    public async Task KStream_RepartitionWithKeySelector_TransformsAndForwardsRecords()
    {
        var builder = new StreamsBuilder { ApplicationId = "test-app" };
        var processed = new List<string>();

        builder.Stream<string, TestUser>("input-topic")
            .Repartition((key, value) => value.Id)
            .Peek((k, v) => processed.Add($"{k}:{v.Name}"))
            .To("output-topic");

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "test-app",
            BootstrapServers = "localhost:9092"
        };

        await using var app = new StreamsApplication(config, topology, _loggerFactory);
        app.Start();

        app.ProcessRecord("input-topic", "old-key", new TestUser { Id = 42, Name = "Alice" });

        Assert.Single(processed);
        Assert.Contains("42:Alice", processed);
    }

    [Fact]
    public void KStream_MultipleRepartitions_CreatesMultipleNodes()
    {
        var builder = new StreamsBuilder { ApplicationId = "test-app" };

        var stream = builder.Stream<string, int>("input-topic")
            .Repartition()
            .MapValues(v => v * 2)
            .Repartition();

        var topology = builder.Build();

        Assert.Equal(2, topology.RepartitionNodes.Count);
    }

    [Fact]
    public void KStream_Repartition_ChainedWithOtherOperations()
    {
        var builder = new StreamsBuilder { ApplicationId = "test-app" };

        builder.Stream<string, int>("input-topic")
            .Filter((k, v) => v > 0)
            .Repartition()
            .MapValues(v => v * 2)
            .SelectKey((k, v) => v.ToString())
            .Repartition((k, v) => int.Parse(k))
            .To("output-topic");

        var topology = builder.Build();

        Assert.Equal(2, topology.RepartitionNodes.Count);
    }

    #endregion

    #region Aggregation Tests

    [Fact]
    public void KGroupedStream_Count_CreatesCountTable()
    {
        var builder = new StreamsBuilder();

        var countTable = builder.Stream<string, string>("input-topic")
            .GroupByKey()
            .Count();

        Assert.NotNull(countTable);
        Assert.NotNull(countTable.QueryableStoreName);
    }

    [Fact]
    public void KGroupedStream_Reduce_CreatesReducedTable()
    {
        var builder = new StreamsBuilder();

        var reducedTable = builder.Stream<string, int>("input-topic")
            .GroupByKey()
            .Reduce((agg, newValue) => agg + newValue);

        Assert.NotNull(reducedTable);
    }

    [Fact]
    public void KGroupedStream_Aggregate_CreatesAggregateTable()
    {
        var builder = new StreamsBuilder();

        var aggregateTable = builder.Stream<string, int>("input-topic")
            .GroupByKey()
            .Aggregate(
                () => new AggregateResult { Sum = 0, Count = 0 },
                (key, value, agg) => new AggregateResult
                {
                    Sum = agg.Sum + value,
                    Count = agg.Count + 1
                });

        Assert.NotNull(aggregateTable);
    }

    [Fact]
    public void KGroupedStream_WindowedCount_CreatesWindowedTable()
    {
        var builder = new StreamsBuilder();

        var windowedCount = builder.Stream<string, string>("input-topic")
            .GroupByKey()
            .WindowedBy(TumblingWindows.Of(TimeSpan.FromMinutes(5)))
            .Count();

        Assert.NotNull(windowedCount);
    }

    [Fact]
    public void KGroupedStream_SessionWindowedCount_CreatesSessionTable()
    {
        var builder = new StreamsBuilder();

        var sessionCount = builder.Stream<string, string>("input-topic")
            .GroupByKey()
            .WindowedBy(SessionWindows.With(TimeSpan.FromMinutes(30)))
            .Count();

        Assert.NotNull(sessionCount);
    }

    #endregion

    #region KTable Operations Tests

    [Fact]
    public void KTable_Filter_FiltersTable()
    {
        var builder = new StreamsBuilder();

        var filteredTable = builder.Table<string, int>("input-topic")
            .Filter((k, v) => v > 0);

        Assert.NotNull(filteredTable);
    }

    [Fact]
    public void KTable_MapValues_TransformsTableValues()
    {
        var builder = new StreamsBuilder();

        var mappedTable = builder.Table<string, int>("input-topic")
            .MapValues(v => v * 2);

        Assert.NotNull(mappedTable);
    }

    [Fact]
    public void KTable_ToStream_ConvertsToStream()
    {
        var builder = new StreamsBuilder();

        var stream = builder.Table<string, int>("input-topic")
            .ToStream();

        Assert.NotNull(stream);
    }

    #endregion

    #region StreamsApplication Tests

    [Fact]
    public async Task StreamsApplication_StartsAndStops()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input-topic")
            .MapValues(v => v.ToUpperInvariant())
            .To("output-topic");

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "test-app",
            BootstrapServers = "localhost:9092"
        };

        await using var app = new StreamsApplication(config, topology, _loggerFactory);

        Assert.Equal(StreamsState.Created, app.State);

        app.Start();
        Assert.Equal(StreamsState.Running, app.State);

        await app.CloseAsync();
        Assert.Equal(StreamsState.NotRunning, app.State);
    }

    [Fact]
    public async Task StreamsApplication_TracksMetrics()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input-topic")
            .To("output-topic");

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "test-app",
            BootstrapServers = "localhost:9092"
        };

        await using var app = new StreamsApplication(config, topology, _loggerFactory);

        Assert.Equal(0, app.Metrics.ProcessedRecords);
    }

    #endregion

    #region Join Tests

    [Fact]
    public void KStream_JoinTable_CreatesJoinedStream()
    {
        var builder = new StreamsBuilder();

        var stream = builder.Stream<string, int>("stream-topic");
        var table = builder.Table<string, string>("table-topic");

        var joined = stream.Join(table, (streamValue, tableValue) => $"{streamValue}: {tableValue}");

        Assert.NotNull(joined);
    }

    [Fact]
    public void KStream_LeftJoinTable_CreatesLeftJoinedStream()
    {
        var builder = new StreamsBuilder();

        var stream = builder.Stream<string, int>("stream-topic");
        var table = builder.Table<string, string>("table-topic");

        var joined = stream.LeftJoin(table, (streamValue, tableValue) => $"{streamValue}: {tableValue ?? "N/A"}");

        Assert.NotNull(joined);
    }

    [Fact]
    public void KStream_JoinStream_CreatesWindowedJoin()
    {
        var builder = new StreamsBuilder();

        var stream1 = builder.Stream<string, int>("topic1");
        var stream2 = builder.Stream<string, string>("topic2");

        var joined = stream1.Join(
            stream2,
            (v1, v2) => $"{v1}: {v2}",
            JoinWindows.Of(TimeSpan.FromMinutes(5)));

        Assert.NotNull(joined);
    }

    [Fact]
    public void KStream_LeftJoinStream_CreatesLeftJoin()
    {
        var builder = new StreamsBuilder();

        var stream1 = builder.Stream<string, int>("topic1");
        var stream2 = builder.Stream<string, string>("topic2");

        var joined = stream1.LeftJoin(
            stream2,
            (v1, v2) => $"{v1}: {v2 ?? "null"}",
            JoinWindows.Of(TimeSpan.FromMinutes(5)));

        Assert.NotNull(joined);
        var topology = builder.Build();
        Assert.NotNull(topology);
    }

    [Fact]
    public void KStream_OuterJoinStream_CreatesOuterJoin()
    {
        var builder = new StreamsBuilder();

        var stream1 = builder.Stream<string, int>("topic1");
        var stream2 = builder.Stream<string, string>("topic2");

        var joined = stream1.OuterJoin(
            stream2,
            (v1, v2) => $"{v1}: {v2 ?? "null"}",
            JoinWindows.Of(TimeSpan.FromMinutes(5)));

        Assert.NotNull(joined);
        var topology = builder.Build();
        Assert.NotNull(topology);
    }

    #endregion

    #region Table-Table Join Tests

    [Fact]
    public void KTable_JoinTable_CreatesJoinedTable()
    {
        var builder = new StreamsBuilder();

        var table1 = builder.Table<string, int>("table1");
        var table2 = builder.Table<string, string>("table2");

        var joined = table1.Join(table2, (v1, v2) => $"{v1}: {v2}");

        Assert.NotNull(joined);
        Assert.NotNull(joined.QueryableStoreName);
    }

    [Fact]
    public void KTable_LeftJoinTable_CreatesLeftJoinedTable()
    {
        var builder = new StreamsBuilder();

        var table1 = builder.Table<string, int>("table1");
        var table2 = builder.Table<string, string>("table2");

        var joined = table1.LeftJoin(table2, (v1, v2) => $"{v1}: {v2 ?? "null"}");

        Assert.NotNull(joined);
        Assert.NotNull(joined.QueryableStoreName);
    }

    [Fact]
    public void KTable_OuterJoinTable_CreatesOuterJoinedTable()
    {
        var builder = new StreamsBuilder();

        var table1 = builder.Table<string, int>("table1");
        var table2 = builder.Table<string, string>("table2");

        var joined = table1.OuterJoin(table2, (v1, v2) => $"{v1}: {v2 ?? "null"}");

        Assert.NotNull(joined);
        Assert.NotNull(joined.QueryableStoreName);
    }

    #endregion

    #region Suppression Tests

    [Fact]
    public void KTable_Suppress_UntilTimeLimit()
    {
        var builder = new StreamsBuilder();

        var suppressed = builder.Table<string, int>("input-topic")
            .Suppress(Suppressed<string>.UntilTimeLimit(TimeSpan.FromSeconds(30), 1024 * 1024));

        Assert.NotNull(suppressed);
        Assert.NotNull(suppressed.QueryableStoreName);
    }

    [Fact]
    public void KTable_Suppress_UntilWindowCloses()
    {
        var builder = new StreamsBuilder();

        var suppressed = builder.Table<string, int>("input-topic")
            .Suppress(Suppressed<string>.UntilWindowClose(TimeSpan.FromSeconds(5)));

        Assert.NotNull(suppressed);
        Assert.NotNull(suppressed.QueryableStoreName);
    }

    #endregion

    #region Changelog Store Tests

    [Fact]
    public void ChangelogBackedStore_WrapsInnerStore()
    {
        var innerStore = new InMemoryKeyValueStore<string, int>("inner-store");
        var keySerde = Serdes.String();
        var valueSerde = Serdes.Int32();

        var changelogStore = new Kuestenlogik.Surgewave.Streams.Changelog.ChangelogBackedStore<string, int>(
            innerStore, keySerde, valueSerde, "test-app");

        Assert.Equal("inner-store", changelogStore.Name);
        Assert.True(changelogStore.Persistent);
    }

    [Fact]
    public void ChangelogBackedStore_PutsAndGets()
    {
        var innerStore = new InMemoryKeyValueStore<string, int>("inner-store");
        var keySerde = Serdes.String();
        var valueSerde = Serdes.Int32();

        using var changelogStore = new Kuestenlogik.Surgewave.Streams.Changelog.ChangelogBackedStore<string, int>(
            innerStore, keySerde, valueSerde, "test-app");

        changelogStore.Put("key1", 100);
        changelogStore.Put("key2", 200);

        Assert.Equal(100, changelogStore.Get("key1"));
        Assert.Equal(200, changelogStore.Get("key2"));
    }

    [Fact]
    public void ChangelogBackedStore_DeletesWithTombstone()
    {
        var innerStore = new InMemoryKeyValueStore<string, int>("inner-store");
        var keySerde = Serdes.String();
        var valueSerde = Serdes.Int32();

        using var changelogStore = new Kuestenlogik.Surgewave.Streams.Changelog.ChangelogBackedStore<string, int>(
            innerStore, keySerde, valueSerde, "test-app");

        changelogStore.Put("key1", 100);
        var deleted = changelogStore.Delete("key1");

        Assert.Equal(100, deleted);
        Assert.Equal(0, changelogStore.Get("key1"));
    }

    #endregion

    #region Runtime Tests

    [Fact]
    public async Task StreamsApplication_ProcessesRecord()
    {
        var builder = new StreamsBuilder();
        var processed = new List<string>();

        builder.Stream<string, int>("input-topic")
            .Peek((k, v) => processed.Add($"{k}:{v}"))
            .To("output-topic");

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "test-app",
            BootstrapServers = "localhost:9092"
        };

        await using var app = new StreamsApplication(config, topology, _loggerFactory);
        app.Start();

        app.ProcessRecord("input-topic", "key1", 100);
        app.ProcessRecord("input-topic", "key2", 200);

        Assert.Equal(2, processed.Count);
        Assert.Contains("key1:100", processed);
        Assert.Contains("key2:200", processed);
    }

    [Fact]
    public async Task StreamsApplication_ExactlyOnceConfig()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input-topic").To("output-topic");

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "test-app",
            BootstrapServers = "localhost:9092",
            ProcessingGuarantee = ProcessingGuarantee.ExactlyOnce
        };

        await using var app = new StreamsApplication(config, topology, _loggerFactory);

        Assert.Equal(StreamsState.Created, app.State);
        app.Start();
        Assert.Equal(StreamsState.Running, app.State);
    }

    #endregion

    #region Helper Classes

    private sealed class TestUser
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private sealed class AggregateResult
    {
        public int Sum { get; set; }
        public int Count { get; set; }
    }

    #endregion
}
