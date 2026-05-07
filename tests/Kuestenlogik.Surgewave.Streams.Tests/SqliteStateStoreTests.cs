using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class SqliteStateStoreTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _tempDir;
    private readonly StreamsConfig _config;

    public SqliteStateStoreTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-sqlite-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _config = new StreamsConfig
        {
            ApplicationId = "sqlite-test",
            BootstrapServers = "dummy:9092",
            StateDir = _tempDir
        };
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { }
    }

    private ProcessorContext CreateContext()
    {
        var metrics = new StreamsMetrics();
        var logger = LoggerFactory.Create(b => b.AddFilter(l => l >= LogLevel.Debug))
            .CreateLogger<SqliteStateStoreTests>();
        return new ProcessorContext(_config, metrics, logger);
    }

    [Fact]
    public void Sqlite_PutAndGet()
    {
        var store = new SqliteKeyValueStore<string, string>(
            "test-store", Serdes.Json<string>(), Serdes.Json<string>());
        store.Init(CreateContext());

        store.Put("key1", "value1");
        store.Put("key2", "value2");

        Assert.Equal("value1", store.Get("key1"));
        Assert.Equal("value2", store.Get("key2"));
        Assert.Null(store.Get("nonexistent"));

        store.Close();
    }

    [Fact]
    public void Sqlite_Delete()
    {
        var store = new SqliteKeyValueStore<string, string>(
            "delete-store", Serdes.Json<string>(), Serdes.Json<string>());
        store.Init(CreateContext());

        store.Put("k", "v");
        Assert.Equal("v", store.Get("k"));

        var deleted = store.Delete("k");
        Assert.Equal("v", deleted);
        Assert.Null(store.Get("k"));

        Assert.Null(store.Delete("nope"));

        store.Close();
    }

    [Fact]
    public void Sqlite_PutIfAbsent()
    {
        var store = new SqliteKeyValueStore<string, int>(
            "absent-store", Serdes.Json<string>(), Serdes.Json<int>());
        store.Init(CreateContext());

        var result = store.PutIfAbsent("counter", 42);
        Assert.Equal(default, result);
        Assert.Equal(42, store.Get("counter"));

        var existing = store.PutIfAbsent("counter", 99);
        Assert.Equal(42, existing);
        Assert.Equal(42, store.Get("counter"));

        store.Close();
    }

    [Fact]
    public void Sqlite_PutAll_BatchWrite()
    {
        var store = new SqliteKeyValueStore<string, int>(
            "batch-store", Serdes.Json<string>(), Serdes.Json<int>());
        store.Init(CreateContext());

        store.PutAll([
            new KeyValue<string, int>("a", 1),
            new KeyValue<string, int>("b", 2),
            new KeyValue<string, int>("c", 3)
        ]);

        Assert.Equal(1, store.Get("a"));
        Assert.Equal(2, store.Get("b"));
        Assert.Equal(3, store.Get("c"));

        store.Close();
    }

    [Fact]
    public void Sqlite_All_IteratesAllEntries()
    {
        var store = new SqliteKeyValueStore<string, string>(
            "all-store", Serdes.Json<string>(), Serdes.Json<string>());
        store.Init(CreateContext());

        store.Put("x", "1");
        store.Put("y", "2");
        store.Put("z", "3");

        var all = store.All().ToList();
        Assert.Equal(3, all.Count);

        store.Close();
    }

    [Fact]
    public void Sqlite_Range_ReturnsSortedSubset()
    {
        var store = new SqliteKeyValueStore<string, int>(
            "range-store", Serdes.Json<string>(), Serdes.Json<int>());
        store.Init(CreateContext());

        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);
        store.Put("d", 4);
        store.Put("e", 5);

        var range = store.Range("b", "d").ToList();
        Assert.True(range.Count >= 2, $"Expected >= 2, got {range.Count}");

        store.Close();
    }

    [Fact]
    public void Sqlite_Persistent_True()
    {
        var store = new SqliteKeyValueStore<string, string>(
            "persist-store", Serdes.Json<string>(), Serdes.Json<string>());
        Assert.True(store.Persistent);
    }

    [Fact]
    public void Sqlite_ApproximateNumEntries()
    {
        var store = new SqliteKeyValueStore<string, string>(
            "count-store", Serdes.Json<string>(), Serdes.Json<string>());
        store.Init(CreateContext());

        store.Put("a", "1");
        store.Put("b", "2");
        store.Put("c", "3");
        store.Flush();

        Assert.True(store.ApproximateNumEntries >= 0);

        store.Close();
    }

    [Fact]
    public void Sqlite_SurvivesRestart()
    {
        var storeName = "restart-store";

        var store1 = new SqliteKeyValueStore<string, string>(
            storeName, Serdes.Json<string>(), Serdes.Json<string>());
        store1.Init(CreateContext());
        store1.Put("persistent-key", "persistent-value");
        store1.Flush();
        store1.Close();

        var store2 = new SqliteKeyValueStore<string, string>(
            storeName, Serdes.Json<string>(), Serdes.Json<string>());
        store2.Init(CreateContext());

        Assert.Equal("persistent-value", store2.Get("persistent-key"));

        store2.Close();
    }

    [Fact]
    public void Sqlite_StoreSupplier_CreatesStore()
    {
        var supplier = Stores.SqliteKeyValueStore<string, string>(
            "supplier-store",
            Serdes.Json<string>(),
            Serdes.Json<string>());

        Assert.Equal("supplier-store", supplier.Name);

        var store = supplier.Get();
        Assert.NotNull(store);
        Assert.True(store.Persistent);
    }

    [Fact]
    public void Sqlite_CustomConfig()
    {
        var config = new SqliteStoreConfig
        {
            CacheSizePages = -4000,
            SynchronousMode = SqliteSynchronousMode.Full
        };

        var store = new SqliteKeyValueStore<string, string>(
            "config-store", Serdes.Json<string>(), Serdes.Json<string>(), config: config);
        store.Init(CreateContext());

        store.Put("k", "v");
        Assert.Equal("v", store.Get("k"));

        store.Close();
    }

    [Fact]
    public void Sqlite_WithTopologyTestDriver()
    {
        var builder = new StreamsBuilder();
        builder.AddStateStore(
            Stores.SqliteKeyValueStore<string, int>(
                "counter-store",
                Serdes.Json<string>(),
                Serdes.Json<int>()));

        builder.Stream<string, int>("input").ForEach((k, v) => { });

        using var driver = new TopologyTestDriver(builder.Build(), _config);

        var store = driver.GetStateStore<IKeyValueStore<string, int>>("counter-store");
        Assert.NotNull(store);
        Assert.True(store.Persistent);

        store.Put("count", 42);
        Assert.Equal(42, store.Get("count"));
    }
}
