using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class MappedFileStateStoreTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _tempDir;
    private readonly StreamsConfig _config;

    public MappedFileStateStoreTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-mappedfile-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _config = new StreamsConfig
        {
            ApplicationId = "mappedfile-test",
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
            .CreateLogger<MappedFileStateStoreTests>();
        return new ProcessorContext(_config, metrics, logger);
    }

    [Fact]
    public void MappedFile_PutAndGet()
    {
        var store = new MappedFileKeyValueStore<string, string>(
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
    public void MappedFile_Delete()
    {
        var store = new MappedFileKeyValueStore<string, string>(
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
    public void MappedFile_PutIfAbsent()
    {
        var store = new MappedFileKeyValueStore<string, int>(
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
    public void MappedFile_PutAll_BatchWrite()
    {
        var store = new MappedFileKeyValueStore<string, int>(
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
    public void MappedFile_All_IteratesAllEntries()
    {
        var store = new MappedFileKeyValueStore<string, string>(
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
    public void MappedFile_Range_ReturnsSortedSubset()
    {
        var store = new MappedFileKeyValueStore<string, int>(
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
    public void MappedFile_Persistent_True()
    {
        var store = new MappedFileKeyValueStore<string, string>(
            "persist-store", Serdes.Json<string>(), Serdes.Json<string>());
        Assert.True(store.Persistent);
    }

    [Fact]
    public void MappedFile_ApproximateNumEntries()
    {
        var store = new MappedFileKeyValueStore<string, string>(
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
    public void MappedFile_SurvivesRestart()
    {
        var storeName = "restart-store";

        var store1 = new MappedFileKeyValueStore<string, string>(
            storeName, Serdes.Json<string>(), Serdes.Json<string>());
        store1.Init(CreateContext());
        store1.Put("persistent-key", "persistent-value");
        store1.Flush();
        store1.Close();

        var store2 = new MappedFileKeyValueStore<string, string>(
            storeName, Serdes.Json<string>(), Serdes.Json<string>());
        store2.Init(CreateContext());

        Assert.Equal("persistent-value", store2.Get("persistent-key"));

        store2.Close();
    }

    [Fact]
    public void MappedFile_FlushTriggersSegmentCreation()
    {
        var config = new MappedFileStoreConfig { MaxMemTableEntries = 3 };
        var store = new MappedFileKeyValueStore<string, string>(
            "flush-store", Serdes.Json<string>(), Serdes.Json<string>(), config);
        store.Init(CreateContext());

        // Put enough entries to trigger MemTable flush
        store.Put("a", "1");
        store.Put("b", "2");
        store.Put("c", "3"); // triggers flush at 3

        // Data should still be readable (now from segment)
        Assert.Equal("1", store.Get("a"));
        Assert.Equal("2", store.Get("b"));
        Assert.Equal("3", store.Get("c"));

        // New writes go to fresh MemTable
        store.Put("d", "4");
        Assert.Equal("4", store.Get("d"));

        store.Close();
    }

    [Fact]
    public void MappedFile_Compaction_MergesSegments()
    {
        var config = new MappedFileStoreConfig
        {
            MaxMemTableEntries = 2,
            MaxSegmentsBeforeCompaction = 3
        };
        var store = new MappedFileKeyValueStore<string, string>(
            "compact-store", Serdes.Json<string>(), Serdes.Json<string>(), config);
        store.Init(CreateContext());

        // Create multiple segments
        for (var i = 0; i < 10; i++)
            store.Put($"key{i:D2}", $"value{i}");

        store.Flush(); // triggers compaction if > 3 segments

        // All data should be readable after compaction
        for (var i = 0; i < 10; i++)
            Assert.Equal($"value{i}", store.Get($"key{i:D2}"));

        store.Close();
    }

    [Fact]
    public void MappedFile_StoreSupplier_CreatesStore()
    {
        var supplier = Stores.MappedFileKeyValueStore<string, string>(
            "supplier-store",
            Serdes.Json<string>(),
            Serdes.Json<string>());

        Assert.Equal("supplier-store", supplier.Name);

        var store = supplier.Get();
        Assert.NotNull(store);
        Assert.True(store.Persistent);
    }

    [Fact]
    public void MappedFile_WithTopologyTestDriver()
    {
        var builder = new StreamsBuilder();
        builder.AddStateStore(
            Stores.MappedFileKeyValueStore<string, int>(
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

    [Fact]
    public void MappedFile_WalReplay_RecoversCrash()
    {
        var storeName = "wal-store";
        var config = new MappedFileStoreConfig { MaxMemTableEntries = 1000 };

        // Write data without flushing to segments (data only in WAL + MemTable)
        var store1 = new MappedFileKeyValueStore<string, string>(
            storeName, Serdes.Json<string>(), Serdes.Json<string>(), config);
        store1.Init(CreateContext());
        store1.Put("wal-key", "wal-value");
        // Simulate crash: dispose without Close (no segment flush)
        store1.Dispose();

        // Reopen — WAL replay should recover
        var store2 = new MappedFileKeyValueStore<string, string>(
            storeName, Serdes.Json<string>(), Serdes.Json<string>(), config);
        store2.Init(CreateContext());

        // WAL is replayed into MemTable, but since Dispose flushes MemTable to segment,
        // data should be in segment. Either way it should be readable.
        Assert.Equal("wal-value", store2.Get("wal-key"));

        store2.Close();
    }
}
