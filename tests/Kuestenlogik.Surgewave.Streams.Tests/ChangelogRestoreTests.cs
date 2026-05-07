using Kuestenlogik.Surgewave.Streams.Changelog;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

[Trait("Category", "Unit")]
public class ChangelogRestoreTests
{
    [Fact]
    public void ChangelogBackedStore_ImplementsIChangelogBacked()
    {
        var innerStore = new InMemoryKeyValueStore<string, int>("test-store");
        var store = new ChangelogBackedStore<string, int>(
            innerStore,
            Serdes.Json<string>(),
            Serdes.Json<int>(),
            "test-app");

        Assert.IsAssignableFrom<IChangelogBacked>(store);

        store.Dispose();
    }

    [Fact]
    public void ChangelogBackedStore_ChangelogTopicName_Correct()
    {
        var innerStore = new InMemoryKeyValueStore<string, int>("my-store");
        var store = new ChangelogBackedStore<string, int>(
            innerStore,
            Serdes.Json<string>(),
            Serdes.Json<int>(),
            "my-app");

        Assert.Equal("my-app-my-store-changelog", store.ChangelogTopicName);

        store.Dispose();
    }

    [Fact]
    public void ChangelogBackedStore_RestoreRecord_PutOperation()
    {
        var innerStore = new InMemoryKeyValueStore<string, int>("test-store");
        var keySerde = Serdes.Json<string>();
        var valueSerde = Serdes.Json<int>();
        var store = new ChangelogBackedStore<string, int>(
            innerStore, keySerde, valueSerde, "test-app");

        var keyBytes = keySerde.Serialize("key1");
        var valueBytes = valueSerde.Serialize(42);

        store.RestoreRecord(keyBytes, valueBytes);

        Assert.Equal(42, innerStore.Get("key1"));

        store.Dispose();
    }

    [Fact]
    public void ChangelogBackedStore_RestoreRecord_DeleteOperation()
    {
        var innerStore = new InMemoryKeyValueStore<string, int>("test-store");
        var keySerde = Serdes.Json<string>();
        var valueSerde = Serdes.Json<int>();
        var store = new ChangelogBackedStore<string, int>(
            innerStore, keySerde, valueSerde, "test-app");

        // First put a record
        innerStore.Put("key1", 42);
        Assert.Equal(42, innerStore.Get("key1"));

        // Then restore a tombstone (empty value = delete)
        var keyBytes = keySerde.Serialize("key1");
        store.RestoreRecord(keyBytes, []);

        Assert.Equal(default, innerStore.Get("key1"));

        store.Dispose();
    }

    [Fact]
    public void ChangelogBackedStore_RestoreMultipleRecords()
    {
        var innerStore = new InMemoryKeyValueStore<string, string>("test-store");
        var keySerde = Serdes.Json<string>();
        var valueSerde = Serdes.Json<string>();
        var store = new ChangelogBackedStore<string, string>(
            innerStore, keySerde, valueSerde, "test-app");

        // Restore several records
        for (int i = 0; i < 100; i++)
        {
            var key = keySerde.Serialize($"key-{i}");
            var value = valueSerde.Serialize($"value-{i}");
            store.RestoreRecord(key, value);
        }

        Assert.Equal(100, innerStore.ApproximateNumEntries);
        Assert.Equal("value-50", innerStore.Get("key-50"));
        Assert.Equal("value-99", innerStore.Get("key-99"));

        store.Dispose();
    }

    [Fact]
    public void StateRestoreListener_DelegateListener_FiresCallbacks()
    {
        var startCalled = false;
        var batchCalls = new List<int>();
        var endTotal = 0L;

        var listener = new DelegateStateRestoreListener(
            onStart: ctx =>
            {
                startCalled = true;
                Assert.Equal("test-store", ctx.StoreName);
            },
            onBatch: (ctx, count) => batchCalls.Add(count),
            onEnd: (ctx, total) => endTotal = total);

        var context = new StateRestoreContext
        {
            StoreName = "test-store",
            Topic = "test-changelog",
            Partition = 0,
            StartingOffset = 0,
            EndingOffset = 1000
        };

        listener.OnRestoreStart(context);
        listener.OnBatchRestored(context, 50);
        listener.OnBatchRestored(context, 50);
        listener.OnRestoreEnd(context, 100);

        Assert.True(startCalled);
        Assert.Equal([50, 50], batchCalls);
        Assert.Equal(100, endTotal);
    }

    [Fact]
    public void NoOpStateRestoreListener_DoesNotThrow()
    {
        var listener = NoOpStateRestoreListener.Instance;
        var context = new StateRestoreContext
        {
            StoreName = "store",
            Topic = "topic",
            Partition = 0,
            StartingOffset = 0,
            EndingOffset = 100
        };

        // Should not throw
        listener.OnRestoreStart(context);
        listener.OnBatchRestored(context, 50);
        listener.OnRestoreEnd(context, 50);
    }

    [Fact]
    public async Task StreamTask_InitializeWithRestore_NoChangelogStores_Succeeds()
    {
        // Build a simple topology without changelog-backed stores
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input-topic");
        var topology = builder.Build();

        var config = new StreamsConfig
        {
            ApplicationId = "test-app",
            BootstrapServers = "localhost:9092"
        };

        var metrics = new StreamsMetrics();
        var context = new ProcessorContext(config, metrics, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        var taskId = new TaskId(0, 0)
        {
            Partitions = [new TopicPartition("input-topic", 0)]
        };

        var task = new StreamTask(taskId, topology, context, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        // Should complete without errors — no changelog stores to restore
        await task.InitializeWithRestoreAsync();

        task.Dispose();
        metrics.Dispose();
    }

    [Fact]
    public void StreamsMetrics_RestoreCounters_Initialize()
    {
        var metrics = new StreamsMetrics();

        Assert.Equal(0, metrics.StoresRestored);
        Assert.Equal(0, metrics.RecordsRestored);

        metrics.RecordStoreRestored();
        metrics.RecordRestoredRecords(50);

        Assert.Equal(1, metrics.StoresRestored);
        Assert.Equal(50, metrics.RecordsRestored);

        metrics.RecordRestoredRecords(25);
        Assert.Equal(75, metrics.RecordsRestored);

        metrics.Dispose();
    }

    [Fact]
    public void StateRestoreContext_TracksProgress()
    {
        var context = new StateRestoreContext
        {
            StoreName = "my-store",
            Topic = "app-my-store-changelog",
            Partition = 2,
            StartingOffset = 0,
            EndingOffset = 5000
        };

        Assert.Equal("my-store", context.StoreName);
        Assert.Equal("app-my-store-changelog", context.Topic);
        Assert.Equal(2, context.Partition);
        Assert.Equal(0, context.StartingOffset);
        Assert.Equal(5000, context.EndingOffset);
        Assert.Equal(0, context.TotalRestored);

        context.TotalRestored = 2500;
        Assert.Equal(2500, context.TotalRestored);
    }

    [Fact]
    public void IChangelogBacked_PartitionIsAccessible()
    {
        var innerStore = new InMemoryKeyValueStore<string, string>("partitioned-store");
        var store = new ChangelogBackedStore<string, string>(
            innerStore,
            Serdes.Json<string>(),
            Serdes.Json<string>(),
            "app");

        // Before Init, partition defaults to 0
        Assert.Equal(0, store.ChangelogPartition);

        store.Dispose();
    }
}
