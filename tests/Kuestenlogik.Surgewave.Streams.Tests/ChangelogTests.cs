using Kuestenlogik.Surgewave.Streams.Changelog;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class ChangelogTests
{
    [Fact]
    public void ChangelogBackedStore_WritesChangelog()
    {
        var inner = new InMemoryKeyValueStore<string, int>("test-store");
        var keySerde = Serdes.Json<string>();
        var valueSerde = Serdes.Json<int>();

        var store = new ChangelogBackedStore<string, int>(inner, keySerde, valueSerde, "app");

        var config = new StreamsConfig { ApplicationId = "app", BootstrapServers = "localhost:9092" };
        var context = new ProcessorContext(config, new StreamsMetrics(),
            NullLoggerFactory.Instance.CreateLogger("test"));
        store.Init(context);

        store.Put("key1", 42);
        Assert.Equal(42, store.Get("key1"));
        Assert.Equal(42, inner.Get("key1")); // writes through
    }

    [Fact]
    public void ChangelogBackedStore_DeleteWritesTombstone()
    {
        var inner = new InMemoryKeyValueStore<string, int>("test-store");
        var keySerde = Serdes.Json<string>();
        var valueSerde = Serdes.Json<int>();

        var store = new ChangelogBackedStore<string, int>(inner, keySerde, valueSerde, "app");

        var config = new StreamsConfig { ApplicationId = "app", BootstrapServers = "localhost:9092" };
        var context = new ProcessorContext(config, new StreamsMetrics(),
            NullLoggerFactory.Instance.CreateLogger("test"));
        store.Init(context);

        store.Put("key1", 42);
        var deleted = store.Delete("key1");

        Assert.Equal(42, deleted);
        Assert.Equal(0, inner.Get("key1")); // default int
    }

    [Fact]
    public void Checkpoint_CreateAndRestore()
    {
        var inner = new InMemoryKeyValueStore<string, int>("test-store");
        var keySerde = Serdes.Json<string>();
        var valueSerde = Serdes.Json<int>();

        var store = new ChangelogBackedStore<string, int>(inner, keySerde, valueSerde, "app");

        var config = new StreamsConfig { ApplicationId = "app", BootstrapServers = "localhost:9092" };
        var context = new ProcessorContext(config, new StreamsMetrics(),
            NullLoggerFactory.Instance.CreateLogger("test"));
        store.Init(context);

        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);

        // Create checkpoint
        var checkpoint = store.CreateCheckpoint();
        Assert.Equal("test-store", checkpoint.StoreName);
        Assert.Equal(3, checkpoint.EntryCount);
        Assert.NotNull(checkpoint.SnapshotData);
        Assert.True(checkpoint.SnapshotData!.Length > 0);

        // Restore to a new store
        var inner2 = new InMemoryKeyValueStore<string, int>("test-store");
        var store2 = new ChangelogBackedStore<string, int>(inner2, keySerde, valueSerde, "app");
        store2.Init(context);

        store2.RestoreFromCheckpoint(checkpoint);

        Assert.Equal(1, store2.Get("a"));
        Assert.Equal(2, store2.Get("b"));
        Assert.Equal(3, store2.Get("c"));
    }

    [Fact]
    public void Materialized_WithLogging()
    {
        var mat = Materialized<string, int>.As("my-store")
            .WithLogging(new ChangelogConfig { Enabled = true, Compacted = true });

        Assert.True(mat.LoggingEnabled);
        Assert.NotNull(mat.ChangelogConfig);
        Assert.True(mat.ChangelogConfig!.Enabled);
        Assert.True(mat.ChangelogConfig.Compacted);
    }

    [Fact]
    public void Materialized_WithCleanupPolicy()
    {
        var mat = Materialized<string, int>.As("my-store")
            .WithCleanupPolicy(CleanupPolicy.CompactAndDelete);

        Assert.NotNull(mat.ChangelogConfig);
        Assert.Equal(CleanupPolicy.CompactAndDelete, mat.ChangelogConfig!.CleanupPolicy);
        Assert.True(mat.ChangelogConfig.Compacted);
    }

    [Fact]
    public void Materialized_WithLoggingDisabled()
    {
        var mat = Materialized<string, int>.As("my-store")
            .WithLoggingDisabled();

        Assert.False(mat.LoggingEnabled);
        Assert.NotNull(mat.ChangelogConfig);
        Assert.False(mat.ChangelogConfig!.Enabled);
    }
}
