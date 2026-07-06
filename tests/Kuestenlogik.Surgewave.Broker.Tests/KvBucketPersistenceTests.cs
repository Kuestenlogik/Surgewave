using System.Text;
using Kuestenlogik.Surgewave.Broker.KeyValue;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Regression tests for KV bucket persistence: entries must be written to the
/// backing topic as real Kafka record batches (PutAsync used to hand the log a
/// custom byte blob, which the append pipeline rejected with
/// ArgumentOutOfRangeException) and must survive a restart via RestoreAsync.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class KvBucketPersistenceTests : IAsyncLifetime, IDisposable
{
    private readonly string _testDirectory;
    private readonly LogManager _logManager;

    public KvBucketPersistenceTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"surgewave-kv-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);
        _logManager = new LogManager(_testDirectory, new MemoryLogSegmentFactory());
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        _logManager.Dispose();
        Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDirectory))
                Directory.Delete(_testDirectory, recursive: true);
        }
        catch (IOException)
        {
            // Best effort — temp cleanup only.
        }
    }

    private async Task<KvBucket> CreateBucketAsync(string name, KvBucketConfig? config = null)
    {
        var bucket = new KvBucket(name, config ?? new KvBucketConfig { MaxHistoryPerKey = 5 }, _logManager);
        await bucket.EnsureBackingTopicAsync();
        return bucket;
    }

    [Fact]
    public async Task PutAsync_PersistsAndGetReturnsValue()
    {
        using var bucket = await CreateBucketAsync("put-get");

        var entry = await bucket.PutAsync("greeting", Encoding.UTF8.GetBytes("hallo kv"));

        Assert.Equal("greeting", entry.Key);
        Assert.Equal(1, entry.Revision);

        var fetched = bucket.Get("greeting");
        Assert.NotNull(fetched);
        Assert.Equal("hallo kv", Encoding.UTF8.GetString(fetched!.Value));
    }

    [Fact]
    public async Task PutAsync_MultipleRevisions_HistoryIsTracked()
    {
        using var bucket = await CreateBucketAsync("history");

        await bucket.PutAsync("key", Encoding.UTF8.GetBytes("v1"));
        await bucket.PutAsync("key", Encoding.UTF8.GetBytes("v2"));
        await bucket.PutAsync("key", Encoding.UTF8.GetBytes("v3"));

        var history = bucket.History("key");
        Assert.Equal(3, history.Count);
        Assert.Equal("v3", Encoding.UTF8.GetString(bucket.Get("key")!.Value));
    }

    [Fact]
    public async Task DeleteAsync_TombstoneHidesKey()
    {
        using var bucket = await CreateBucketAsync("delete");

        await bucket.PutAsync("gone", Encoding.UTF8.GetBytes("x"));
        var tombstone = await bucket.DeleteAsync("gone");

        Assert.Equal("Delete", tombstone.Operation.ToString());
        Assert.Null(bucket.Get("gone"));
        Assert.DoesNotContain("gone", bucket.Keys());
    }

    [Fact]
    public async Task RestoreAsync_RebuildsStateFromBackingTopic()
    {
        using (var bucket = await CreateBucketAsync("restore"))
        {
            await bucket.PutAsync("config", Encoding.UTF8.GetBytes("v1"));
            await bucket.PutAsync("config", Encoding.UTF8.GetBytes("v2"));
            await bucket.PutAsync("flag", Encoding.UTF8.GetBytes("on"));
            await bucket.DeleteAsync("flag");
        }

        // Simulate a broker restart: fresh bucket instance over the same log.
        using var restored = await CreateBucketAsync("restore");
        await restored.RestoreAsync();

        var entry = restored.Get("config");
        Assert.NotNull(entry);
        Assert.Equal("v2", Encoding.UTF8.GetString(entry!.Value));
        Assert.Equal(2, entry.Revision);

        Assert.Null(restored.Get("flag"));
        Assert.Equal(["config"], restored.Keys());

        // Revision high-watermark continues after the last persisted revision.
        var next = await restored.PutAsync("config", Encoding.UTF8.GetBytes("v3"));
        Assert.Equal(5, next.Revision);
    }

    [Fact]
    public async Task RestoreFromTopics_RestoresPersistedBucketConfig()
    {
        using (var manager = new KvBucketManager(_logManager))
        {
            await manager.CreateBucketAsync("cfg", new KvBucketConfig
            {
                MaxHistoryPerKey = 7,
                Ttl = TimeSpan.FromMinutes(30),
                Description = "config survives restarts",
            });
        }

        using var restoredManager = new KvBucketManager(_logManager);
        await restoredManager.RestoreFromTopicsAsync();

        var bucket = restoredManager.GetBucket("cfg");
        Assert.NotNull(bucket);
        Assert.Equal(7, bucket!.Config.MaxHistoryPerKey);
        Assert.Equal(TimeSpan.FromMinutes(30), bucket.Config.Ttl);
        Assert.Equal("config survives restarts", bucket.Config.Description);
    }

    [Fact]
    public async Task GetInfo_ReflectsLiveKeysAndSizes()
    {
        using var bucket = await CreateBucketAsync("info");

        await bucket.PutAsync("a", Encoding.UTF8.GetBytes("12345"));
        await bucket.PutAsync("b", Encoding.UTF8.GetBytes("123"));
        await bucket.DeleteAsync("b");

        var info = bucket.GetInfo();
        Assert.Equal(1, info.KeyCount);
        Assert.Equal(5, info.TotalValueBytes);
        Assert.Equal(3, info.LatestRevision);
    }
}
