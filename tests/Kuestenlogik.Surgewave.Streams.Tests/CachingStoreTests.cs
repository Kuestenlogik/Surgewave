using Kuestenlogik.Surgewave.Streams;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class CachingStoreTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public CachingStoreTests(ITestOutputHelper output)
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

    private ProcessorContext CreateContext()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "caching-test",
            BootstrapServers = "localhost:9092"
        };
        var logger = _loggerFactory.CreateLogger<CachingStoreTests>();
        return new ProcessorContext(config, new StreamsMetrics(), logger);
    }

    [Fact]
    public void CachingStore_Put_Get_ReturnsFromCache()
    {
        var underlying = new InMemoryKeyValueStore<string, string>("test-store");
        var cache = new CachingKeyValueStore<string, string>(underlying, maxCacheSize: 100);
        cache.Init(CreateContext());

        cache.Put("key1", "value1");

        // Should read from cache, not underlying
        Assert.Equal("value1", cache.Get("key1"));
        Assert.Equal(1, cache.DirtyCount);

        // Underlying should NOT have the value yet (not flushed)
        Assert.Null(underlying.Get("key1"));
    }

    [Fact]
    public void CachingStore_Flush_WritesToUnderlying()
    {
        var underlying = new InMemoryKeyValueStore<string, string>("test-store");
        var cache = new CachingKeyValueStore<string, string>(underlying, maxCacheSize: 100);
        cache.Init(CreateContext());

        cache.Put("key1", "value1");
        cache.Put("key2", "value2");

        Assert.Equal(2, cache.DirtyCount);

        cache.Flush();

        Assert.Equal(0, cache.DirtyCount);
        Assert.Equal("value1", underlying.Get("key1"));
        Assert.Equal("value2", underlying.Get("key2"));
    }

    [Fact]
    public void CachingStore_Delete_CachesTombstone()
    {
        var underlying = new InMemoryKeyValueStore<string, string>("test-store");
        var cache = new CachingKeyValueStore<string, string>(underlying, maxCacheSize: 100);
        cache.Init(CreateContext());

        // Put and flush to underlying
        cache.Put("key1", "value1");
        cache.Flush();

        // Delete should create tombstone in cache
        cache.Delete("key1");

        // Get should return null (tombstone)
        Assert.Null(cache.Get("key1"));

        // Underlying still has the value until flush
        Assert.Equal("value1", underlying.Get("key1"));

        cache.Flush();
        Assert.Null(underlying.Get("key1"));
    }

    [Fact]
    public void CachingStore_EvictsOnMaxSize()
    {
        var underlying = new InMemoryKeyValueStore<string, int>("test-store");
        var cache = new CachingKeyValueStore<string, int>(underlying, maxCacheSize: 5);
        cache.Init(CreateContext());

        // Fill beyond cache limit
        for (var i = 0; i < 10; i++)
        {
            cache.Put($"key{i}", i);
        }

        // Should have auto-flushed when exceeding maxCacheSize
        Assert.True(underlying.ApproximateNumEntries > 0);
    }

    [Fact]
    public void CachingStore_Get_FallsThrough_ToUnderlying()
    {
        var underlying = new InMemoryKeyValueStore<string, string>("test-store");
        var cache = new CachingKeyValueStore<string, string>(underlying, maxCacheSize: 100);
        var context = CreateContext();
        underlying.Init(context);
        cache.Init(context);

        // Put directly in underlying (simulating pre-existing data)
        underlying.Put("preexisting", "data");

        // Cache should fall through and find it
        Assert.Equal("data", cache.Get("preexisting"));
    }

    [Fact]
    public void CachingStore_PutOverwrite_UpdatesCache()
    {
        var underlying = new InMemoryKeyValueStore<string, string>("test-store");
        var cache = new CachingKeyValueStore<string, string>(underlying, maxCacheSize: 100);
        cache.Init(CreateContext());

        cache.Put("key1", "v1");
        cache.Put("key1", "v2");
        cache.Put("key1", "v3");

        // Only latest value
        Assert.Equal("v3", cache.Get("key1"));
        Assert.Equal(1, cache.DirtyCount);

        cache.Flush();
        Assert.Equal("v3", underlying.Get("key1"));
    }
}
