using Kuestenlogik.Surgewave.Streams.InteractiveQueries;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class InteractiveQueryTests
{
    private static StreamsApplication CreateApp(Topology topology)
    {
        var config = new StreamsConfig { ApplicationId = "test-app", BootstrapServers = "localhost:9092" };
        return new StreamsApplication(config, topology, NullLoggerFactory.Instance);
    }

    [Fact]
    public async Task InteractiveQuery_KV_Get()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("input");
        var table = stream.ToTable();

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        var store = app.GetStateStore<IKeyValueStore<string, int>>(table.QueryableStoreName);
        store!.Put("key1", 42);

        var queryStore = app.Store(StoreQueryParameters<ReadOnlyKeyValueStoreWrapper<string, int>>
            .FromNameAndType(table.QueryableStoreName, QueryableStoreTypes.KeyValueStore<string, int>()));

        Assert.NotNull(queryStore);
        Assert.Equal(42, queryStore!.Get("key1"));
    }

    [Fact]
    public async Task InteractiveQuery_KV_All()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("input");
        var table = stream.ToTable();

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        var store = app.GetStateStore<IKeyValueStore<string, int>>(table.QueryableStoreName);
        store!.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);

        var queryStore = app.Store(StoreQueryParameters<ReadOnlyKeyValueStoreWrapper<string, int>>
            .FromNameAndType(table.QueryableStoreName, QueryableStoreTypes.KeyValueStore<string, int>()));

        var all = queryStore!.All().ToList();
        Assert.Equal(3, all.Count);
    }

    [Fact]
    public async Task InteractiveQuery_KV_Range()
    {
        var builder = new StreamsBuilder();
        builder.AddStateStore(Stores.KeyValueStore<string, int>("range-store", StringComparer.Ordinal));

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        var store = app.GetStateStore<IKeyValueStore<string, int>>("range-store");
        store!.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);
        store.Put("d", 4);

        var queryStore = app.Store(StoreQueryParameters<ReadOnlyKeyValueStoreWrapper<string, int>>
            .FromNameAndType("range-store", QueryableStoreTypes.KeyValueStore<string, int>()));

        var range = queryStore!.Range("b", "c").ToList();
        Assert.Equal(2, range.Count);
    }

    [Fact]
    public async Task InteractiveQuery_KV_ApproximateNumEntries()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("input");
        var table = stream.ToTable();

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        var store = app.GetStateStore<IKeyValueStore<string, int>>(table.QueryableStoreName);
        store!.Put("a", 1);
        store.Put("b", 2);

        var queryStore = app.Store(StoreQueryParameters<ReadOnlyKeyValueStoreWrapper<string, int>>
            .FromNameAndType(table.QueryableStoreName, QueryableStoreTypes.KeyValueStore<string, int>()));

        Assert.Equal(2, queryStore!.ApproximateNumEntries);
    }

    [Fact]
    public async Task InteractiveQuery_Window_Fetch()
    {
        var builder = new StreamsBuilder();
        builder.AddStateStore(Stores.WindowStore<string, int>("win-store",
            TimeSpan.FromMinutes(5), TimeSpan.FromHours(1)));

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var store = app.GetStateStore<IWindowStore<string, int>>("win-store");
        store!.Put("key1", 10, now);
        store.Put("key1", 20, now + 1000);

        var queryStore = app.Store(StoreQueryParameters<ReadOnlyWindowStoreWrapper<string, int>>
            .FromNameAndType("win-store", QueryableStoreTypes.WindowStore<string, int>()));

        Assert.NotNull(queryStore);
        Assert.Equal(10, queryStore!.Fetch("key1", now));
        Assert.Equal(20, queryStore.Fetch("key1", now + 1000));
    }

    [Fact]
    public async Task InteractiveQuery_Window_FetchRange()
    {
        var builder = new StreamsBuilder();
        builder.AddStateStore(Stores.WindowStore<string, int>("win-store",
            TimeSpan.FromMinutes(5), TimeSpan.FromHours(1)));

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var store = app.GetStateStore<IWindowStore<string, int>>("win-store");
        store!.Put("key1", 10, now);
        store.Put("key1", 20, now + 1000);
        store.Put("key1", 30, now + 2000);

        var queryStore = app.Store(StoreQueryParameters<ReadOnlyWindowStoreWrapper<string, int>>
            .FromNameAndType("win-store", QueryableStoreTypes.WindowStore<string, int>()));

        var results = queryStore!.Fetch("key1", now, now + 2000).ToList();
        Assert.True(results.Count >= 2);
    }

    [Fact]
    public async Task InteractiveQuery_Session_FindSessions()
    {
        var builder = new StreamsBuilder();
        builder.AddStateStore(Stores.SessionStore<string, int>("sess-store",
            TimeSpan.FromHours(1)));

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        var store = app.GetStateStore<ISessionStore<string, int>>("sess-store");
        Assert.NotNull(store);

        var queryStore = app.Store(StoreQueryParameters<ReadOnlySessionStoreWrapper<string, int>>
            .FromNameAndType("sess-store", QueryableStoreTypes.SessionStore<string, int>()));

        Assert.NotNull(queryStore);
    }

    [Fact]
    public async Task InteractiveQuery_StoreNotFound_ReturnsNull()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, int>("input").To("output");

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        var queryStore = app.Store(StoreQueryParameters<ReadOnlyKeyValueStoreWrapper<string, int>>
            .FromNameAndType("non-existent-store", QueryableStoreTypes.KeyValueStore<string, int>()));

        Assert.Null(queryStore);
    }
}
