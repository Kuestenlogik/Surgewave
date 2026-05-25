using Kuestenlogik.Surgewave.Streams.InteractiveQueries;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// Tests for the IQS core infrastructure:
/// StateStoreRegistry, StateStoreQueryExecutor, StateStoreInfo, and response DTOs.
/// </summary>
public sealed class InteractiveQueryServiceTests
{
    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static InMemoryKeyValueStore<string, int> MakeKvStore(string name = "kv-store")
        => new(name, StringComparer.Ordinal);

    private static InMemoryWindowStore<string, int> MakeWindowStore(string name = "win-store")
        => new(name, TimeSpan.FromMinutes(1), TimeSpan.FromHours(1));

    private static InMemorySessionStore<string, int> MakeSessionStore(string name = "sess-store")
        => new(name, TimeSpan.FromHours(1));

    private static StateStoreRegistry NewRegistry() => new();

    // -----------------------------------------------------------------------
    // Registry: basic CRUD
    // -----------------------------------------------------------------------

    [Fact]
    public void Registry_Register_StoreIsRetrievable()
    {
        var registry = NewRegistry();
        var store = MakeKvStore();
        registry.Register(store.Name, store);

        var retrieved = registry.GetStore(store.Name);

        Assert.NotNull(retrieved);
        Assert.Same(store, retrieved);
    }

    [Fact]
    public void Registry_GetStore_ReturnsNullForUnknownName()
    {
        var registry = NewRegistry();

        Assert.Null(registry.GetStore("nonexistent"));
    }

    [Fact]
    public void Registry_Unregister_RemovesStore()
    {
        var registry = NewRegistry();
        var store = MakeKvStore();
        registry.Register(store.Name, store);

        registry.Unregister(store.Name);

        Assert.Null(registry.GetStore(store.Name));
    }

    [Fact]
    public void Registry_GetAllStores_ReturnsAllRegistered()
    {
        var registry = NewRegistry();
        registry.Register("a", MakeKvStore("a"));
        registry.Register("b", MakeKvStore("b"));
        registry.Register("c", MakeWindowStore("c"));

        var all = registry.GetAllStores();

        Assert.Equal(3, all.Count);
    }

    [Fact]
    public void Registry_GetAllStores_EmptyWhenNoneRegistered()
    {
        var registry = NewRegistry();
        Assert.Empty(registry.GetAllStores());
    }

    [Fact]
    public void Registry_Register_OverwritesPreviousRegistration()
    {
        var registry = NewRegistry();
        var storeA = MakeKvStore("s");
        var storeB = MakeKvStore("s");
        registry.Register("s", storeA);
        registry.Register("s", storeB);

        Assert.Same(storeB, registry.GetStore("s"));
    }

    // -----------------------------------------------------------------------
    // Registry: store type detection
    // -----------------------------------------------------------------------

    [Fact]
    public void Registry_StoreInfo_KvStore_DetectedAsKeyValue()
    {
        var registry = NewRegistry();
        var store = MakeKvStore("kv");
        registry.Register("kv", store);

        var info = registry.GetStoreInfo("kv");

        Assert.NotNull(info);
        Assert.Equal(StateStoreType.KeyValue, info!.StoreType);
    }

    [Fact]
    public void Registry_StoreInfo_WindowStore_DetectedAsWindow()
    {
        var registry = NewRegistry();
        var store = MakeWindowStore("win");
        registry.Register("win", store);

        var info = registry.GetStoreInfo("win");

        Assert.NotNull(info);
        Assert.Equal(StateStoreType.Window, info!.StoreType);
    }

    [Fact]
    public void Registry_StoreInfo_SessionStore_DetectedAsSession()
    {
        var registry = NewRegistry();
        var store = MakeSessionStore("sess");
        registry.Register("sess", store);

        var info = registry.GetStoreInfo("sess");

        Assert.NotNull(info);
        Assert.Equal(StateStoreType.Session, info!.StoreType);
    }

    [Fact]
    public void Registry_StoreInfo_ReturnsNullForUnknownName()
    {
        var registry = NewRegistry();

        Assert.Null(registry.GetStoreInfo("no-such-store"));
    }

    // -----------------------------------------------------------------------
    // Registry: StateStoreInfo metadata
    // -----------------------------------------------------------------------

    [Fact]
    public void Registry_StoreInfo_PersistentFlagCorrect_InMemory()
    {
        var registry = NewRegistry();
        var store = MakeKvStore("kv");
        registry.Register("kv", store);

        var info = registry.GetStoreInfo("kv")!;

        Assert.False(info.Persistent);
    }

    [Fact]
    public void Registry_StoreInfo_NameMatchesRegisteredName()
    {
        var registry = NewRegistry();
        var store = MakeKvStore("my-store");
        registry.Register("my-store", store);

        var info = registry.GetStoreInfo("my-store")!;

        Assert.Equal("my-store", info.Name);
    }

    [Fact]
    public void Registry_StoreInfo_EntryCountReflectsStoreContents()
    {
        var registry = NewRegistry();
        var store = MakeKvStore("kv");
        store.Put("x", 1);
        store.Put("y", 2);
        registry.Register("kv", store);

        var info = registry.GetStoreInfo("kv")!;

        Assert.Equal(2, info.ApproximateEntryCount);
    }

    // -----------------------------------------------------------------------
    // QueryExecutor: GetCount
    // -----------------------------------------------------------------------

    [Fact]
    public void QueryExecutor_GetCount_ReturnsCorrectCount()
    {
        var registry = NewRegistry();
        var store = MakeKvStore("kv");
        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);
        registry.Register("kv", store);

        var executor = new StateStoreQueryExecutor(registry);

        Assert.Equal(3, executor.GetCount("kv"));
    }

    [Fact]
    public void QueryExecutor_GetCount_ThrowsForUnknownStore()
    {
        var registry = NewRegistry();
        var executor = new StateStoreQueryExecutor(registry);

        Assert.Throws<KeyNotFoundException>(() => executor.GetCount("missing"));
    }

    // -----------------------------------------------------------------------
    // QueryExecutor: GetAll with pagination
    // -----------------------------------------------------------------------

    [Fact]
    public void QueryExecutor_GetAll_ReturnsAllEntries()
    {
        var registry = NewRegistry();
        var store = MakeKvStore("kv");
        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);
        registry.Register("kv", store);

        var executor = new StateStoreQueryExecutor(registry);
        var result = executor.GetAll("kv", 0, 100);

        Assert.Equal(3, result.Count);
    }

    [Fact]
    public void QueryExecutor_GetAll_RespectsOffset()
    {
        var registry = NewRegistry();
        var store = MakeKvStore("kv");
        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);
        registry.Register("kv", store);

        var executor = new StateStoreQueryExecutor(registry);
        var result = executor.GetAll("kv", 2, 100);

        Assert.Single(result);
    }

    [Fact]
    public void QueryExecutor_GetAll_RespectsLimit()
    {
        var registry = NewRegistry();
        var store = MakeKvStore("kv");
        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);
        registry.Register("kv", store);

        var executor = new StateStoreQueryExecutor(registry);
        var result = executor.GetAll("kv", 0, 2);

        Assert.Equal(2, result.Count);
    }

    [Fact]
    public void QueryExecutor_GetAll_EmptyStoreReturnsEmpty()
    {
        var registry = NewRegistry();
        var store = MakeKvStore("kv");
        registry.Register("kv", store);

        var executor = new StateStoreQueryExecutor(registry);
        var result = executor.GetAll("kv", 0, 100);

        Assert.Empty(result);
    }

    [Fact]
    public void QueryExecutor_GetAll_ThrowsForUnknownStore()
    {
        var registry = NewRegistry();
        var executor = new StateStoreQueryExecutor(registry);

        Assert.Throws<KeyNotFoundException>(() => executor.GetAll("missing", 0, 100));
    }

    // -----------------------------------------------------------------------
    // QueryExecutor: GetByKey
    // -----------------------------------------------------------------------

    [Fact]
    public void QueryExecutor_GetByKey_ReturnsValueForExistingKey()
    {
        var registry = NewRegistry();
        var store = MakeKvStore("kv");
        store.Put("hello", 99);
        registry.Register("kv", store);

        var executor = new StateStoreQueryExecutor(registry);
        var result = executor.GetByKey("kv", "hello");

        Assert.NotNull(result);
    }

    [Fact]
    public void QueryExecutor_GetByKey_ReturnsNullForMissingKey()
    {
        // Use string value type so null is a genuine absent-value signal
        var registry = NewRegistry();
        var store = new InMemoryKeyValueStore<string, string>("kv-str", StringComparer.Ordinal);
        registry.Register("kv-str", store);

        var executor = new StateStoreQueryExecutor(registry);
        var result = executor.GetByKey("kv-str", "no-such-key");

        Assert.Null(result);
    }

    [Fact]
    public void QueryExecutor_GetByKey_ThrowsForUnknownStore()
    {
        var registry = NewRegistry();
        var executor = new StateStoreQueryExecutor(registry);

        Assert.Throws<KeyNotFoundException>(() => executor.GetByKey("missing", "k"));
    }

    // -----------------------------------------------------------------------
    // Response DTOs
    // -----------------------------------------------------------------------

    [Fact]
    public void StoreListResponse_TotalCount_ReflectsStoreCount()
    {
        var response = new StoreListResponse
        {
            Stores =
            [
                new StateStoreInfo("a", StateStoreType.KeyValue, false, 0),
                new StateStoreInfo("b", StateStoreType.Window, false, 10)
            ]
        };

        Assert.Equal(2, response.TotalCount);
    }

    [Fact]
    public void StoreEntriesResponse_HasMore_FalseWhenLastPage()
    {
        var response = new StoreEntriesResponse
        {
            StoreName = "kv",
            Offset = 0,
            Limit = 10,
            TotalCount = 3,
            Entries =
            [
                new StoreEntryResponse { Key = "a", Value = 1 },
                new StoreEntryResponse { Key = "b", Value = 2 },
                new StoreEntryResponse { Key = "c", Value = 3 }
            ]
        };

        Assert.False(response.HasMore);
    }

    [Fact]
    public void StoreEntriesResponse_HasMore_TrueWhenMoreEntriesExist()
    {
        var response = new StoreEntriesResponse
        {
            StoreName = "kv",
            Offset = 0,
            Limit = 2,
            TotalCount = 5,
            Entries =
            [
                new StoreEntryResponse { Key = "a", Value = 1 },
                new StoreEntryResponse { Key = "b", Value = 2 }
            ]
        };

        Assert.True(response.HasMore);
    }

    // -----------------------------------------------------------------------
    // InteractiveQueryConfig
    // -----------------------------------------------------------------------

    [Fact]
    public void InteractiveQueryConfig_Defaults_AreCorrect()
    {
        var config = new InteractiveQueryConfig();

        Assert.False(config.Enabled);
        Assert.Equal(1000, config.MaxEntriesPerPage);
    }
}
