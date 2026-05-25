using Kuestenlogik.Surgewave.Streams.InteractiveQueries;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// Tests for the Interactive Query Service REST API handler methods.
/// All handlers are tested directly (no web host required).
/// </summary>
public sealed class InteractiveQueryRestApiTests
{
    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static (StateStoreRegistry registry, StateStoreQueryExecutor executor)
        CreateSut() =>
        (new StateStoreRegistry(), new StateStoreQueryExecutor(new StateStoreRegistry()));

    private static (StateStoreRegistry registry, StateStoreQueryExecutor executor)
        CreateSutShared()
    {
        var registry = new StateStoreRegistry();
        var executor = new StateStoreQueryExecutor(registry);
        return (registry, executor);
    }

    private static InMemoryKeyValueStore<string, int> CreateKvStore(string name)
        => new(name);

    // -----------------------------------------------------------------------
    // ListStores
    // -----------------------------------------------------------------------

    [Fact]
    public void ListStores_EmptyRegistry_ReturnsEmptyList()
    {
        var registry = new StateStoreRegistry();

        var response = InteractiveQueryRestApi.ListStores(registry);

        Assert.NotNull(response);
        Assert.Empty(response.Stores);
        Assert.Equal(0, response.TotalCount);
    }

    [Fact]
    public void ListStores_WithRegisteredStores_ReturnsAllStores()
    {
        var registry = new StateStoreRegistry();
        registry.Register("store-a", CreateKvStore("store-a"));
        registry.Register("store-b", CreateKvStore("store-b"));

        var response = InteractiveQueryRestApi.ListStores(registry);

        Assert.Equal(2, response.TotalCount);
        Assert.Contains(response.Stores, s => s.Name == "store-a");
        Assert.Contains(response.Stores, s => s.Name == "store-b");
    }

    [Fact]
    public void ListStores_StoreMetadata_CorrectType()
    {
        var registry = new StateStoreRegistry();
        registry.Register("kv-store", CreateKvStore("kv-store"));

        var response = InteractiveQueryRestApi.ListStores(registry);

        var info = Assert.Single(response.Stores);
        Assert.Equal("kv-store", info.Name);
        Assert.Equal(StateStoreType.KeyValue, info.StoreType);
        Assert.False(info.Persistent);
    }

    // -----------------------------------------------------------------------
    // GetStoreInfo
    // -----------------------------------------------------------------------

    [Fact]
    public void GetStoreInfo_ExistingStore_ReturnsInfo()
    {
        var registry = new StateStoreRegistry();
        registry.Register("my-store", CreateKvStore("my-store"));

        var info = InteractiveQueryRestApi.GetStoreInfo("my-store", registry);

        Assert.NotNull(info);
        Assert.Equal("my-store", info.Name);
        Assert.Equal(StateStoreType.KeyValue, info.StoreType);
    }

    [Fact]
    public void GetStoreInfo_NonExistentStore_ReturnsNull()
    {
        var registry = new StateStoreRegistry();

        var info = InteractiveQueryRestApi.GetStoreInfo("missing", registry);

        Assert.Null(info);
    }

    // -----------------------------------------------------------------------
    // GetEntries (list / pagination)
    // -----------------------------------------------------------------------

    [Fact]
    public void GetEntries_NonExistentStore_ReturnsNull()
    {
        var (registry, executor) = CreateSutShared();

        var result = InteractiveQueryRestApi.GetEntries("missing", 0, 100, registry, executor);

        Assert.Null(result);
    }

    [Fact]
    public void GetEntries_EmptyStore_ReturnsEmptyPage()
    {
        var (registry, executor) = CreateSutShared();
        registry.Register("empty-store", CreateKvStore("empty-store"));

        var result = InteractiveQueryRestApi.GetEntries("empty-store", 0, 100, registry, executor);

        Assert.NotNull(result);
        Assert.Equal("empty-store", result.StoreName);
        Assert.Empty(result.Entries);
        Assert.Equal(0, result.TotalCount);
        Assert.Equal(0, result.Offset);
        Assert.Equal(100, result.Limit);
    }

    [Fact]
    public void GetEntries_WithEntries_ReturnsPaginatedPage()
    {
        var (registry, executor) = CreateSutShared();
        var store = CreateKvStore("paged-store");
        store.Put("key1", 10);
        store.Put("key2", 20);
        store.Put("key3", 30);
        registry.Register("paged-store", store);

        var result = InteractiveQueryRestApi.GetEntries("paged-store", 0, 100, registry, executor);

        Assert.NotNull(result);
        Assert.Equal(3, result.Entries.Count);
        Assert.Equal(3, result.TotalCount);
        Assert.False(result.HasMore);
    }

    [Fact]
    public void GetEntries_Pagination_RespectsOffsetAndLimit()
    {
        var (registry, executor) = CreateSutShared();
        var store = CreateKvStore("page-test");
        for (var i = 1; i <= 10; i++)
            store.Put($"key{i:D2}", i);
        registry.Register("page-test", store);

        var page = InteractiveQueryRestApi.GetEntries("page-test", offset: 3, limit: 4, registry, executor);

        Assert.NotNull(page);
        Assert.Equal(4, page.Entries.Count);
        Assert.Equal(3, page.Offset);
        Assert.Equal(4, page.Limit);
        Assert.Equal(10, page.TotalCount);
        Assert.True(page.HasMore);
    }

    // -----------------------------------------------------------------------
    // GetEntry (single key lookup)
    // -----------------------------------------------------------------------

    [Fact]
    public void GetEntry_NonExistentStore_ReturnsNull()
    {
        var (registry, executor) = CreateSutShared();

        var result = InteractiveQueryRestApi.GetEntry("missing", "key", registry, executor);

        Assert.Null(result);
    }

    [Fact]
    public void GetEntry_ExistingKey_ReturnsEntry()
    {
        var (registry, executor) = CreateSutShared();
        var store = CreateKvStore("lookup-store");
        store.Put("alpha", 42);
        registry.Register("lookup-store", store);

        var result = InteractiveQueryRestApi.GetEntry("lookup-store", "alpha", registry, executor);

        Assert.NotNull(result);
        Assert.Equal("lookup-store", result.StoreName);
        Assert.Equal("alpha", result.Key);
        Assert.NotNull(result.Value);
    }

    [Fact]
    public void GetEntry_NonExistentKey_InStringValueStore_ReturnsNull()
    {
        // String-value stores return null for missing keys (reference type default)
        var (registry, executor) = CreateSutShared();
        var store = new InMemoryKeyValueStore<string, string>("str-store");
        registry.Register("str-store", store);

        var result = InteractiveQueryRestApi.GetEntry("str-store", "ghost", registry, executor);

        Assert.Null(result);
    }

    // -----------------------------------------------------------------------
    // GetCount
    // -----------------------------------------------------------------------

    [Fact]
    public void GetCount_NonExistentStore_ReturnsNull()
    {
        var (registry, executor) = CreateSutShared();

        var result = InteractiveQueryRestApi.GetCount("missing", registry, executor);

        Assert.Null(result);
    }

    [Fact]
    public void GetCount_EmptyStore_ReturnsZero()
    {
        var (registry, executor) = CreateSutShared();
        registry.Register("count-store", CreateKvStore("count-store"));

        var result = InteractiveQueryRestApi.GetCount("count-store", registry, executor);

        Assert.NotNull(result);
        Assert.Equal("count-store", result.StoreName);
        Assert.Equal(0, result.ApproximateCount);
    }

    [Fact]
    public void GetCount_StoreWithEntries_ReturnsApproximateCount()
    {
        var (registry, executor) = CreateSutShared();
        var store = CreateKvStore("count-store-2");
        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);
        registry.Register("count-store-2", store);

        var result = InteractiveQueryRestApi.GetCount("count-store-2", registry, executor);

        Assert.NotNull(result);
        Assert.Equal(3, result.ApproximateCount);
    }

    // -----------------------------------------------------------------------
    // Registry lifecycle
    // -----------------------------------------------------------------------

    [Fact]
    public void Register_ThenUnregister_StoreDisappears()
    {
        var registry = new StateStoreRegistry();
        var store = CreateKvStore("temp-store");
        registry.Register("temp-store", store);

        var before = InteractiveQueryRestApi.ListStores(registry);
        Assert.Single(before.Stores);

        registry.Unregister("temp-store");

        var after = InteractiveQueryRestApi.ListStores(registry);
        Assert.Empty(after.Stores);
    }
}
