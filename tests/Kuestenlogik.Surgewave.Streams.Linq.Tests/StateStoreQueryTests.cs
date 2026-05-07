using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Linq;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Linq.Tests;

public class StateStoreQueryTests
{
    [Fact]
    public void AsQueryable_ReturnsQueryableFromStore()
    {
        var store = new InMemoryStore<string, int>();
        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);

        var results = store.AsQueryable()
            .Where(kv => kv.Value > 1)
            .ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void AsQueryable_WithRange_ReturnsSubset()
    {
        var store = new InMemoryStore<string, int>();
        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);

        var results = store.AsQueryable("a", "b").ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void AsQueryable_SupportsSelect()
    {
        var store = new InMemoryStore<string, int>();
        store.Put("x", 42);

        var values = store.AsQueryable()
            .Select(kv => kv.Value)
            .ToList();

        Assert.Single(values);
        Assert.Equal(42, values[0]);
    }

    [Fact]
    public void AsQueryable_SupportsCount()
    {
        var store = new InMemoryStore<string, int>();
        store.Put("a", 1);
        store.Put("b", 2);

        var count = store.AsQueryable().Count();

        Assert.Equal(2, count);
    }

    [Fact]
    public void AsQueryable_SupportsFirst()
    {
        var store = new InMemoryStore<string, int>();
        store.Put("a", 1);
        store.Put("b", 2);

        var first = store.AsQueryable().First();

        Assert.Equal("a", first.Key);
    }

    [Fact]
    public void AsQueryable_EmptyStore_ReturnsEmpty()
    {
        var store = new InMemoryStore<string, int>();

        var results = store.AsQueryable().ToList();

        Assert.Empty(results);
    }

    [Fact]
    public void AsQueryable_WhereNoMatch_ReturnsEmpty()
    {
        var store = new InMemoryStore<string, int>();
        store.Put("a", 1);

        var results = store.AsQueryable()
            .Where(kv => kv.Value > 100)
            .ToList();

        Assert.Empty(results);
    }
}

/// <summary>
/// Simple in-memory state store for testing.
/// </summary>
internal sealed class InMemoryStore<TKey, TValue> : IReadOnlyKeyValueStore<TKey, TValue>
    where TKey : notnull, IComparable<TKey>
{
    private readonly SortedDictionary<TKey, TValue> _data = new();

    public string Name => "test-store";
    public bool Persistent => false;

    public void Put(TKey key, TValue value) => _data[key] = value;
    public TValue? Get(TKey key) => _data.TryGetValue(key, out var value) ? value : default;

    public IEnumerable<KeyValue<TKey, TValue>> Range(TKey from, TKey to)
        => _data.Where(kv => kv.Key.CompareTo(from) >= 0 && kv.Key.CompareTo(to) <= 0)
            .Select(kv => new KeyValue<TKey, TValue>(kv.Key, kv.Value));

    public IEnumerable<KeyValue<TKey, TValue>> All()
        => _data.Select(kv => new KeyValue<TKey, TValue>(kv.Key, kv.Value));

    public long ApproximateNumEntries => _data.Count;

    public void Init(ProcessorContext context) { }
    public void Flush() { }
    public void Close() { }
    public void Dispose() { }
}
