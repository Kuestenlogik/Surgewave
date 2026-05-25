using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// Verifies the composition path for state stores: <see cref="IByteKeyValueBackend"/> +
/// <see cref="SerdeBackedKeyValueStore{TKey,TValue}"/>. A test-only byte-level backend
/// (no files, no sockets, no Surgewave runtime knowledge) is wrapped into a full generic
/// store, and the generic key/value API is exercised end-to-end.
///
/// <para>
/// This is the architectural justification for the composition path next to the inheritance
/// path: third parties can write ~50 lines of <see cref="IByteKeyValueBackend"/> and get a
/// full Surgewave state store with serialization, metrics and context integration — no generics,
/// no <see cref="ProcessorContext"/> knowledge, no base class coupling.
/// </para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SerdeBackedKeyValueStoreTests
{
    [Fact]
    public void PutGet_RoundTrip_ThroughBackend()
    {
        var backend = new DictionaryBackend();
        var store = new SerdeBackedKeyValueStore<string, int>(
            "counts", Serdes.String(), Serdes.Json<int>(), backend);

        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);

        Assert.Equal(1, store.Get("a"));
        Assert.Equal(2, store.Get("b"));
        Assert.Equal(3, store.Get("c"));
        Assert.Equal(0, store.Get("missing")); // default(int)
    }

    [Fact]
    public void PutIfAbsent_DoesNotOverwriteExistingValue()
    {
        var backend = new DictionaryBackend();
        var store = new SerdeBackedKeyValueStore<string, int>(
            "counts", Serdes.String(), Serdes.Json<int>(), backend);

        store.Put("a", 1);
        var existing = store.PutIfAbsent("a", 99);
        Assert.Equal(1, existing);
        Assert.Equal(1, store.Get("a"));

        var freshExisting = store.PutIfAbsent("b", 42);
        Assert.Equal(0, freshExisting); // default(int) since there was no existing
        Assert.Equal(42, store.Get("b"));
    }

    [Fact]
    public void Delete_ReturnsDeserializedPreviousValue()
    {
        var backend = new DictionaryBackend();
        var store = new SerdeBackedKeyValueStore<string, int>(
            "counts", Serdes.String(), Serdes.Json<int>(), backend);

        store.Put("a", 42);
        var deleted = store.Delete("a");
        Assert.Equal(42, deleted);
        Assert.Equal(0, store.Get("a")); // default(int) — key is gone
    }

    [Fact]
    public void PutAll_RoundTrips_AllEntries()
    {
        var backend = new DictionaryBackend();
        var store = new SerdeBackedKeyValueStore<string, int>(
            "counts", Serdes.String(), Serdes.Json<int>(), backend);

        store.PutAll(
        [
            new KeyValue<string, int>("a", 1),
            new KeyValue<string, int>("b", 2),
            new KeyValue<string, int>("c", 3)
        ]);

        Assert.Equal(1, store.Get("a"));
        Assert.Equal(2, store.Get("b"));
        Assert.Equal(3, store.Get("c"));
        Assert.Equal(3, store.ApproximateNumEntries);
    }

    [Fact]
    public void All_YieldsDeserializedEntries_FromBackend()
    {
        var backend = new DictionaryBackend();
        var store = new SerdeBackedKeyValueStore<string, int>(
            "counts", Serdes.String(), Serdes.Json<int>(), backend);

        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);

        var all = store.All().ToList();
        Assert.Equal(3, all.Count);
        Assert.Contains(all, kv => kv.Key == "a" && kv.Value == 1);
        Assert.Contains(all, kv => kv.Key == "b" && kv.Value == 2);
        Assert.Contains(all, kv => kv.Key == "c" && kv.Value == 3);
    }

    [Fact]
    public void ApproximateNumEntries_FlowsFromBackend()
    {
        var backend = new DictionaryBackend();
        var store = new SerdeBackedKeyValueStore<string, int>(
            "counts", Serdes.String(), Serdes.Json<int>(), backend);

        Assert.Equal(0, store.ApproximateNumEntries);
        store.Put("a", 1);
        Assert.Equal(1, store.ApproximateNumEntries);
        store.Put("b", 2);
        Assert.Equal(2, store.ApproximateNumEntries);
        store.Delete("a");
        Assert.Equal(1, store.ApproximateNumEntries);
    }

    [Fact]
    public void Persistent_Flag_CanBeOverridden()
    {
        var persistentStore = new SerdeBackedKeyValueStore<string, int>(
            "c1", Serdes.String(), Serdes.Json<int>(), new DictionaryBackend(), persistent: true);
        Assert.True(persistentStore.Persistent);

        var volatileStore = new SerdeBackedKeyValueStore<string, int>(
            "c2", Serdes.String(), Serdes.Json<int>(), new DictionaryBackend(), persistent: false);
        Assert.False(volatileStore.Persistent);
    }

    [Fact]
    public void Dispose_InvokesBackendDispose()
    {
        var backend = new DictionaryBackend();
        var store = new SerdeBackedKeyValueStore<string, int>(
            "counts", Serdes.String(), Serdes.Json<int>(), backend);

        store.Dispose();
        Assert.True(backend.Disposed);
    }

    // ────────────────────────────────────────────────────────────────────────
    // DictionaryBackend — a ~50-line IByteKeyValueBackend. Zero Surgewave runtime
    // dependencies. The entire point of this test class is to prove that a
    // backend this small gets lifted into a full IKeyValueStore<TKey,TValue>
    // via SerdeBackedKeyValueStore.
    // ────────────────────────────────────────────────────────────────────────
    private sealed class DictionaryBackend : IByteKeyValueBackend
    {
        private readonly SortedDictionary<byte[], byte[]> _store = new(ByteArrayLexComparer.Instance);

        public long ApproximateNumEntries => _store.Count;

        public bool Disposed { get; private set; }

        public void Open(BackendOpenContext context)
        {
            // Nothing to open — in-memory backend.
        }

        public byte[]? Get(byte[] keyBytes)
            => _store.TryGetValue(keyBytes, out var v) ? v : null;

        public bool Put(byte[] keyBytes, byte[] valueBytes)
        {
            var fresh = !_store.ContainsKey(keyBytes);
            _store[keyBytes] = valueBytes;
            return fresh;
        }

        public byte[]? Delete(byte[] keyBytes)
        {
            if (_store.TryGetValue(keyBytes, out var v))
            {
                _store.Remove(keyBytes);
                return v;
            }
            return null;
        }

        public IEnumerable<(byte[] key, byte[] value)> Range(byte[] fromBytes, byte[] toBytes)
        {
            foreach (var kv in _store)
            {
                if (ByteArrayLexComparer.Instance.Compare(kv.Key, fromBytes) < 0) continue;
                if (ByteArrayLexComparer.Instance.Compare(kv.Key, toBytes) > 0) break;
                yield return (kv.Key, kv.Value);
            }
        }

        public IEnumerable<(byte[] key, byte[] value)> All()
        {
            foreach (var kv in _store)
                yield return (kv.Key, kv.Value);
        }

        public void Flush() { }

        public void Dispose()
        {
            _store.Clear();
            Disposed = true;
        }

        private sealed class ByteArrayLexComparer : IComparer<byte[]>
        {
            public static readonly ByteArrayLexComparer Instance = new();
            public int Compare(byte[]? x, byte[]? y)
            {
                if (x is null || y is null) return (x is null ? 0 : 1) - (y is null ? 0 : 1);
                var len = Math.Min(x.Length, y.Length);
                for (var i = 0; i < len; i++)
                {
                    if (x[i] != y[i]) return x[i].CompareTo(y[i]);
                }
                return x.Length.CompareTo(y.Length);
            }
        }
    }
}
