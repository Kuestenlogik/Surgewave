using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Processors;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// Deep persistence tests for PersistentForeignKeySubscriptionStore and ForeignKeySubscriptionStore,
/// covering large fan-out round-trips, concurrent subscribe/unsubscribe, subscription updates,
/// and cache coherence with the backing store.
/// </summary>
public sealed class ForeignKeyPersistenceTests
{
    // ── helpers ───────────────────────────────────────────────────────────────

    private static InMemoryKeyValueStore<string, string> NewBacking(string name)
        => new(name);

    private static PersistentForeignKeySubscriptionStore<string, string> NewPersistent(
        IKeyValueStore<string, string> backing)
        => new(backing, pk => pk, fk => fk, s => s, s => s);

    // ── Large fan-out persistence: 1 FK → 500 PKs, serialize/deserialize round-trip ──

    [Fact]
    public void LargeFanOut_500Pks_RoundTripThroughBackingStore()
    {
        const int fanOut = 500;
        var backing = NewBacking("large-fanout");

        // Write via store1
        using var store1 = NewPersistent(backing);
        for (int i = 0; i < fanOut; i++)
            store1.Subscribe($"pk-{i}", "fk-shared");

        Assert.Equal(fanOut, store1.Count);

        var subs1 = store1.GetSubscribers("fk-shared");
        Assert.Equal(fanOut, subs1.Count);

        // Verify all PKs are in the set
        for (int i = 0; i < fanOut; i++)
            Assert.Contains($"pk-{i}", subs1);

        // Read back via a fresh store instance (cold cache)
        using var store2 = NewPersistent(backing);
        var subs2 = store2.GetSubscribers("fk-shared");

        Assert.Equal(fanOut, subs2.Count);
        for (int i = 0; i < fanOut; i++)
            Assert.Contains($"pk-{i}", subs2);
    }

    [Fact]
    public void LargeFanOut_BackingStoreContainsSerializedPkSet()
    {
        const int fanOut = 50;
        var backing = NewBacking("serialized-set");

        using var store = NewPersistent(backing);
        for (int i = 0; i < fanOut; i++)
            store.Subscribe($"pk-{i}", "fk-X");

        // The backing store must contain a "fk:fk-X" key with JSON-serialized set
        var raw = backing.Get("fk:fk-X");
        Assert.NotNull(raw);
        Assert.Contains("pk-0", raw);
        Assert.Contains("pk-49", raw);

        // Each PK has its own reverse-mapping key
        for (int i = 0; i < fanOut; i++)
        {
            var pkMapping = backing.Get($"pk:pk-{i}");
            Assert.Equal("fk-X", pkMapping);
        }
    }

    [Fact]
    public void LargeFanOut_AfterUnsubscribeAll_BackingStoreEmpty()
    {
        const int fanOut = 100;
        var backing = NewBacking("unsubscribe-all");

        using var store = NewPersistent(backing);
        for (int i = 0; i < fanOut; i++)
            store.Subscribe($"pk-{i}", "fk-Y");

        Assert.Equal(fanOut, store.Count);

        for (int i = 0; i < fanOut; i++)
            store.Unsubscribe($"pk-{i}", "fk-Y");

        Assert.Equal(0, store.Count);

        // All backing store entries must be gone
        var fkEntry = backing.Get("fk:fk-Y");
        Assert.Null(fkEntry);

        for (int i = 0; i < fanOut; i++)
        {
            var pkEntry = backing.Get($"pk:pk-{i}");
            Assert.Null(pkEntry);
        }
    }

    // ── Concurrent subscribe/unsubscribe from 10 threads ─────────────────────

    [Fact]
    public async Task ConcurrentSubscribeUnsubscribe_10Threads_NoDeadlockOrDataCorruption()
    {
        using var store = new ForeignKeySubscriptionStore<string, string>();

        const int threads = 10;
        const int opsPerThread = 100;

        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        var tasks = Enumerable.Range(0, threads).Select(t => Task.Run(() =>
        {
            try
            {
                for (int i = 0; i < opsPerThread; i++)
                {
                    var pk = $"thread{t}_pk{i}";
                    var fk = $"fk{i % 5}"; // 5 distinct FKs

                    store.Subscribe(pk, fk);
                    _ = store.GetSubscribers(fk);
                    _ = store.GetForeignKey(pk);

                    if (i % 2 == 0)
                        store.Unsubscribe(pk, fk);
                }
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }
        })).ToArray();

        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Empty(errors);
    }

    [Fact]
    public async Task ConcurrentSubscribeUnsubscribe_Persistent_NoDeadlock()
    {
        var backing = NewBacking("concurrent-persistent");
        using var store = NewPersistent(backing);

        const int threads = 10;
        const int opsPerThread = 50;
        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        var tasks = Enumerable.Range(0, threads).Select(t => Task.Run(() =>
        {
            try
            {
                for (int i = 0; i < opsPerThread; i++)
                {
                    var pk = $"t{t}_pk{i}";
                    var fk = $"fk-{i % 3}";

                    store.Subscribe(pk, fk);
                    _ = store.GetSubscribers(fk);
                    store.Unsubscribe(pk, fk);
                }
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }
        })).ToArray();

        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Empty(errors);
    }

    // ── UpdateSubscription: change FK for existing PK ─────────────────────────

    [Fact]
    public void UpdateSubscription_ChangesForeignKey_OldFkEmpty_NewFkPopulated()
    {
        using var store = new ForeignKeySubscriptionStore<string, string>();

        store.Subscribe("pk-1", "fk-old");
        store.Subscribe("pk-2", "fk-old");

        Assert.Equal(2, store.GetSubscribers("fk-old").Count);
        Assert.Empty(store.GetSubscribers("fk-new"));

        store.UpdateSubscription("pk-1", "fk-old", "fk-new");

        Assert.Single(store.GetSubscribers("fk-old")); // only pk-2 remains
        Assert.Contains("pk-2", store.GetSubscribers("fk-old"));

        Assert.Single(store.GetSubscribers("fk-new")); // pk-1 moved
        Assert.Contains("pk-1", store.GetSubscribers("fk-new"));
    }

    [Fact]
    public void UpdateSubscription_SameFk_NoOp()
    {
        using var store = new ForeignKeySubscriptionStore<string, string>();

        store.Subscribe("pk-A", "fk-X");

        // UpdateSubscription with the same FK should be a no-op / still subscribed
        store.UpdateSubscription("pk-A", "fk-X", "fk-X");

        Assert.Single(store.GetSubscribers("fk-X"));
        Assert.Contains("pk-A", store.GetSubscribers("fk-X"));
        Assert.Equal("fk-X", store.GetForeignKey("pk-A"));
    }

    [Fact]
    public void UpdateSubscription_Persistent_OldFkRemovedFromBackingStore()
    {
        var backing = NewBacking("update-sub-backing");
        using var store = NewPersistent(backing);

        store.Subscribe("order-1", "cust-old");
        Assert.NotNull(backing.Get("fk:cust-old"));
        Assert.Equal("cust-old", backing.Get("pk:order-1"));

        store.UpdateSubscription("order-1", "cust-old", "cust-new");

        // Old FK entry must be removed (no subscribers left)
        Assert.Null(backing.Get("fk:cust-old"));

        // New FK entry must exist with pk order-1
        Assert.NotNull(backing.Get("fk:cust-new"));
        Assert.Equal("cust-new", backing.Get("pk:order-1"));
    }

    [Fact]
    public void UpdateSubscription_Persistent_MultipleUpdateCycles_DataConsistent()
    {
        var backing = NewBacking("update-cycles");
        using var store = NewPersistent(backing);

        store.Subscribe("pk-X", "fk-1");

        for (int i = 1; i <= 10; i++)
        {
            var oldFk = $"fk-{i}";
            var newFk = $"fk-{i + 1}";
            store.UpdateSubscription("pk-X", oldFk, newFk);
        }

        // After 10 updates, pk-X should be under fk-11
        var subs = store.GetSubscribers("fk-11");
        Assert.Single(subs);
        Assert.Contains("pk-X", subs);

        // All old FKs should have no subscribers in backing
        for (int i = 1; i <= 10; i++)
            Assert.Null(backing.Get($"fk:fk-{i}"));
    }

    // ── Cache coherence: direct backing store modification reflected in reads ──

    [Fact]
    public void CacheCoherence_DirectBackingStoreWrite_ReflectedAfterCacheMiss()
    {
        var backing = NewBacking("cache-coherence");

        // Write via store1 to populate the backing store
        using var store1 = NewPersistent(backing);
        store1.Subscribe("pk-1", "fk-A");
        store1.Subscribe("pk-2", "fk-A");

        // Create store2 with cold cache — reads must fall through to backing
        using var store2 = NewPersistent(backing);

        // First read populates cache from backing store
        var subs = store2.GetSubscribers("fk-A");
        Assert.Equal(2, subs.Count);
        Assert.Contains("pk-1", subs);
        Assert.Contains("pk-2", subs);
    }

    [Fact]
    public void CacheCoherence_WriteViaStoreModifiesBacking_SecondStoreReadsUpdated()
    {
        var backing = NewBacking("cache-write-through");

        // store1 subscribes pk-1 → fk-A
        using var store1 = NewPersistent(backing);
        store1.Subscribe("pk-1", "fk-A");

        // store2 (cold cache) reads — gets pk-1
        using var store2 = NewPersistent(backing);
        var subs = store2.GetSubscribers("fk-A");
        Assert.Contains("pk-1", subs);

        // store1 adds pk-2 → fk-A (write-through)
        store1.Subscribe("pk-2", "fk-A");

        // store3 (freshly cold) reads both PKs from backing
        using var store3 = NewPersistent(backing);
        var fresh = store3.GetSubscribers("fk-A");
        Assert.Equal(2, fresh.Count);
        Assert.Contains("pk-1", fresh);
        Assert.Contains("pk-2", fresh);
    }

    [Fact]
    public void CacheCoherence_GetForeignKey_CacheMiss_LoadsFromBacking()
    {
        var backing = NewBacking("cache-getfk");

        using var store1 = NewPersistent(backing);
        store1.Subscribe("order-7", "customer-Z");

        // store2: cold cache, must read PK→FK from backing
        using var store2 = NewPersistent(backing);
        var fk = store2.GetForeignKey("order-7");
        Assert.Equal("customer-Z", fk);
    }

    // ── Count reflects current subscriptions ─────────────────────────────────

    [Fact]
    public void Persistent_Count_ReflectsSubscribeAndUnsubscribeOperations()
    {
        var backing = NewBacking("count-ops");
        using var store = NewPersistent(backing);

        Assert.Equal(0, store.Count);

        for (int i = 0; i < 10; i++)
            store.Subscribe($"pk-{i}", "fk-shared");

        Assert.Equal(10, store.Count);

        for (int i = 0; i < 5; i++)
            store.Unsubscribe($"pk-{i}", "fk-shared");

        Assert.Equal(5, store.Count);
    }

    [Fact]
    public void InMemory_Dispose_CalledTwice_DoesNotThrow()
    {
        var store = new ForeignKeySubscriptionStore<string, string>();
        store.Subscribe("pk-1", "fk-1");

        store.Dispose();
        store.Dispose(); // second dispose must be safe
    }

    [Fact]
    public void Persistent_Dispose_CalledTwice_DoesNotThrow()
    {
        var backing = NewBacking("dispose-twice");
        var store = NewPersistent(backing);
        store.Subscribe("pk-1", "fk-1");

        store.Dispose();
        store.Dispose(); // second dispose must be safe
    }
}
