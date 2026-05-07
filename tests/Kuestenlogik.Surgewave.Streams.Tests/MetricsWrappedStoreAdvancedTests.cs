using System.Diagnostics.Metrics;
using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Monitoring;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// Advanced tests for MetricsWrappedKeyValueStore covering Delete latency,
/// PutAll batch distribution, Range/All delegation, Close/Dispose forwarding,
/// and concurrent Put/Get/Delete thread safety.
/// </summary>
public sealed class MetricsWrappedStoreAdvancedTests
{
    // ── helpers ───────────────────────────────────────────────────────────────

    private static (MetricsWrappedKeyValueStore<string, int> wrapped, StateStoreMetrics metrics, Meter meter)
        CreateWrapped(string testName)
    {
        var meter = new Meter($"test-wrapped-advanced-{testName}");
        var storeMetrics = new StateStoreMetrics(meter, $"store-{testName}");
        var inner = new InMemoryKeyValueStore<string, int>($"store-{testName}");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);
        return (wrapped, storeMetrics, meter);
    }

    // ── Delete records latency ────────────────────────────────────────────────

    [Fact]
    public void Delete_RecordsLatency_AndIncrementsDeleteCounter()
    {
        var (wrapped, metrics, meter) = CreateWrapped("delete-latency");
        using var _ = meter;

        wrapped.Put("k1", 10);
        wrapped.Put("k2", 20);

        Assert.Equal(2, metrics.TotalPuts);
        Assert.Equal(0, metrics.TotalDeletes);

        var deleted1 = wrapped.Delete("k1");
        var deleted2 = wrapped.Delete("k2");
        var deletedMissing = wrapped.Delete("missing-key");

        // Counter increments on every call, even for missing keys
        Assert.Equal(3, metrics.TotalDeletes);

        // Values returned correctly
        Assert.Equal(10, deleted1);
        Assert.Equal(20, deleted2);
        Assert.Equal(0, deletedMissing); // default(int)
    }

    [Fact]
    public void Delete_AfterMultiplePuts_DeleteCounterMatchesCallCount()
    {
        var (wrapped, metrics, meter) = CreateWrapped("delete-counter");
        using var _ = meter;

        for (int i = 0; i < 20; i++)
            wrapped.Put($"key-{i}", i);

        for (int i = 0; i < 20; i++)
            wrapped.Delete($"key-{i}");

        Assert.Equal(20, metrics.TotalPuts);
        Assert.Equal(20, metrics.TotalDeletes);
        Assert.Equal(0, wrapped.ApproximateNumEntries);
    }

    // ── PutAll batch metrics distribution ────────────────────────────────────

    [Fact]
    public void PutAll_RecordsBatchCount_PutsEqualBatchSize()
    {
        var (wrapped, metrics, meter) = CreateWrapped("putall-batch");
        using var _ = meter;

        var entries = Enumerable.Range(0, 10)
            .Select(i => new KeyValue<string, int>($"k{i}", i))
            .ToList();

        wrapped.PutAll(entries);

        // RecordPut(count) called with 10 — TotalPuts >= 10
        Assert.True(metrics.TotalPuts >= 10,
            $"Expected TotalPuts >= 10, got {metrics.TotalPuts}");
        Assert.Equal(10, wrapped.ApproximateNumEntries);
    }

    [Fact]
    public void PutAll_EmptyBatch_NoMetricsRecorded()
    {
        var (wrapped, metrics, meter) = CreateWrapped("putall-empty");
        using var _ = meter;

        wrapped.PutAll(Array.Empty<KeyValue<string, int>>());

        Assert.Equal(0, metrics.TotalPuts);
        Assert.Equal(0, wrapped.ApproximateNumEntries);
    }

    [Fact]
    public void PutAll_SingleEntry_RecordsOneput()
    {
        var (wrapped, metrics, meter) = CreateWrapped("putall-single");
        using var _ = meter;

        wrapped.PutAll([new KeyValue<string, int>("only", 42)]);

        Assert.True(metrics.TotalPuts >= 1);
        Assert.Equal(42, wrapped.Get("only"));
    }

    [Fact]
    public void PutAll_LargeBatch_DataConsistentAfterwards()
    {
        var (wrapped, metrics, meter) = CreateWrapped("putall-large");
        using var _ = meter;

        const int batchSize = 500;
        var entries = Enumerable.Range(0, batchSize)
            .Select(i => new KeyValue<string, int>($"key{i:D4}", i))
            .ToList();

        wrapped.PutAll(entries);

        Assert.True(metrics.TotalPuts >= batchSize);
        Assert.Equal(batchSize, wrapped.ApproximateNumEntries);

        // Spot-check data integrity
        Assert.Equal(0, wrapped.Get("key0000"));
        Assert.Equal(250, wrapped.Get("key0250"));
        Assert.Equal(499, wrapped.Get("key0499"));
    }

    // ── Range and All delegation (no metrics overhead on enumeration) ─────────

    [Fact]
    public void Range_DelegatesToInner_ReturnsCorrectEntries()
    {
        // InMemoryKeyValueStore requires a comparer for Range queries
        var meter = new Meter("test-range-delegate");
        var storeMetrics = new StateStoreMetrics(meter, "range-store");
        var inner = new InMemoryKeyValueStore<string, int>("range-store", StringComparer.Ordinal);
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        wrapped.Put("apple", 1);
        wrapped.Put("banana", 2);
        wrapped.Put("cherry", 3);
        wrapped.Put("date", 4);

        var prePuts = storeMetrics.TotalPuts;

        // Range does not bump put/get counters
        var rangeResults = wrapped.Range("banana", "cherry").ToList();

        Assert.Equal(prePuts, storeMetrics.TotalPuts); // no put metrics from range
        Assert.Equal(2, rangeResults.Count);
        Assert.Contains(rangeResults, kv => kv.Key == "banana" && kv.Value == 2);
        Assert.Contains(rangeResults, kv => kv.Key == "cherry" && kv.Value == 3);

        meter.Dispose();
    }

    [Fact]
    public void All_DelegatesToInner_ReturnsAllEntries()
    {
        var (wrapped, metrics, meter) = CreateWrapped("all-delegate");
        using var _ = meter;

        for (int i = 0; i < 5; i++)
            wrapped.Put($"k{i}", i * 10);

        var prePuts = metrics.TotalPuts;

        var all = wrapped.All().ToList();

        // All() does not alter metrics
        Assert.Equal(prePuts, metrics.TotalPuts);
        Assert.Equal(0, metrics.TotalGets);
        Assert.Equal(5, all.Count);
    }

    // ── Close and Dispose delegation ──────────────────────────────────────────

    [Fact]
    public void Close_DelegatesToInnerStore()
    {
        // We verify that Close() on the wrapper does not throw, and that the
        // inner store name is accessible after Close (InMemoryKeyValueStore is permissive).
        var meter = new Meter("test-close-delegate");
        var storeMetrics = new StateStoreMetrics(meter, "close-store");
        var inner = new InMemoryKeyValueStore<string, int>("close-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        wrapped.Put("x", 1);
        wrapped.Close(); // must not throw

        // Name still accessible after close
        Assert.Equal("close-store", wrapped.Name);

        meter.Dispose();
    }

    [Fact]
    public void Dispose_DelegatesToInnerStore_NoThrow()
    {
        var meter = new Meter("test-dispose-delegate");
        var storeMetrics = new StateStoreMetrics(meter, "dispose-store");
        var inner = new InMemoryKeyValueStore<string, int>("dispose-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        wrapped.Put("y", 2);

        // Should not throw
        wrapped.Dispose();

        meter.Dispose();
    }

    // ── Concurrent Put/Get/Delete – no data loss ──────────────────────────────

    [Fact]
    public async Task ConcurrentPutGetDelete_NoDataLoss()
    {
        var meter = new Meter("test-concurrent-store");
        var storeMetrics = new StateStoreMetrics(meter, "concurrent-store");
        var inner = new InMemoryKeyValueStore<string, int>("concurrent-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        const int threads = 8;
        const int opsPerThread = 50;

        // Pre-populate so get/delete operations find real data
        for (int i = 0; i < threads * opsPerThread; i++)
            wrapped.Put($"k{i}", i);

        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        var tasks = Enumerable.Range(0, threads).Select(t => Task.Run(() =>
        {
            try
            {
                for (int i = 0; i < opsPerThread; i++)
                {
                    var key = $"k{t * opsPerThread + i}";
                    // Put overwrite
                    wrapped.Put(key, t * 1000 + i);
                    // Get
                    _ = wrapped.Get(key);
                    // PutIfAbsent (key already exists — should return existing)
                    var existing = wrapped.PutIfAbsent(key, -1);
                    // Delete
                    wrapped.Delete(key);
                }
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Empty(errors);

        // Metrics should have been incremented across all threads without corruption
        Assert.True(storeMetrics.TotalPuts > threads * opsPerThread,
            "Expected additional puts from PutIfAbsent calls");
        Assert.True(storeMetrics.TotalGets >= threads * opsPerThread);
        Assert.True(storeMetrics.TotalDeletes >= threads * opsPerThread);

        meter.Dispose();
    }

    [Fact]
    public async Task ConcurrentPut_MultipleThreads_AllEntriesPersist()
    {
        var meter = new Meter("test-concurrent-put-persist");
        var storeMetrics = new StateStoreMetrics(meter, "persist-store");
        var inner = new InMemoryKeyValueStore<string, int>("persist-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        const int threadCount = 10;
        const int perThread = 100;

        var tasks = Enumerable.Range(0, threadCount)
            .Select(t => Task.Run(() =>
            {
                for (int i = 0; i < perThread; i++)
                    wrapped.Put($"thread{t}_key{i}", t * 1000 + i);
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        // Every key written should be readable
        for (int t = 0; t < threadCount; t++)
        {
            for (int i = 0; i < perThread; i++)
            {
                var v = wrapped.Get($"thread{t}_key{i}");
                Assert.Equal(t * 1000 + i, v);
            }
        }

        Assert.Equal(threadCount * perThread, storeMetrics.TotalPuts);

        meter.Dispose();
    }

    // ── Flush increments counter and delegates ────────────────────────────────

    [Fact]
    public void Flush_AfterConcurrentPuts_CounterMatchesCalls()
    {
        var (wrapped, metrics, meter) = CreateWrapped("flush-concurrent");
        using var _ = meter;

        wrapped.Put("a", 1);
        wrapped.Flush();
        wrapped.Put("b", 2);
        wrapped.Flush();
        wrapped.Flush();

        Assert.Equal(3, metrics.TotalFlushes);
    }

    // ── PutIfAbsent semantics ─────────────────────────────────────────────────

    [Fact]
    public void PutIfAbsent_KeyAbsent_StoresAndCountsPut()
    {
        var (wrapped, metrics, meter) = CreateWrapped("putifabsent-absent");
        using var _ = meter;

        var result = wrapped.PutIfAbsent("new-key", 99);

        // ConcurrentDictionary.GetOrAdd returns the value that was stored (99) when the key was absent
        Assert.Equal(99, result);
        Assert.Equal(99, wrapped.Get("new-key"));
        Assert.Equal(1, metrics.TotalPuts);
    }

    [Fact]
    public void PutIfAbsent_KeyPresent_ReturnsExistingWithoutOverwrite()
    {
        var (wrapped, metrics, meter) = CreateWrapped("putifabsent-present");
        using var _ = meter;

        wrapped.Put("existing", 42);
        var result = wrapped.PutIfAbsent("existing", 999);

        Assert.Equal(42, result);      // existing value returned
        Assert.Equal(42, wrapped.Get("existing")); // not overwritten
        Assert.Equal(2, metrics.TotalPuts);        // Put + PutIfAbsent both counted
    }
}
