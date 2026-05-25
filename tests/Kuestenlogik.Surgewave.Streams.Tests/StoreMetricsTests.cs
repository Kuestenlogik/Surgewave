using System.Diagnostics.Metrics;
using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Monitoring;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// Tests verifying per-store metrics instrumentation via StateStoreMetrics,
/// WindowStoreMetrics, SessionStoreMetrics, and MetricsWrappedKeyValueStore.
/// </summary>
public sealed class StoreMetricsTests
{
    // ── StateStoreMetrics ────────────────────────────────────────────────────

    [Fact]
    public void StateStoreMetrics_RecordFlush_IncrementsCounter()
    {
        using var meter = new Meter("test-flush");
        var metrics = new StateStoreMetrics(meter, "flush-store");

        metrics.RecordFlush();
        metrics.RecordFlush(1.5);

        Assert.Equal(2, metrics.TotalFlushes);
    }

    [Fact]
    public void StateStoreMetrics_RecordRestore_AccumulatesCount()
    {
        using var meter = new Meter("test-restore");
        var metrics = new StateStoreMetrics(meter, "restore-store");

        metrics.RecordRestore(100);
        metrics.RecordRestore(50);

        Assert.Equal(150, metrics.TotalRestored);
    }

    [Fact]
    public void StateStoreMetrics_RecordFlush_ZeroLatency_DoesNotThrow()
    {
        using var meter = new Meter("test-flush-zero");
        var metrics = new StateStoreMetrics(meter, "flush-zero-store");

        // Latency of 0 means "skip histogram recording" — should not throw
        metrics.RecordFlush(0);

        Assert.Equal(1, metrics.TotalFlushes);
    }

    // ── ProcessorNodeMetrics ─────────────────────────────────────────────────

    [Fact]
    public void ProcessorNodeMetrics_RecordSkipped_IncrementsCounter()
    {
        using var meter = new Meter("test-skipped");
        var metrics = new ProcessorNodeMetrics(meter, "filter-node");

        metrics.RecordSkipped();
        metrics.RecordSkipped(4);

        Assert.Equal(5, metrics.TotalSkipped);
    }

    [Fact]
    public void ProcessorNodeMetrics_RecordSkipped_IndependentOfOtherCounters()
    {
        using var meter = new Meter("test-skipped-independent");
        var metrics = new ProcessorNodeMetrics(meter, "filter-node-2");

        metrics.RecordIn(10);
        metrics.RecordOut(7);
        metrics.RecordSkipped(3);

        Assert.Equal(10, metrics.TotalIn);
        Assert.Equal(7, metrics.TotalOut);
        Assert.Equal(3, metrics.TotalSkipped);
        Assert.Equal(0, metrics.TotalErrors);
    }

    // ── WindowStoreMetrics ───────────────────────────────────────────────────

    [Fact]
    public void WindowStoreMetrics_RecordPut_IncrementsCounter()
    {
        using var meter = new Meter("test-window-put");
        var metrics = new WindowStoreMetrics(meter, "window-store");

        metrics.RecordPut();
        metrics.RecordPut(0.5);

        Assert.Equal(2, metrics.TotalPuts);
    }

    [Fact]
    public void WindowStoreMetrics_RecordFetch_IncrementsCounter()
    {
        using var meter = new Meter("test-window-fetch");
        var metrics = new WindowStoreMetrics(meter, "window-store");

        metrics.RecordFetch();
        metrics.RecordFetch(1.2);

        Assert.Equal(2, metrics.TotalFetches);
    }

    [Fact]
    public void WindowStoreMetrics_RecordExpired_AccumulatesCount()
    {
        using var meter = new Meter("test-window-expired");
        var metrics = new WindowStoreMetrics(meter, "window-store");

        metrics.RecordExpired(3);
        metrics.RecordExpired(7);

        Assert.Equal(10, metrics.TotalExpired);
    }

    [Fact]
    public void WindowStoreMetrics_StoreName_IsPreserved()
    {
        using var meter = new Meter("test-window-name");
        var metrics = new WindowStoreMetrics(meter, "my-window-store");

        Assert.Equal("my-window-store", metrics.StoreName);
    }

    // ── SessionStoreMetrics ──────────────────────────────────────────────────

    [Fact]
    public void SessionStoreMetrics_RecordPut_IncrementsCounter()
    {
        using var meter = new Meter("test-session-put");
        var metrics = new SessionStoreMetrics(meter, "session-store");

        metrics.RecordPut();
        metrics.RecordPut(2.0);

        Assert.Equal(2, metrics.TotalPuts);
    }

    [Fact]
    public void SessionStoreMetrics_RecordFetch_IncrementsCounter()
    {
        using var meter = new Meter("test-session-fetch");
        var metrics = new SessionStoreMetrics(meter, "session-store");

        metrics.RecordFetch();

        Assert.Equal(1, metrics.TotalFetches);
    }

    [Fact]
    public void SessionStoreMetrics_RecordRemove_IncrementsCounter()
    {
        using var meter = new Meter("test-session-remove");
        var metrics = new SessionStoreMetrics(meter, "session-store");

        metrics.RecordRemove();
        metrics.RecordRemove();

        Assert.Equal(2, metrics.TotalRemoves);
    }

    [Fact]
    public void SessionStoreMetrics_RecordMerge_IncrementsCounter()
    {
        using var meter = new Meter("test-session-merge");
        var metrics = new SessionStoreMetrics(meter, "session-store");

        metrics.RecordMerge();

        Assert.Equal(1, metrics.TotalMerges);
    }

    [Fact]
    public void SessionStoreMetrics_StoreName_IsPreserved()
    {
        using var meter = new Meter("test-session-name");
        var metrics = new SessionStoreMetrics(meter, "my-session-store");

        Assert.Equal("my-session-store", metrics.StoreName);
    }

    // ── MetricsWrappedKeyValueStore ──────────────────────────────────────────

    [Fact]
    public void MetricsWrapped_Put_InstrumentsCounter()
    {
        using var meter = new Meter("test-wrapped-put");
        var storeMetrics = new StateStoreMetrics(meter, "wrapped-store");
        var inner = new InMemoryKeyValueStore<string, int>("wrapped-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        wrapped.Put("k1", 1);
        wrapped.Put("k2", 2);

        Assert.Equal(2, storeMetrics.TotalPuts);
    }

    [Fact]
    public void MetricsWrapped_Get_InstrumentsCounter()
    {
        using var meter = new Meter("test-wrapped-get");
        var storeMetrics = new StateStoreMetrics(meter, "wrapped-store");
        var inner = new InMemoryKeyValueStore<string, int>("wrapped-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        wrapped.Put("k1", 42);
        var value = wrapped.Get("k1");

        Assert.Equal(42, value);
        Assert.Equal(1, storeMetrics.TotalGets);
    }

    [Fact]
    public void MetricsWrapped_Delete_InstrumentsCounter()
    {
        using var meter = new Meter("test-wrapped-delete");
        var storeMetrics = new StateStoreMetrics(meter, "wrapped-store");
        var inner = new InMemoryKeyValueStore<string, int>("wrapped-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        wrapped.Put("k1", 1);
        wrapped.Delete("k1");

        Assert.Equal(1, storeMetrics.TotalDeletes);
    }

    [Fact]
    public void MetricsWrapped_Flush_InstrumentsCounter()
    {
        using var meter = new Meter("test-wrapped-flush");
        var storeMetrics = new StateStoreMetrics(meter, "wrapped-store");
        var inner = new InMemoryKeyValueStore<string, int>("wrapped-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        wrapped.Flush();
        wrapped.Flush();

        Assert.Equal(2, storeMetrics.TotalFlushes);
    }

    [Fact]
    public void MetricsWrapped_PutAll_InstrumentsBatchCount()
    {
        using var meter = new Meter("test-wrapped-putall");
        var storeMetrics = new StateStoreMetrics(meter, "wrapped-store");
        var inner = new InMemoryKeyValueStore<string, int>("wrapped-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        var entries = new[]
        {
            new KeyValue<string, int>("a", 1),
            new KeyValue<string, int>("b", 2),
            new KeyValue<string, int>("c", 3),
        };

        wrapped.PutAll(entries);

        // Batch put records count >= 3 (3 from RecordPut(count) + possibly 1 latency put)
        Assert.True(storeMetrics.TotalPuts >= 3);
    }

    [Fact]
    public void MetricsWrapped_DelegatesCorrectly_Get_ReturnsDelegatedValue()
    {
        using var meter = new Meter("test-wrapped-delegate");
        var storeMetrics = new StateStoreMetrics(meter, "wrapped-store");
        var inner = new InMemoryKeyValueStore<string, string>("wrapped-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, string>(inner, storeMetrics);

        wrapped.Put("hello", "world");
        var result = wrapped.Get("hello");

        Assert.Equal("world", result);
    }

    [Fact]
    public void MetricsWrapped_Name_DelegatesFromInner()
    {
        using var meter = new Meter("test-wrapped-name");
        var storeMetrics = new StateStoreMetrics(meter, "my-inner-store");
        var inner = new InMemoryKeyValueStore<string, int>("my-inner-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        Assert.Equal("my-inner-store", wrapped.Name);
        Assert.False(wrapped.Persistent);
    }

    [Fact]
    public void MetricsWrapped_ApproximateNumEntries_DelegatesFromInner()
    {
        using var meter = new Meter("test-wrapped-count");
        var storeMetrics = new StateStoreMetrics(meter, "count-store");
        var inner = new InMemoryKeyValueStore<string, int>("count-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        wrapped.Put("a", 1);
        wrapped.Put("b", 2);

        Assert.Equal(2, wrapped.ApproximateNumEntries);
    }

    [Fact]
    public void MetricsWrapped_PutIfAbsent_InstrumentsAndDelegates()
    {
        using var meter = new Meter("test-wrapped-putifabsent");
        var storeMetrics = new StateStoreMetrics(meter, "absent-store");
        var inner = new InMemoryKeyValueStore<string, int>("absent-store");
        var wrapped = new MetricsWrappedKeyValueStore<string, int>(inner, storeMetrics);

        wrapped.Put("k1", 10);
        var result = wrapped.PutIfAbsent("k1", 99);

        // PutIfAbsent returns existing value
        Assert.Equal(10, result);
        // Two puts recorded: one from Put, one from PutIfAbsent
        Assert.Equal(2, storeMetrics.TotalPuts);
    }

    [Fact]
    public void MetricsWrapped_NullInner_ThrowsArgumentNullException()
    {
        using var meter = new Meter("test-wrapped-null");
        var storeMetrics = new StateStoreMetrics(meter, "null-store");

        Assert.Throws<ArgumentNullException>(() =>
            new MetricsWrappedKeyValueStore<string, int>(null!, storeMetrics));
    }

    [Fact]
    public void MetricsWrapped_NullMetrics_ThrowsArgumentNullException()
    {
        var inner = new InMemoryKeyValueStore<string, int>("null-metrics-store");

        Assert.Throws<ArgumentNullException>(() =>
            new MetricsWrappedKeyValueStore<string, int>(inner, null!));
    }

    // ── StreamsMetrics registry ──────────────────────────────────────────────

    [Fact]
    public void StreamsMetrics_GetOrCreateStoreMetrics_ReturnsSameInstance()
    {
        using var metrics = new StreamsMetrics();

        var first = metrics.GetOrCreateStoreMetrics("shared-store");
        var second = metrics.GetOrCreateStoreMetrics("shared-store");

        Assert.Same(first, second);
    }
}
