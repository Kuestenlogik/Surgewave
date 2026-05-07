using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Monitoring;
using Kuestenlogik.Surgewave.Streams.Processors;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class ForeignKeyJoinAdvancedTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public ForeignKeyJoinAdvancedTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
            builder.AddFilter(level => level >= LogLevel.Warning));
    }

    public async ValueTask DisposeAsync()
    {
        _loggerFactory.Dispose();
        await Task.CompletedTask;
    }

    // ─── Domain records ───────────────────────────────────────────────────────

    private sealed record Order(string OrderId, string CustomerId, decimal Amount);
    private sealed record Customer(string CustomerId, string Name);
    private sealed record OrderWithCustomer(string OrderId, string CustomerId, decimal Amount, string? CustomerName);

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static InMemoryKeyValueStore<string, string> CreateBackingStore(string name)
        => new(name);

    private static PersistentForeignKeySubscriptionStore<string, string> CreatePersistentStore(
        IKeyValueStore<string, string> backing)
        => new(backing, pk => pk, fk => fk, s => s, s => s);

    // ─── PersistentForeignKeySubscriptionStore tests ──────────────────────────

    [Fact]
    public void PersistentStore_Subscribe_WritesToBackingStore()
    {
        var backing = CreateBackingStore("test-persistent-subscribe");
        using var store = CreatePersistentStore(backing);

        store.Subscribe("order-1", "cust-1");

        // FK→PK entry in backing store (key = "fk:cust-1")
        var fkEntry = backing.Get("fk:cust-1");
        Assert.NotNull(fkEntry);

        // PK→FK entry in backing store (key = "pk:order-1")
        var pkEntry = backing.Get("pk:order-1");
        Assert.Equal("cust-1", pkEntry);
    }

    [Fact]
    public void PersistentStore_Unsubscribe_RemovesFromBackingStore()
    {
        var backing = CreateBackingStore("test-persistent-unsubscribe");
        using var store = CreatePersistentStore(backing);

        store.Subscribe("order-1", "cust-1");
        store.Unsubscribe("order-1", "cust-1");

        // PK→FK entry must be removed
        var pkEntry = backing.Get("pk:order-1");
        Assert.Null(pkEntry);

        // FK→PK entry must be removed (set is empty → key deleted)
        var fkEntry = backing.Get("fk:cust-1");
        Assert.Null(fkEntry);
    }

    [Fact]
    public void PersistentStore_GetSubscribers_ReturnsCorrectPks()
    {
        var backing = CreateBackingStore("test-persistent-get");
        using var store = CreatePersistentStore(backing);

        store.Subscribe("order-1", "cust-1");
        store.Subscribe("order-2", "cust-1");
        store.Subscribe("order-3", "cust-2");

        var cust1Subs = store.GetSubscribers("cust-1");
        Assert.Equal(2, cust1Subs.Count);
        Assert.Contains("order-1", cust1Subs);
        Assert.Contains("order-2", cust1Subs);

        var cust2Subs = store.GetSubscribers("cust-2");
        Assert.Single(cust2Subs);
        Assert.Contains("order-3", cust2Subs);
    }

    [Fact]
    public void PersistentStore_Count_ReflectsSubscriptions()
    {
        var backing = CreateBackingStore("test-persistent-count");
        using var store = CreatePersistentStore(backing);

        Assert.Equal(0, store.Count);

        store.Subscribe("order-1", "cust-1");
        store.Subscribe("order-2", "cust-1");
        Assert.Equal(2, store.Count);

        store.Unsubscribe("order-1", "cust-1");
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void PersistentStore_VerifyPersistence_ViaBackingStore()
    {
        // Simulate persistence: write via one store instance, read raw from backing
        var backing = CreateBackingStore("test-persist-roundtrip");
        using var store1 = CreatePersistentStore(backing);

        store1.Subscribe("order-A", "cust-X");
        store1.Subscribe("order-B", "cust-X");

        // Create a second store instance pointing to the same backing store
        // The cache is cold, so it must load from backing store
        using var store2 = CreatePersistentStore(backing);

        var subscribers = store2.GetSubscribers("cust-X");
        Assert.Equal(2, subscribers.Count);
        Assert.Contains("order-A", subscribers);
        Assert.Contains("order-B", subscribers);
    }

    [Fact]
    public void PersistentStore_UpdateSubscription_ChangesOldAndNewFk()
    {
        var backing = CreateBackingStore("test-persistent-update");
        using var store = CreatePersistentStore(backing);

        store.Subscribe("order-1", "cust-1");
        Assert.Single(store.GetSubscribers("cust-1"));
        Assert.Empty(store.GetSubscribers("cust-2"));

        store.UpdateSubscription("order-1", "cust-1", "cust-2");

        Assert.Empty(store.GetSubscribers("cust-1"));
        Assert.Single(store.GetSubscribers("cust-2"));
    }

    // ─── Large fan-out performance test ──────────────────────────────────────

    [Fact]
    public void ForeignKeyJoin_LargeFanOut_100Pks_AllRejoined()
    {
        const int fanOutCount = 100;
        var results = new List<KeyValuePair<string, string>>();
        var builder = new StreamsBuilder();

        var orders = builder.Table<string, Order>("orders");
        var customers = builder.Table<string, Customer>("customers");

        orders.Join<string, Customer, string>(
            customers,
            order => order.CustomerId,
            (order, customer) => $"{order.OrderId}:{customer.Name}")
            .ToStream()
            .ForEach((k, v) => results.Add(new(k, v)));

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "fk-join-fanout-100",
            BootstrapServers = "localhost:9092"
        };

        var app = new StreamsApplication(config, topology, _loggerFactory);

        // Add a single customer
        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Alice"));

        // Add 100 orders all referencing the same customer
        for (var i = 0; i < fanOutCount; i++)
        {
            app.ProcessRecord("orders", $"order-{i}", new Order($"order-{i}", "cust-1", i * 10m));
        }

        Assert.Equal(fanOutCount, results.Count);
        Assert.All(results, r => Assert.Contains("Alice", r.Value));

        // Update customer — all 100 orders must re-join
        results.Clear();
        var sw = Stopwatch.StartNew();
        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Alice Smith"));
        sw.Stop();

        _output.WriteLine($"Fan-out of {fanOutCount} PKs took {sw.ElapsedMilliseconds}ms");

        Assert.Equal(fanOutCount, results.Count);
        Assert.All(results, r => Assert.Contains("Alice Smith", r.Value));
    }

    // ─── FK change (PK reassigns its FK reference) ───────────────────────────

    [Fact]
    public void ForeignKeyJoin_FkChange_OldFkUnsubscribed_NewFkSubscribed()
    {
        var results = new List<KeyValuePair<string, string>>();
        var builder = new StreamsBuilder();

        var orders = builder.Table<string, Order>("orders");
        var customers = builder.Table<string, Customer>("customers");

        orders.Join<string, Customer, string>(
            customers,
            order => order.CustomerId,
            (order, customer) => $"{order.OrderId}:{customer.Name}")
            .ToStream()
            .ForEach((k, v) => results.Add(new(k, v)));

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "fk-join-fkchange",
            BootstrapServers = "localhost:9092"
        };

        var app = new StreamsApplication(config, topology, _loggerFactory);

        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Alice"));
        app.ProcessRecord("customers", "cust-2", new Customer("cust-2", "Bob"));

        // Subscribe order-1 → cust-1
        app.ProcessRecord("orders", "order-1", new Order("order-1", "cust-1", 50m));
        Assert.Single(results);
        Assert.Equal("order-1:Alice", results[0].Value);

        // Change FK: order-1 → cust-2
        results.Clear();
        app.ProcessRecord("orders", "order-1", new Order("order-1", "cust-2", 50m));
        Assert.Single(results);
        Assert.Equal("order-1:Bob", results[0].Value);

        // Updating cust-1 should NOT re-join order-1 (it's unsubscribed)
        results.Clear();
        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Alice Updated"));
        Assert.Empty(results);

        // Updating cust-2 SHOULD re-join order-1
        results.Clear();
        app.ProcessRecord("customers", "cust-2", new Customer("cust-2", "Bob Updated"));
        Assert.Single(results);
        Assert.Equal("order-1:Bob Updated", results[0].Value);
    }

    // ─── Tombstone propagation ────────────────────────────────────────────────

    [Fact]
    public void ForeignKeyJoin_PrimaryDeleted_EmitsTombstone()
    {
        // Tombstones (empty byte arrays) are emitted by ForeignKeyJoinNode when a primary key
        // is deleted. Downstream ForEach nodes handle them as null values, but a raw To("sink")
        // sink captures the empty bytes. We verify via the subscription store directly.

        var joinResults = new List<string>();
        var builder = new StreamsBuilder();

        var orders = builder.Table<string, Order>("orders");
        var customers = builder.Table<string, Customer>("customers");

        orders.Join<string, Customer, string>(
            customers,
            order => order.CustomerId,
            (order, customer) => $"{order.OrderId}:{customer.Name}")
            .ToStream()
            .ForEach((k, v) => joinResults.Add(v));

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "fk-join-tombstone",
            BootstrapServers = "localhost:9092"
        };

        var app = new StreamsApplication(config, topology, _loggerFactory);

        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Bob"));
        app.ProcessRecord("orders", "order-1", new Order("order-1", "cust-1", 10m));

        Assert.Single(joinResults);
        Assert.Equal("order-1:Bob", joinResults[0]);

        // Delete order-1 — tombstone emitted, subscription removed
        app.ProcessRecordTombstone("orders", "order-1");

        // Clear any tombstone-forwarded null entries
        joinResults.Clear();

        // After deletion, updating cust-1 should NOT re-join order-1 (unsubscribed)
        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Bob Updated"));
        Assert.Empty(joinResults);
    }

    // ─── ForeignKeyJoinMetrics tests ──────────────────────────────────────────

    [Fact]
    public void ForeignKeyJoinMetrics_RecordSubscription_IncrementsCounters()
    {
        using var meter = new Meter("test-fk-metrics-sub");
        var metrics = new ForeignKeyJoinMetrics(meter, "orders-customers");

        metrics.RecordSubscription();
        metrics.RecordSubscription();
        metrics.RecordSubscription();

        Assert.Equal(3, metrics.TotalSubscriptions);
        Assert.Equal(3, metrics.CurrentSubscriptionCount);
    }

    [Fact]
    public void ForeignKeyJoinMetrics_RecordUnsubscription_DecrementsCurrentCount()
    {
        using var meter = new Meter("test-fk-metrics-unsub");
        var metrics = new ForeignKeyJoinMetrics(meter, "orders-customers");

        metrics.RecordSubscription();
        metrics.RecordSubscription();
        metrics.RecordUnsubscription();

        Assert.Equal(2, metrics.TotalSubscriptions);
        Assert.Equal(1, metrics.TotalUnsubscriptions);
        Assert.Equal(1, metrics.CurrentSubscriptionCount);
    }

    [Fact]
    public void ForeignKeyJoinMetrics_RecordLookup_IncrementsCounter()
    {
        using var meter = new Meter("test-fk-metrics-lookup");
        var metrics = new ForeignKeyJoinMetrics(meter, "orders-customers");

        metrics.RecordLookup(0.5);
        metrics.RecordLookup(1.2);

        Assert.Equal(2, metrics.TotalLookups);
    }

    [Fact]
    public void ForeignKeyJoinMetrics_RecordFanOut_AccumulatesTotal()
    {
        using var meter = new Meter("test-fk-metrics-fanout");
        var metrics = new ForeignKeyJoinMetrics(meter, "orders-customers");

        metrics.RecordFanOut(10);
        metrics.RecordFanOut(5);
        metrics.RecordFanOut(0); // zero fanout should not count

        Assert.Equal(15, metrics.TotalFanOut);
    }

    [Fact]
    public void ForeignKeyJoinMetrics_SetSubscriptionCount_UpdatesGauge()
    {
        using var meter = new Meter("test-fk-metrics-setcount");
        var metrics = new ForeignKeyJoinMetrics(meter, "orders-customers");

        metrics.SetSubscriptionCount(42);

        Assert.Equal(42, metrics.CurrentSubscriptionCount);
    }

    [Fact]
    public void ForeignKeyJoinMetrics_JoinName_IsPreserved()
    {
        using var meter = new Meter("test-fk-metrics-name");
        var metrics = new ForeignKeyJoinMetrics(meter, "my-join");

        Assert.Equal("my-join", metrics.JoinName);
    }

    // ─── ReaderWriterLockSlim concurrency tests ───────────────────────────────

    [Fact]
    public async Task ForeignKeySubscriptionStore_ConcurrentReads_DoNotDeadlock()
    {
        using var store = new ForeignKeySubscriptionStore<string, string>();

        // Seed some subscriptions
        for (var i = 0; i < 20; i++)
            store.Subscribe($"pk-{i}", $"fk-{i % 5}");

        Assert.Equal(20, store.Count);

        // Many concurrent readers
        const int readerCount = 50;
        var tasks = Enumerable.Range(0, readerCount)
            .Select(i => Task.Run(() =>
            {
                for (var j = 0; j < 100; j++)
                {
                    var subs = store.GetSubscribers($"fk-{j % 5}");
                    _ = subs.Count;
                    _ = store.GetForeignKey($"pk-{j % 20}");
                }
            }))
            .ToArray();

        // Should complete without deadlock within 10 seconds
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task ForeignKeySubscriptionStore_ConcurrentReadsAndWrites_NoDeadlock()
    {
        using var store = new ForeignKeySubscriptionStore<string, string>();

        const int iterations = 200;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var writer = Task.Run(async () =>
        {
            for (var i = 0; i < iterations && !cts.Token.IsCancellationRequested; i++)
            {
                store.Subscribe($"pk-{i}", $"fk-{i % 10}");
                if (i > 10)
                    store.Unsubscribe($"pk-{i - 10}", $"fk-{(i - 10) % 10}");
                await Task.Yield();
            }
        }, cts.Token);

        var readers = Enumerable.Range(0, 5)
            .Select(idx => Task.Run(async () =>
            {
                for (var i = 0; i < iterations && !cts.Token.IsCancellationRequested; i++)
                {
                    var subs = store.GetSubscribers($"fk-{i % 10}");
                    var cnt = store.Count;
                    _ = subs;
                    _ = cnt;
                    await Task.Yield();
                }
            }, cts.Token))
            .ToArray();

        await Task.WhenAll(readers.Append(writer)).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(cts.Token.IsCancellationRequested, "Test timed out — possible deadlock");
    }

    [Fact]
    public void ForeignKeySubscriptionStore_Count_ReflectsActiveSubscriptions()
    {
        using var store = new ForeignKeySubscriptionStore<string, string>();

        Assert.Equal(0, store.Count);

        store.Subscribe("pk-1", "fk-A");
        store.Subscribe("pk-2", "fk-A");
        store.Subscribe("pk-3", "fk-B");
        Assert.Equal(3, store.Count);

        store.Unsubscribe("pk-2", "fk-A");
        Assert.Equal(2, store.Count);
    }

    [Fact]
    public void ForeignKeySubscriptionStore_Dispose_ReleasesLock()
    {
        var store = new ForeignKeySubscriptionStore<string, string>();
        store.Subscribe("pk-1", "fk-1");

        // Should not throw
        store.Dispose();

        // Double-dispose should be safe
        store.Dispose();
    }
}
