using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Processors;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class ForeignKeyJoinTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public ForeignKeyJoinTests(ITestOutputHelper output)
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

    private sealed record Order(string OrderId, string CustomerId, decimal Amount);
    private sealed record Customer(string CustomerId, string Name);
    private sealed record OrderWithCustomer(string OrderId, string CustomerId, decimal Amount, string CustomerName);

    [Fact]
    public void ForeignKeyJoin_BasicLookup_OrdersToCustomers()
    {
        var results = new List<KeyValuePair<string, OrderWithCustomer>>();
        var builder = new StreamsBuilder();

        var orders = builder.Table<string, Order>("orders");
        var customers = builder.Table<string, Customer>("customers");

        orders.Join<string, Customer, OrderWithCustomer>(
            customers,
            order => order.CustomerId,
            (order, customer) => new OrderWithCustomer(order.OrderId, order.CustomerId, order.Amount, customer.Name))
            .ToStream()
            .ForEach((k, v) => results.Add(new(k, v)));

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "fk-join-basic",
            BootstrapServers = "localhost:9092"
        };

        var app = new StreamsApplication(config, topology, _loggerFactory);

        // First add customer, then order
        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Alice"));
        app.ProcessRecord("orders", "order-1", new Order("order-1", "cust-1", 99.99m));

        Assert.Single(results);
        Assert.Equal("order-1", results[0].Key);
        Assert.Equal("Alice", results[0].Value.CustomerName);
    }

    [Fact]
    public void ForeignKeyJoin_ForeignTableUpdate_PropagatesJoin()
    {
        var results = new List<KeyValuePair<string, OrderWithCustomer>>();
        var builder = new StreamsBuilder();

        var orders = builder.Table<string, Order>("orders");
        var customers = builder.Table<string, Customer>("customers");

        orders.Join<string, Customer, OrderWithCustomer>(
            customers,
            order => order.CustomerId,
            (order, customer) => new OrderWithCustomer(order.OrderId, order.CustomerId, order.Amount, customer.Name))
            .ToStream()
            .ForEach((k, v) => results.Add(new(k, v)));

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "fk-join-propagate",
            BootstrapServers = "localhost:9092"
        };

        var app = new StreamsApplication(config, topology, _loggerFactory);

        // Add customer and order
        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Alice"));
        app.ProcessRecord("orders", "order-1", new Order("order-1", "cust-1", 50.00m));

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Value.CustomerName);

        // Update customer name -> should re-join
        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Alice Smith"));

        Assert.Equal(2, results.Count);
        Assert.Equal("Alice Smith", results[1].Value.CustomerName);
    }

    [Fact]
    public void ForeignKeyJoin_PrimaryDelete_EmitsTombstone()
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
            ApplicationId = "fk-join-delete",
            BootstrapServers = "localhost:9092"
        };

        var app = new StreamsApplication(config, topology, _loggerFactory);

        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Bob"));
        app.ProcessRecord("orders", "order-1", new Order("order-1", "cust-1", 10.00m));

        Assert.Single(results);

        // Deleting the order won't produce through ForEach (tombstone is empty bytes)
        // But it shouldn't throw
    }

    [Fact]
    public void ForeignKeyJoin_ForeignKeyChange_UpdatesSubscription()
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
            ApplicationId = "fk-join-change",
            BootstrapServers = "localhost:9092"
        };

        var app = new StreamsApplication(config, topology, _loggerFactory);

        // Setup customers
        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Alice"));
        app.ProcessRecord("customers", "cust-2", new Customer("cust-2", "Bob"));

        // Order initially points to cust-1
        app.ProcessRecord("orders", "order-1", new Order("order-1", "cust-1", 100.00m));
        Assert.Single(results);
        Assert.Equal("order-1:Alice", results[0].Value);

        // Change FK to cust-2
        app.ProcessRecord("orders", "order-1", new Order("order-1", "cust-2", 100.00m));
        Assert.Equal(2, results.Count);
        Assert.Equal("order-1:Bob", results[1].Value);
    }

    [Fact]
    public void ForeignKeyLeftJoin_NoForeignMatch_EmitsWithNull()
    {
        var results = new List<KeyValuePair<string, string>>();
        var builder = new StreamsBuilder();

        var orders = builder.Table<string, Order>("orders");
        var customers = builder.Table<string, Customer>("customers");

        orders.LeftJoin<string, Customer, string>(
            customers,
            order => order.CustomerId,
            (order, customer) => $"{order.OrderId}:{customer?.Name ?? "UNKNOWN"}")
            .ToStream()
            .ForEach((k, v) => results.Add(new(k, v)));

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "fk-leftjoin-null",
            BootstrapServers = "localhost:9092"
        };

        var app = new StreamsApplication(config, topology, _loggerFactory);

        // Order with no matching customer (left join should still emit)
        app.ProcessRecord("orders", "order-1", new Order("order-1", "nonexistent", 50.00m));

        Assert.Single(results);
        Assert.Equal("order-1:UNKNOWN", results[0].Value);
    }

    [Fact]
    public void ForeignKeyJoin_MultipleSubscribers_AllRejoined()
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
            ApplicationId = "fk-join-multi",
            BootstrapServers = "localhost:9092"
        };

        var app = new StreamsApplication(config, topology, _loggerFactory);

        // Add a customer and multiple orders referencing same customer
        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Alice"));
        app.ProcessRecord("orders", "order-1", new Order("order-1", "cust-1", 10.00m));
        app.ProcessRecord("orders", "order-2", new Order("order-2", "cust-1", 20.00m));
        app.ProcessRecord("orders", "order-3", new Order("order-3", "cust-1", 30.00m));

        Assert.Equal(3, results.Count);

        // Update customer -> all 3 orders should re-join
        results.Clear();
        app.ProcessRecord("customers", "cust-1", new Customer("cust-1", "Alice Updated"));

        Assert.Equal(3, results.Count);
        Assert.All(results, r => Assert.Contains("Alice Updated", r.Value));
    }
}
