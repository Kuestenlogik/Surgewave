using Kuestenlogik.Surgewave.Linq;
using Xunit;

namespace Kuestenlogik.Surgewave.Linq.Tests;

public class SurgewaveQueryContextTests
{
    [Fact]
    public void Query_ReturnsQueryable()
    {
        var context = new SurgewaveQueryContext("localhost:9092");
        var queryable = context.Query<TestOrder>("orders");

        Assert.NotNull(queryable);
        Assert.IsAssignableFrom<IQueryable<TestOrder>>(queryable);
    }

    [Fact]
    public void Query_WithPartition_ReturnsQueryable()
    {
        var context = new SurgewaveQueryContext("localhost:9092");
        var queryable = context.Query<TestOrder>("orders", partition: 0, fromOffset: 100);

        Assert.NotNull(queryable);
    }

    [Fact]
    public void Query_WithOptions_ReturnsQueryable()
    {
        var options = new SurgewaveQueryOptions
        {
            BootstrapServers = "localhost:9092",
            MaxScanMessages = 5000,
            ParallelPartitionScan = false
        };
        var context = new SurgewaveQueryContext(options);
        var queryable = context.Query<TestOrder>("orders");

        Assert.NotNull(queryable);
    }

    [Fact]
    public void Query_SupportsWhereChaining()
    {
        var context = new SurgewaveQueryContext("localhost:9092");
        var queryable = context.Query<TestOrder>("orders")
            .Where(o => o.Total > 100)
            .Where(o => o.Region == "EU")
            .Take(10);

        Assert.NotNull(queryable);
        Assert.IsAssignableFrom<IQueryable<TestOrder>>(queryable);
    }

    [Fact]
    public void Query_SupportsSelectProjection()
    {
        var context = new SurgewaveQueryContext("localhost:9092");
        var queryable = context.Query<TestOrder>("orders")
            .Where(o => o.Total > 100)
            .Select(o => new { o.Id, o.Total });

        Assert.NotNull(queryable);
    }
}

public class TestOrder
{
    public string Id { get; set; } = "";
    public string Region { get; set; } = "";
    public decimal Total { get; set; }
    public string Status { get; set; } = "";
}
