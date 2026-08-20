namespace Kuestenlogik.Surgewave.Pipelines.Tests;

public class SurgewavePipelineSampleTests
{
    private sealed record OrderEvent(string Status, double Amount);

    private sealed class HighValueOrders : ISurgewavePipeline
    {
        public BuiltPipeline Define() => Pipeline
            .From<OrderEvent>("orders")
            .Named("high-value-orders")
            .Filter(o => o.Amount > 1000)
            .To("orders-high-value")
            .Build();
    }

    private sealed class UnnamedPipeline : ISurgewavePipeline
    {
        public BuiltPipeline Define() => Pipeline
            .From<OrderEvent>("orders")
            .Filter(o => o.Status == "active")
            .To("active-orders")
            .Build();
    }

    [Fact]
    public void PackagedPipeline_DefinesCompletely()
    {
        var built = new HighValueOrders().Define();

        Assert.Equal("high-value-orders", built.Name);
        var node = Assert.Single(built.Nodes);
        Assert.Equal("orders", node.Config["topics"]);
        Assert.Equal("orders-high-value", node.Config["output.topic"]);
    }

    [Fact]
    public void UnnamedPipeline_LeavesNameNull()
    {
        Assert.Null(new UnnamedPipeline().Define().Name);
    }
}
