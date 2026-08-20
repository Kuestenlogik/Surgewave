using Kuestenlogik.Surgewave.Connect.Pipelines;
using Xunit;

namespace Kuestenlogik.Surgewave.Samples.PipelineAsCode.Tests;

/// <summary>
/// Pipelines built with the DSL are plain definitions — they can be asserted on in unit
/// tests without any broker, which is half the point of pipeline-as-code.
/// </summary>
public class HighValueOrdersPipelineTests
{
    [Fact]
    public void Define_ProducesTheExpectedTopology()
    {
        var built = new HighValueOrdersPipeline().Define();

        Assert.Equal("high-value-orders", built.Name);

        // the && filter splits into two chained filter nodes, then the map node
        Assert.Equal(4, built.Nodes.Count);
        Assert.Equal("orders", built.Nodes[0].Config["topics"]);
        Assert.Equal("$.status == 'active'", built.Nodes[0].Config["condition"]);
        Assert.Equal("$.amount > 1000", built.Nodes[1].Config["condition"]);

        var map = built.Nodes[2];
        Assert.Equal("$.orderId", map.Config["mapping.order"]);
        Assert.Equal("orders-high-value", map.Config["output.topic"]);
        Assert.NotNull(map.RetryPolicy);

        var dlq = built.Nodes[3];
        Assert.Equal("orders-high-value-dlq", dlq.Config["output.topic"]);
        Assert.Contains(built.Connections, c => c.Type == PipelineConnectionType.Error);
    }

    [Fact]
    public void Export_IsDeterministic()
    {
        var first = new HighValueOrdersPipeline().Define().ToJson();
        var second = new HighValueOrdersPipeline().Define().ToJson();

        Assert.Equal(first, second);
    }
}
