using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Pipelines.Tests;

public class PipelineFlowTests
{
    private sealed record Order(string Status, double Amount, string CustomerId);

    [Fact]
    public void SingleFilter_ConsumesSourceTopicsDirectly()
    {
        var built = Pipeline.From<Order>("orders")
            .Named("high-value")
            .Filter(o => o.Amount > 1000)
            .To("orders-high-value")
            .Build();

        var node = Assert.Single(built.Nodes);
        Assert.Equal("filter-1", node.Id);
        Assert.Equal(ConnectNodeTypes.Filter, node.ConnectorType);
        Assert.Equal("orders", node.Config["topics"]);
        Assert.Equal("$.amount > 1000", node.Config["condition"]);
        Assert.Equal("orders-high-value", node.Config["output.topic"]);
        Assert.Empty(built.Connections);
    }

    [Fact]
    public void MultiStageChain_ConnectsNodesWithoutExplicitTopics()
    {
        var built = Pipeline.From<Order>("orders")
            .Named("chain")
            .Filter(o => o.Status == "active")
            .Map(m => m.Field("customer", o => o.CustomerId))
            .To("out")
            .Build();

        Assert.Equal(2, built.Nodes.Count);
        var filter = built.Nodes[0];
        var map = built.Nodes[1];

        Assert.Equal("orders", filter.Config["topics"]);
        Assert.False(filter.Config.ContainsKey("output.topic"));

        Assert.Equal("$.customerId", map.Config["mapping.customer"]);
        Assert.False(map.Config.ContainsKey("topics"));
        Assert.Equal("out", map.Config["output.topic"]);

        var connection = Assert.Single(built.Connections);
        Assert.Equal(filter.Id, connection.SourceNodeId);
        Assert.Equal(map.Id, connection.TargetNodeId);
        Assert.Equal(PipelineConnectionType.Normal, connection.Type);
    }

    [Fact]
    public void ConjunctionFilter_BecomesChainedFilterNodes()
    {
        var built = Pipeline.From<Order>("orders")
            .Named("double-filter")
            .Filter(o => o.Amount > 1000 && o.Status == "active")
            .To("out")
            .Build();

        Assert.Equal(2, built.Nodes.Count);
        Assert.All(built.Nodes, n => Assert.Equal(ConnectNodeTypes.Filter, n.ConnectorType));
        Assert.Equal("$.amount > 1000", built.Nodes[0].Config["condition"]);
        Assert.Equal("$.status == 'active'", built.Nodes[1].Config["condition"]);
        Assert.Single(built.Connections);
    }

    [Fact]
    public void MultipleSourceTopics_AreCommaJoined()
    {
        var built = Pipeline.From("orders-eu", "orders-us")
            .Named("merged")
            .Filter("$.amount > 0")
            .To("out")
            .Build();

        Assert.Equal("orders-eu,orders-us", built.Nodes[0].Config["topics"]);
    }

    [Fact]
    public void BareFromTo_InsertsPassThroughNode()
    {
        var built = Pipeline.From("a").Named("copy").To("b").Build();

        var node = Assert.Single(built.Nodes);
        Assert.Equal(ConnectNodeTypes.TopicTrigger, node.ConnectorType);
        Assert.Equal("a", node.Config["topics"]);
        Assert.Equal("b", node.Config["output.topic"]);
    }

    [Fact]
    public void FromSchedule_UsesTopicKeyForOutput()
    {
        var built = new PipelineBuilder("scheduled")
            .FromSchedule("*/5 * * * *", payload: "{\"tick\":true}")
            .To("ticks")
            .Build();

        var node = Assert.Single(built.Nodes);
        Assert.Equal(ConnectNodeTypes.ScheduleTrigger, node.ConnectorType);
        Assert.Equal("*/5 * * * *", node.Config["cron"]);
        Assert.Equal("ticks", node.Config["topic"]);
        Assert.False(node.Config.ContainsKey("output.topic"));
    }

    [Fact]
    public void ScheduleTriggerIntoChain_GetsWiredViaConnection()
    {
        var built = new PipelineBuilder("scheduled-chain")
            .FromSchedule("* * * * *")
            .Filter("$.tick == true")
            .To("out")
            .Build();

        Assert.Equal(2, built.Nodes.Count);
        var trigger = built.Nodes[0];
        Assert.False(trigger.Config.ContainsKey("topic"));
        Assert.Single(built.Connections);
    }

    [Fact]
    public void OnError_AttachesDlqNodeViaErrorConnection()
    {
        var built = Pipeline.From<Order>("orders")
            .Named("with-dlq")
            .Filter(o => o.Amount > 0)
            .OnError("orders-dlq")
            .To("out")
            .Build();

        Assert.Equal(2, built.Nodes.Count);
        var dlq = built.Nodes.Single(n => n.ConnectorType == ConnectNodeTypes.DlqSink);
        Assert.Equal("orders-dlq", dlq.Config["output.topic"]);

        var connection = Assert.Single(built.Connections);
        Assert.Equal(PipelineConnectionType.Error, connection.Type);
        Assert.Equal("filter-1", connection.SourceNodeId);
        Assert.Equal(dlq.Id, connection.TargetNodeId);
    }

    [Fact]
    public void RouteIf_ConfiguresBranchTopics()
    {
        var built = Pipeline.From<Order>("orders")
            .Named("branching")
            .RouteIf(o => o.Amount > 1000, "high", "low");

        var pipeline = built.Build();
        var node = Assert.Single(pipeline.Nodes);
        Assert.Equal(ConnectNodeTypes.If, node.ConnectorType);
        Assert.Equal("$.amount > 1000", node.Config["condition"]);
        Assert.Equal("high", node.Config["output.true.topic"]);
        Assert.Equal("low", node.Config["output.false.topic"]);
    }

    [Fact]
    public void NegatedRouteIf_SwapsBranchTopics()
    {
        var pipeline = Pipeline.From<Order>("orders")
            .Named("negated-branch")
            .RouteIf(o => !(o.Amount > 1000), "small", "big")
            .Build();

        var node = Assert.Single(pipeline.Nodes);
        Assert.Equal("$.amount > 1000", node.Config["condition"]);
        Assert.Equal("big", node.Config["output.true.topic"]);
        Assert.Equal("small", node.Config["output.false.topic"]);
    }

    [Fact]
    public void RouteBy_ConfiguresCases()
    {
        var pipeline = Pipeline.From<Order>("orders")
            .Named("switching")
            .RouteBy(o => o.Status, new Dictionary<string, string>
            {
                ["active"] = "active-orders",
                ["archived"] = "archived-orders",
            }, defaultTopic: "other-orders")
            .Build();

        var node = Assert.Single(pipeline.Nodes);
        Assert.Equal(ConnectNodeTypes.Switch, node.ConnectorType);
        Assert.Equal("$.status", node.Config["discriminator"]);
        Assert.Equal("active-orders", node.Config["case.active"]);
        Assert.Equal("archived-orders", node.Config["case.archived"]);
        Assert.Equal("other-orders", node.Config["default.topic"]);
    }

    [Fact]
    public void WithRetry_SetsRetryPolicyOnLastNode()
    {
        var built = Pipeline.From<Order>("orders")
            .Named("retrying")
            .Filter(o => o.Amount > 0)
            .WithRetry(maxRetries: 5, backoff: TimeSpan.FromMilliseconds(250))
            .To("out")
            .Build();

        var policy = Assert.Single(built.Nodes).RetryPolicy;
        Assert.NotNull(policy);
        Assert.Equal(5, policy.MaxRetries);
        Assert.Equal(250, policy.BackoffMs);
        Assert.True(policy.Enabled);
    }

    [Fact]
    public void Through_AddsArbitraryConnector()
    {
        var built = Pipeline.From("in")
            .Named("custom")
            .Through("My.Plugin.CustomNode", c => c.Set("custom.key", "value").Set("custom.flag", true))
            .To("out")
            .Build();

        var node = Assert.Single(built.Nodes);
        Assert.Equal("My.Plugin.CustomNode", node.ConnectorType);
        Assert.Equal("value", node.Config["custom.key"]);
        Assert.Equal("true", node.Config["custom.flag"]);
    }

    [Fact]
    public void TypedStages_EmitVerifiedConfigKeys()
    {
        var built = Pipeline.From<Order>("orders")
            .Named("kitchen-sink")
            .Deduplicate(o => o.CustomerId, TimeSpan.FromMinutes(2), DeduplicationStrategy.Last)
            .RateLimit(100)
            .MaskFields("***", "customerId")
            .To("out")
            .Build();

        var dedup = built.Nodes[0];
        Assert.Equal("$.customerId", dedup.Config["dedup.key"]);
        Assert.Equal("120000", dedup.Config["dedup.window.ms"]);
        Assert.Equal("last", dedup.Config["dedup.strategy"]);

        var limiter = built.Nodes[1];
        Assert.Equal("100", limiter.Config["rate.limit"]);
        Assert.Equal("1000", limiter.Config["rate.interval.ms"]);

        var mask = built.Nodes[2];
        Assert.Equal("customerId", mask.Config["mask.fields"]);
        Assert.Equal("***", mask.Config["mask.replacement"]);
    }

    [Fact]
    public void StageAfterTo_Throws()
    {
        var flow = Pipeline.From("a").Named("done");
        flow.To("b");

        Assert.Throws<PipelineBuildException>(() => flow.Filter("$.x == 1"));
    }

    [Fact]
    public void WithLabel_AndDescription_ArePreserved()
    {
        var built = Pipeline.From<Order>("orders")
            .Named("labeled")
            .DescribedAs("filters the orders")
            .Filter(o => o.Amount > 0)
            .WithLabel("Only positive")
            .To("out")
            .Build();

        Assert.Equal("filters the orders", built.Description);
        Assert.Equal("Only positive", Assert.Single(built.Nodes).Label);
    }
}
