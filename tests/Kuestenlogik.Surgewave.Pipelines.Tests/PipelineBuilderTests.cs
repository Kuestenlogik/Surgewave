using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Pipelines.Tests;

public class PipelineBuilderTests
{
    [Fact]
    public void ManualGraph_BuildsNodesAndConnections()
    {
        var builder = Pipeline.Create("manual");
        var source = builder.AddNode(ConnectNodeTypes.TopicTrigger, new Dictionary<string, string> { ["topics"] = "in" });
        var sink = builder.AddNode(ConnectNodeTypes.Filter, new Dictionary<string, string> { ["condition"] = "$.x == 1", ["output.topic"] = "out" });
        builder.Connect(source, sink);

        var built = builder.Build();

        Assert.Equal("manual", built.Name);
        Assert.Equal(2, built.Nodes.Count);
        var connection = Assert.Single(built.Connections);
        Assert.Equal("c1", connection.Id);
        Assert.Equal(source, connection.SourceNodeId);
        Assert.Equal(sink, connection.TargetNodeId);
    }

    [Fact]
    public void NodeIds_AreSemanticAndUnique()
    {
        var builder = Pipeline.Create("ids");
        var first = builder.AddNode(ConnectNodeTypes.Filter);
        var second = builder.AddNode(ConnectNodeTypes.Filter);
        var trigger = builder.AddNode(ConnectNodeTypes.TopicTrigger);

        Assert.Equal("filter-1", first);
        Assert.Equal("filter-2", second);
        Assert.Equal("topic-trigger-1", trigger);
    }

    [Fact]
    public void ConnectUnknownNode_Throws()
    {
        var builder = Pipeline.Create("bad");
        var node = builder.AddNode(ConnectNodeTypes.Filter);

        Assert.Throws<PipelineBuildException>(() => builder.Connect(node, "ghost"));
        Assert.Throws<PipelineBuildException>(() => builder.Connect("ghost", node));
    }

    [Fact]
    public void SelfConnection_Throws()
    {
        var builder = Pipeline.Create("self");
        var node = builder.AddNode(ConnectNodeTypes.Filter);

        Assert.Throws<PipelineBuildException>(() => builder.Connect(node, node));
    }

    [Fact]
    public void DuplicateConnection_Throws()
    {
        var builder = Pipeline.Create("dup");
        var a = builder.AddNode(ConnectNodeTypes.Filter);
        var b = builder.AddNode(ConnectNodeTypes.Filter);
        builder.Connect(a, b);

        Assert.Throws<PipelineBuildException>(() => builder.Connect(a, b));
    }

    [Fact]
    public void Cycle_Throws()
    {
        var builder = Pipeline.Create("cyclic");
        var a = builder.AddNode(ConnectNodeTypes.Filter);
        var b = builder.AddNode(ConnectNodeTypes.Filter);
        var c = builder.AddNode(ConnectNodeTypes.Filter);
        builder.Connect(a, b);
        builder.Connect(b, c);
        builder.Connect(c, a);

        var ex = Assert.Throws<PipelineBuildException>(() => builder.Build());
        Assert.Contains("cycle", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void EmptyPipeline_Throws()
    {
        Assert.Throws<PipelineBuildException>(() => Pipeline.Create("empty").Build());
    }

    [Fact]
    public void Layout_PlacesChainLeftToRight()
    {
        var built = Pipeline.From("in")
            .Named("layouted")
            .Filter("$.a == 1")
            .Filter("$.b == 2")
            .Filter("$.c == 3")
            .To("out")
            .Build();

        Assert.Equal(3, built.Nodes.Count);
        Assert.True(built.Nodes[0].X < built.Nodes[1].X);
        Assert.True(built.Nodes[1].X < built.Nodes[2].X);
        Assert.Equal(built.Nodes[0].Y, built.Nodes[1].Y);
    }

    [Fact]
    public void UnnamedPipeline_BuildsButRefusesExport()
    {
        var built = Pipeline.From("a").Filter("$.x == 1").To("b").Build();

        Assert.Null(built.Name);
        Assert.Throws<PipelineBuildException>(() => built.ToJson());
        Assert.Throws<PipelineBuildException>(() => built.ToExport());
    }

    [Fact]
    public void ParametersAndSchedule_FlowIntoBuiltPipeline()
    {
        var built = Pipeline.Create("configured")
            .WithParameter("region", "eu")
            .WithSchedule("0 * * * *", timezone: "Europe/Berlin", maxRunDurationMinutes: 10)
            .FromTopic("in")
            .To("out")
            .Build();

        Assert.NotNull(built.Parameters);
        Assert.Equal("eu", built.Parameters["region"]);
        Assert.NotNull(built.Schedule);
        Assert.Equal("0 * * * *", built.Schedule.CronExpression);
        Assert.Equal("Europe/Berlin", built.Schedule.Timezone);
        Assert.True(built.Schedule.Enabled);
        Assert.Equal(10, built.Schedule.MaxRunDurationMinutes);
    }

    [Fact]
    public void ErrorConnections_KeepTheirType()
    {
        var builder = Pipeline.Create("errors");
        var a = builder.AddNode(ConnectNodeTypes.Filter);
        var dlq = builder.AddNode(ConnectNodeTypes.DlqSink);
        builder.Connect(a, dlq, error: true);

        var built = builder.Build();
        Assert.Equal(PipelineConnectionType.Error, Assert.Single(built.Connections).Type);
    }

    [Fact]
    public void ErrorConnections_SurviveTheJsonRoundTrip()
    {
        var builder = Pipeline.Create("errors-roundtrip");
        var a = builder.AddNode(ConnectNodeTypes.Filter);
        var dlq = builder.AddNode(ConnectNodeTypes.DlqSink);
        builder.Connect(a, dlq, error: true);

        var json = builder.Build().ToJson();
        var restored = Serialization.PipelineJson.Read(json);

        Assert.Equal(PipelineConnectionType.Error, Assert.Single(restored.Pipeline.Connections).Type);
    }
}
