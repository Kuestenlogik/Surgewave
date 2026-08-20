namespace Kuestenlogik.Surgewave.Connect.Tests.Pipelines;

using Kuestenlogik.Surgewave.Connect.Nodes.Transform;
using Kuestenlogik.Surgewave.Connect.Nodes.Trigger;
using Kuestenlogik.Surgewave.Connect.Pipelines;

public class PipelineTopicWiringTests
{
    private static Dictionary<string, string> Wire(
        Type? connectorType,
        Dictionary<string, string>? config = null,
        IReadOnlyList<string>? inputTopics = null,
        string? outputTopic = null,
        int outgoingNormalCount = 0)
    {
        var effective = config ?? [];
        PipelineOrchestrator.ApplyTopicWiring(
            effective,
            connectorType,
            inputTopics ?? [],
            outputTopic,
            outgoingNormalCount);
        return effective;
    }

    [Fact]
    public void ProcessorNode_GetsInputAndOutputWired()
    {
        var config = Wire(
            typeof(FilterNode),
            inputTopics: ["_pipeline-p1-c1"],
            outputTopic: "_pipeline-p1-c2",
            outgoingNormalCount: 1);

        Assert.Equal("_pipeline-p1-c1", config["topics"]);
        Assert.Equal("_pipeline-p1-c2", config["output.topic"]);
    }

    [Fact]
    public void ProcessorNode_ExplicitConfigWins()
    {
        var config = Wire(
            typeof(FilterNode),
            config: new Dictionary<string, string>
            {
                ["topics"] = "orders",
                ["output.topic"] = "orders-filtered",
            },
            inputTopics: ["_pipeline-p1-c1"],
            outputTopic: "_pipeline-p1-c2",
            outgoingNormalCount: 1);

        Assert.Equal("orders", config["topics"]);
        Assert.Equal("orders-filtered", config["output.topic"]);
    }

    [Fact]
    public void ProcessorNode_WithMultipleOutputs_DoesNotWireOutputTopic()
    {
        var config = Wire(
            typeof(Kuestenlogik.Surgewave.Connect.Nodes.Logic.IfNode),
            inputTopics: ["_pipeline-p1-c1"],
            outputTopic: "_pipeline-p1-c3",
            outgoingNormalCount: 2);

        Assert.Equal("_pipeline-p1-c1", config["topics"]);
        Assert.False(config.ContainsKey("output.topic"));
    }

    [Fact]
    public void ProcessorNode_JoinsMultipleInputTopics()
    {
        var config = Wire(
            typeof(Kuestenlogik.Surgewave.Connect.Nodes.Logic.MergeNode),
            inputTopics: ["_pipeline-p1-c1", "_pipeline-p1-c2"],
            outputTopic: "_pipeline-p1-c3",
            outgoingNormalCount: 1);

        Assert.Equal("_pipeline-p1-c1,_pipeline-p1-c2", config["topics"]);
    }

    [Fact]
    public void ProcessorEntryNode_WithoutConnections_StaysUntouched()
    {
        var config = Wire(
            typeof(TopicTrigger),
            config: new Dictionary<string, string> { ["topics"] = "orders" });

        Assert.Equal("orders", config["topics"]);
        Assert.False(config.ContainsKey("output.topic"));
    }

    [Fact]
    public void SourceConnector_GetsTopicOverwritten()
    {
        var config = Wire(
            typeof(ScheduleTrigger),
            config: new Dictionary<string, string> { ["topic"] = "manual" },
            outputTopic: "_pipeline-p1-c1",
            outgoingNormalCount: 1);

        Assert.Equal("_pipeline-p1-c1", config["topic"]);
        Assert.False(config.ContainsKey("topics"));
    }

    [Fact]
    public void SourceConnector_WithoutConnection_KeepsExplicitTopic()
    {
        var config = Wire(
            typeof(ScheduleTrigger),
            config: new Dictionary<string, string> { ["topic"] = "ticks" });

        Assert.Equal("ticks", config["topic"]);
    }

    [Fact]
    public void UnresolvableType_LeavesConfigUntouched()
    {
        var config = Wire(
            connectorType: null,
            config: new Dictionary<string, string> { ["custom"] = "x" },
            inputTopics: ["_pipeline-p1-c1"],
            outputTopic: "_pipeline-p1-c2",
            outgoingNormalCount: 1);

        Assert.Single(config);
        Assert.Equal("x", config["custom"]);
    }
}
