namespace Kuestenlogik.Surgewave.Connect.Tests.Pipelines;

using Kuestenlogik.Surgewave.Connect.Pipelines;

public class PipelineDryRunnerTests
{
    private static PipelineNode N(string id, string type = "A", Dictionary<string, string>? config = null) =>
        new() { Id = id, ConnectorType = type, Config = config ?? [], X = 0, Y = 0 };

    private static PipelineDefinition CreatePipeline(
        List<PipelineNode> nodes,
        List<PipelineConnection> connections)
    {
        return new PipelineDefinition
        {
            Id = "test-pipeline",
            Name = "Test Pipeline",
            Nodes = nodes,
            Connections = connections,
            Status = PipelineStatus.Draft,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }

    [Fact]
    public void TopologicalSort_LinearPipeline_CorrectOrder()
    {
        var nodes = new List<PipelineNode>
        {
            N("c", "C"),
            N("a", "A"),
            N("b", "B")
        };

        var connections = new List<PipelineConnection>
        {
            new() { Id = "1", SourceNodeId = "a", TargetNodeId = "b" },
            new() { Id = "2", SourceNodeId = "b", TargetNodeId = "c" }
        };

        var sorted = PipelineDryRunner.TopologicalSort(nodes, connections);

        Assert.Equal("a", sorted[0]);
        Assert.Equal("b", sorted[1]);
        Assert.Equal("c", sorted[2]);
    }

    [Fact]
    public void TopologicalSort_EmptyPipeline_ReturnsEmpty()
    {
        var sorted = PipelineDryRunner.TopologicalSort([], []);
        Assert.Empty(sorted);
    }

    [Fact]
    public async Task DryRun_UnknownConnectorType_ReturnsError()
    {
        var pipeline = CreatePipeline(
            [N("n1", "NonExistent.Type")],
            []);

        var runner = new PipelineDryRunner();
        var result = await runner.RunAsync(pipeline,
            [new DryRunInput { NodeId = "n1", Records = [new DryRunRecord { Key = "k1", Value = """{"a":1}""" }] }],
            CancellationToken.None);

        Assert.True(result.Success);
        Assert.True(result.NodeTraces.ContainsKey("n1"));
        Assert.NotEmpty(result.NodeTraces["n1"].Errors);
    }

    [Fact]
    public async Task DryRun_EmptyPipeline_ReturnsSuccess()
    {
        var pipeline = CreatePipeline([], []);

        var runner = new PipelineDryRunner();
        var result = await runner.RunAsync(pipeline, [], CancellationToken.None);

        Assert.True(result.Success);
        Assert.Empty(result.NodeTraces);
    }

    [Fact]
    public async Task DryRun_WithMergeNode_ProcessesRecords()
    {
        var mergeNodeType = typeof(Kuestenlogik.Surgewave.Connect.Nodes.Logic.MergeNode).AssemblyQualifiedName!;

        var pipeline = CreatePipeline(
            [N("merge1", mergeNodeType)],
            []);

        var runner = new PipelineDryRunner();
        var result = await runner.RunAsync(pipeline,
            [new DryRunInput
            {
                NodeId = "merge1",
                Records = [new DryRunRecord { Key = "k1", Value = """{"data":"hello"}""" }]
            }],
            CancellationToken.None);

        Assert.True(result.Success);
        Assert.True(result.NodeTraces.ContainsKey("merge1"));
        Assert.Equal(1, result.NodeTraces["merge1"].InputCount);
    }

    [Fact]
    public void TopologicalSort_MultiBranch_AllNodesIncluded()
    {
        var nodes = new List<PipelineNode>
        {
            N("source", "A"),
            N("branch1", "B"),
            N("branch2", "C"),
            N("sink", "D")
        };

        var connections = new List<PipelineConnection>
        {
            new() { Id = "1", SourceNodeId = "source", TargetNodeId = "branch1" },
            new() { Id = "2", SourceNodeId = "source", TargetNodeId = "branch2" },
            new() { Id = "3", SourceNodeId = "branch1", TargetNodeId = "sink" },
            new() { Id = "4", SourceNodeId = "branch2", TargetNodeId = "sink" }
        };

        var sorted = PipelineDryRunner.TopologicalSort(nodes, connections);

        Assert.Equal(4, sorted.Count);
        Assert.Equal("source", sorted[0]);
        Assert.Equal("sink", sorted[3]);
    }
}
