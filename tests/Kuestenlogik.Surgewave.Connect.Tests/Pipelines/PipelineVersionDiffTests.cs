namespace Kuestenlogik.Surgewave.Connect.Tests.Pipelines;

using Kuestenlogik.Surgewave.Connect.Pipelines;

public class PipelineVersionDiffTests
{
    private static PipelineNode N(string id, string type = "A", Dictionary<string, string>? config = null) =>
        new() { Id = id, ConnectorType = type, Config = config ?? [], X = 0, Y = 0 };

    private static PipelineDefinition CreateDefinition(List<PipelineNode>? nodes = null, List<PipelineConnection>? connections = null)
    {
        return new PipelineDefinition
        {
            Id = "p1",
            Name = "Test",
            Nodes = nodes ?? [],
            Connections = connections ?? [],
            Status = PipelineStatus.Draft,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }

    [Fact]
    public void NodeAddsAndRemoves_Detected()
    {
        var from = CreateDefinition(nodes: [N("n1", "A")]);
        var to = CreateDefinition(nodes: [N("n2", "B")]);

        var diff = PipelineVersionDiff.Compute(1, 2, from, to);

        Assert.Contains("n2", diff.NodesAdded);
        Assert.Contains("n1", diff.NodesRemoved);
        Assert.Empty(diff.NodesModified);
    }

    [Fact]
    public void ConnectionChanges_Detected()
    {
        var from = CreateDefinition(
            nodes: [N("n1", "A"), N("n2", "B")],
            connections: [new PipelineConnection { Id = "c1", SourceNodeId = "n1", TargetNodeId = "n2" }]);
        var to = CreateDefinition(
            nodes: [N("n1", "A"), N("n2", "B")],
            connections: [new PipelineConnection { Id = "c2", SourceNodeId = "n2", TargetNodeId = "n1" }]);

        var diff = PipelineVersionDiff.Compute(1, 2, from, to);

        Assert.Single(diff.ConnectionsAdded);
        Assert.Single(diff.ConnectionsRemoved);
        Assert.Contains("n2->n1", diff.ConnectionsAdded);
        Assert.Contains("n1->n2", diff.ConnectionsRemoved);
    }

    [Fact]
    public void ConfigChanges_Detected()
    {
        var from = CreateDefinition(nodes: [N("n1", "A", new() { ["x"] = "1", ["y"] = "old" })]);
        var to = CreateDefinition(nodes: [N("n1", "A", new() { ["x"] = "1", ["y"] = "new", ["z"] = "added" })]);

        var diff = PipelineVersionDiff.Compute(1, 2, from, to);

        Assert.Contains("n1", diff.NodesModified);
        Assert.Equal(2, diff.ConfigChanges.Count);
        Assert.Contains(diff.ConfigChanges, c => c.Key == "y" && c.OldValue == "old" && c.NewValue == "new");
        Assert.Contains(diff.ConfigChanges, c => c.Key == "z" && c.OldValue == null && c.NewValue == "added");
    }

    [Fact]
    public void IdenticalPipelines_EmptyDiff()
    {
        var def = CreateDefinition(
            nodes: [N("n1", "A", new() { ["key"] = "val" })],
            connections: [new PipelineConnection { Id = "c1", SourceNodeId = "n1", TargetNodeId = "n1" }]);

        var diff = PipelineVersionDiff.Compute(1, 2, def, def);

        Assert.Empty(diff.NodesAdded);
        Assert.Empty(diff.NodesRemoved);
        Assert.Empty(diff.NodesModified);
        Assert.Empty(diff.ConnectionsAdded);
        Assert.Empty(diff.ConnectionsRemoved);
        Assert.Empty(diff.ConfigChanges);
    }

    [Fact]
    public void EmptyPipelines_EmptyDiff()
    {
        var from = CreateDefinition();
        var to = CreateDefinition();

        var diff = PipelineVersionDiff.Compute(1, 2, from, to);

        Assert.Empty(diff.NodesAdded);
        Assert.Empty(diff.NodesRemoved);
        Assert.Empty(diff.ConfigChanges);
    }
}
