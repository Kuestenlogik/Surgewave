namespace Kuestenlogik.Surgewave.Connect.Tests.Pipelines;

using Kuestenlogik.Surgewave.Connect.Pipelines;

public class PipelineVersionStoreTests
{
    private static PipelineNode N(string id, string type = "Test", Dictionary<string, string>? config = null) =>
        new() { Id = id, ConnectorType = type, Config = config ?? [], X = 0, Y = 0 };

    private static PipelineDefinition CreateDefinition(string id = "p1", List<PipelineNode>? nodes = null, List<PipelineConnection>? connections = null)
    {
        return new PipelineDefinition
        {
            Id = id,
            Name = "Test Pipeline",
            Nodes = nodes ?? [],
            Connections = connections ?? [],
            Status = PipelineStatus.Draft,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }

    [Fact]
    public void SaveVersion_CreatesVersionEntry()
    {
        var store = new PipelineVersionStore();
        var def = CreateDefinition();

        var entry = store.SaveVersion("p1", def, "Created");

        Assert.Equal(1, entry.Version);
        Assert.Equal("Created", entry.ChangeDescription);
    }

    [Fact]
    public void SaveVersion_AutoIncrementsVersion()
    {
        var store = new PipelineVersionStore();
        var def = CreateDefinition();

        store.SaveVersion("p1", def, "v1");
        store.SaveVersion("p1", def, "v2");
        var entry = store.SaveVersion("p1", def, "v3");

        Assert.Equal(3, entry.Version);

        var versions = store.GetVersions("p1");
        Assert.Equal(3, versions.Count);
    }

    [Fact]
    public void GetVersion_ReturnsCorrectVersion()
    {
        var store = new PipelineVersionStore();
        var def1 = CreateDefinition();
        var def2 = CreateDefinition(nodes: [N("n1")]);

        store.SaveVersion("p1", def1, "v1");
        store.SaveVersion("p1", def2, "v2");

        var v1 = store.GetVersion("p1", 1);
        var v2 = store.GetVersion("p1", 2);

        Assert.NotNull(v1);
        Assert.Empty(v1!.Definition.Nodes);
        Assert.NotNull(v2);
        Assert.Single(v2!.Definition.Nodes);
    }

    [Fact]
    public void Max50Versions_TrimsOldest()
    {
        var store = new PipelineVersionStore();

        for (var i = 0; i < 60; i++)
        {
            store.SaveVersion("p1", CreateDefinition(), $"v{i + 1}");
        }

        var versions = store.GetVersions("p1");
        Assert.Equal(50, versions.Count);
        Assert.Equal(11, versions[0].Version); // First 10 trimmed
    }

    [Fact]
    public void GetDiff_ComputesNodeAndConnectionChanges()
    {
        var store = new PipelineVersionStore();

        var def1 = CreateDefinition(nodes:
        [
            N("n1", "TypeA", new() { ["key1"] = "val1" }),
            N("n2", "TypeB")
        ], connections:
        [
            new PipelineConnection { Id = "c1", SourceNodeId = "n1", TargetNodeId = "n2" }
        ]);

        var def2 = CreateDefinition(nodes:
        [
            N("n1", "TypeA", new() { ["key1"] = "val2" }),
            N("n3", "TypeC")
        ], connections:
        [
            new PipelineConnection { Id = "c2", SourceNodeId = "n1", TargetNodeId = "n3" }
        ]);

        store.SaveVersion("p1", def1, "v1");
        store.SaveVersion("p1", def2, "v2");

        var diff = store.GetDiff("p1", 1, 2);

        Assert.NotNull(diff);
        Assert.Contains("n3", diff!.NodesAdded);
        Assert.Contains("n2", diff.NodesRemoved);
        Assert.Contains("n1", diff.NodesModified);
        Assert.Single(diff.ConfigChanges);
        Assert.Equal("val1", diff.ConfigChanges[0].OldValue);
        Assert.Equal("val2", diff.ConfigChanges[0].NewValue);
    }

    [Fact]
    public void EmptyHistory_ReturnsEmpty()
    {
        var store = new PipelineVersionStore();

        var versions = store.GetVersions("nonexistent");
        Assert.Empty(versions);

        var version = store.GetVersion("nonexistent", 1);
        Assert.Null(version);
    }
}
