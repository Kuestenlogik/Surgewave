using Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests.Sql.MaterializedViews;

public sealed class MaterializedViewRegistryTests
{
    private static ViewDefinition Def(string name, bool ifNotExists = false) => new(
        Name: name,
        OriginalSql: "CREATE MATERIALIZED VIEW " + name + " AS SELECT * FROM t",
        SelectSql: "SELECT * FROM t",
        SourceTopics: ["t"],
        KeyColumns: [],
        HasAggregation: false,
        IfNotExists: ifNotExists,
        CreatedAt: DateTimeOffset.UtcNow);

    [Fact]
    public void TryRegister_NewView_Succeeds()
    {
        var registry = new MaterializedViewRegistry();
        var ok = registry.TryRegister(Def("mv1"), out var view);
        Assert.True(ok);
        Assert.Equal("mv1", view.Definition.Name);
        Assert.True(registry.Contains("mv1"));
    }

    [Fact]
    public void TryRegister_DuplicateName_Fails()
    {
        var registry = new MaterializedViewRegistry();
        Assert.True(registry.TryRegister(Def("mv1"), out _));
        Assert.False(registry.TryRegister(Def("mv1"), out _));
    }

    [Fact]
    public void TryRegister_DuplicateNameWithIfNotExists_ReturnsExisting()
    {
        var registry = new MaterializedViewRegistry();
        Assert.True(registry.TryRegister(Def("mv1"), out var first));
        Assert.True(registry.TryRegister(Def("mv1", ifNotExists: true), out var second));
        Assert.Same(first, second);
    }

    [Fact]
    public void Contains_IsCaseInsensitive()
    {
        var registry = new MaterializedViewRegistry();
        registry.TryRegister(Def("MyView"), out _);
        Assert.True(registry.Contains("myview"));
        Assert.True(registry.Contains("MYVIEW"));
    }

    [Fact]
    public void TryUnregister_RemovesView()
    {
        var registry = new MaterializedViewRegistry();
        registry.TryRegister(Def("mv1"), out _);
        Assert.True(registry.TryUnregister("mv1", out _));
        Assert.False(registry.Contains("mv1"));
    }

    [Fact]
    public void TryUnregister_Missing_ReturnsFalse()
    {
        var registry = new MaterializedViewRegistry();
        Assert.False(registry.TryUnregister("ghost", out _));
    }

    [Fact]
    public void Snapshot_PublishesAtomicReplacement()
    {
        var registry = new MaterializedViewRegistry();
        registry.TryRegister(Def("mv1"), out var view);

        Assert.Empty(view.Snapshot.Rows);

        view.PublishSnapshot(
            [new Dictionary<string, object?> { ["x"] = 1 }],
            ["x"]);

        Assert.Single(view.Snapshot.Rows);
        Assert.Equal(1, view.Snapshot.RefreshCount);

        view.PublishSnapshot(
            [
                new Dictionary<string, object?> { ["x"] = 1 },
                new Dictionary<string, object?> { ["x"] = 2 },
            ],
            ["x"]);

        Assert.Equal(2, view.Snapshot.Rows.Count);
        Assert.Equal(2, view.Snapshot.RefreshCount);
    }
}
