using Kuestenlogik.Surgewave.Streams.Sql;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests.Sql.MaterializedViews;

public sealed class MaterializedViewParserTests
{
    [Fact]
    public void Parse_CreateMaterializedView_Simple()
    {
        var stmt = SqlParser.Parse("CREATE MATERIALIZED VIEW mv AS SELECT * FROM orders");
        var mv = Assert.IsType<CreateMaterializedViewAsSelect>(stmt);
        Assert.Equal("mv", mv.Name);
        Assert.False(mv.IfNotExists);
        Assert.Single(mv.Query.Columns);
    }

    [Fact]
    public void Parse_CreateMaterializedView_IfNotExists()
    {
        var stmt = SqlParser.Parse("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT id FROM t");
        var mv = Assert.IsType<CreateMaterializedViewAsSelect>(stmt);
        Assert.True(mv.IfNotExists);
        Assert.Equal("mv", mv.Name);
    }

    [Fact]
    public void Parse_CreateMaterializedView_WithGroupBy()
    {
        var stmt = SqlParser.Parse(
            "CREATE MATERIALIZED VIEW orders_by_customer AS " +
            "SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer");
        var mv = Assert.IsType<CreateMaterializedViewAsSelect>(stmt);
        Assert.NotNull(mv.Query.GroupBy);
        Assert.Single(mv.Query.GroupBy!);
    }

    [Fact]
    public void Parse_CreateMaterializedView_WithProperties()
    {
        var stmt = SqlParser.Parse(
            "CREATE MATERIALIZED VIEW mv WITH (storage = 'memory') AS SELECT * FROM t");
        var mv = Assert.IsType<CreateMaterializedViewAsSelect>(stmt);
        Assert.NotNull(mv.WithProperties);
        Assert.Equal("memory", mv.WithProperties!["storage"]);
    }

    [Fact]
    public void Parse_DropMaterializedView()
    {
        var stmt = SqlParser.Parse("DROP MATERIALIZED VIEW mv");
        var drop = Assert.IsType<DropMaterializedView>(stmt);
        Assert.Equal("mv", drop.Name);
        Assert.False(drop.IfExists);
    }

    [Fact]
    public void Parse_DropMaterializedView_IfExists()
    {
        var stmt = SqlParser.Parse("DROP MATERIALIZED VIEW IF EXISTS mv");
        var drop = Assert.IsType<DropMaterializedView>(stmt);
        Assert.True(drop.IfExists);
    }

    [Fact]
    public void Parse_DropView_TreatedAsMaterialized()
    {
        var stmt = SqlParser.Parse("DROP VIEW mv");
        var drop = Assert.IsType<DropMaterializedView>(stmt);
        Assert.Equal("mv", drop.Name);
    }
}
