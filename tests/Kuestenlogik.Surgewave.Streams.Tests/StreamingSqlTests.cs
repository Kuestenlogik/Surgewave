using Kuestenlogik.Surgewave.Streams.Sql;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class StreamingSqlTests
{
    private readonly ITestOutputHelper _output;

    public StreamingSqlTests(ITestOutputHelper output)
    {
        _output = output;
    }

    private static SqlEngine CreateOrdersEngine()
    {
        var engine = new SqlEngine();
        engine.RegisterStream("orders",
        [
            new() { ["id"] = 1, ["customer"] = "Alice", ["amount"] = 100.0, ["product"] = "Widget", ["region"] = "EU" },
            new() { ["id"] = 2, ["customer"] = "Bob", ["amount"] = 250.0, ["product"] = "Gadget", ["region"] = "US" },
            new() { ["id"] = 3, ["customer"] = "Alice", ["amount"] = 75.0, ["product"] = "Gadget", ["region"] = "EU" },
            new() { ["id"] = 4, ["customer"] = "Charlie", ["amount"] = 500.0, ["product"] = "Widget", ["region"] = "US" },
            new() { ["id"] = 5, ["customer"] = "Bob", ["amount"] = 150.0, ["product"] = "Widget", ["region"] = "EU" },
        ]);
        return engine;
    }

    // === Lexer/Parser Tests ===

    [Fact]
    public void Lexer_TokenizesSimpleSelect()
    {
        var lexer = new SqlLexer("SELECT * FROM orders");
        var tokens = lexer.Tokenize();
        Assert.Equal(SqlTokenType.Select, tokens[0].Type);
        Assert.Equal(SqlTokenType.Star, tokens[1].Type);
        Assert.Equal(SqlTokenType.From, tokens[2].Type);
        Assert.Equal(SqlTokenType.Identifier, tokens[3].Type);
        Assert.Equal("orders", tokens[3].Value);
        Assert.Equal(SqlTokenType.Eof, tokens[4].Type);
    }

    [Fact]
    public void Lexer_TokenizesStringLiterals()
    {
        var lexer = new SqlLexer("WHERE name = 'Alice'");
        var tokens = lexer.Tokenize();
        Assert.Equal(SqlTokenType.StringLiteral, tokens[3].Type);
        Assert.Equal("Alice", tokens[3].Value);
    }

    [Fact]
    public void Lexer_TokenizesComparisonOperators()
    {
        var lexer = new SqlLexer("a >= 10 AND b <= 20 AND c <> 'x'");
        var tokens = lexer.Tokenize();
        Assert.Contains(tokens, t => t.Type == SqlTokenType.GreaterEqual);
        Assert.Contains(tokens, t => t.Type == SqlTokenType.LessEqual);
        Assert.Contains(tokens, t => t.Type == SqlTokenType.NotEquals);
    }

    [Fact]
    public void Parser_SelectStar()
    {
        var stmt = SqlParser.Parse("SELECT * FROM orders");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.Single(select.Columns);
        Assert.IsType<StarSelectItem>(select.Columns[0]);
        var source = Assert.IsType<TableSource>(select.Source);
        Assert.Equal("orders", source.Name);
    }

    [Fact]
    public void Parser_SelectWithWhere()
    {
        var stmt = SqlParser.Parse("SELECT customer, amount FROM orders WHERE amount > 100");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.Equal(2, select.Columns.Count);
        Assert.NotNull(select.Where);
    }

    [Fact]
    public void Parser_SelectWithGroupBy()
    {
        var stmt = SqlParser.Parse("SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.NotNull(select.GroupBy);
        Assert.Single(select.GroupBy);
    }

    [Fact]
    public void Parser_CreateStreamAsSelect()
    {
        var stmt = SqlParser.Parse("CREATE STREAM high_value AS SELECT * FROM orders WHERE amount > 200");
        var csas = Assert.IsType<CreateStreamAsSelect>(stmt);
        Assert.Equal("high_value", csas.Name);
        Assert.NotNull(csas.Query.Where);
    }

    [Fact]
    public void Parser_CreateTableAsSelect()
    {
        var stmt = SqlParser.Parse("CREATE TABLE customer_totals AS SELECT customer, SUM(amount) FROM orders GROUP BY customer");
        var ctas = Assert.IsType<CreateTableAsSelect>(stmt);
        Assert.Equal("customer_totals", ctas.Name);
    }

    [Fact]
    public void Parser_JoinSyntax()
    {
        var stmt = SqlParser.Parse("SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON o.customer = c.id");
        var select = Assert.IsType<SelectStatement>(stmt);
        var join = Assert.IsType<JoinSource>(select.Source);
        Assert.Equal(JoinType.Left, join.JoinType);
    }

    [Fact]
    public void Parser_EmitChanges()
    {
        var stmt = SqlParser.Parse("SELECT * FROM orders EMIT CHANGES");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.True(select.EmitChanges);
    }

    // === Engine Execution Tests ===

    [Fact]
    public void Engine_SelectStar_ReturnsAllRows()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT * FROM orders");
        Assert.Equal(5, result.Rows.Count);
    }

    [Fact]
    public void Engine_SelectColumns_Projects()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT customer, amount FROM orders");
        Assert.Equal(5, result.Rows.Count);
        Assert.Equal(2, result.Rows[0].Count);
        Assert.True(result.Rows[0].ContainsKey("customer"));
        Assert.True(result.Rows[0].ContainsKey("amount"));
    }

    [Fact]
    public void Engine_WhereFilter()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT * FROM orders WHERE amount > 100");
        Assert.Equal(3, result.Rows.Count);
    }

    [Fact]
    public void Engine_WhereStringEquals()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT * FROM orders WHERE customer = 'Alice'");
        Assert.Equal(2, result.Rows.Count);
    }

    [Fact]
    public void Engine_WhereAndOr()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT * FROM orders WHERE customer = 'Alice' AND amount > 80");
        Assert.Single(result.Rows);
        Assert.Equal(100.0, result.Rows[0]["amount"]);
    }

    [Fact]
    public void Engine_GroupByCount()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT customer, COUNT(*) AS order_count FROM orders GROUP BY customer");

        _output.WriteLine(result.ToFormattedTable());

        Assert.Equal(3, result.Rows.Count); // Alice, Bob, Charlie
        var alice = result.Rows.First(r => r["customer"]?.ToString() == "Alice");
        Assert.Equal(2, Convert.ToInt32(alice["order_count"]));
    }

    [Fact]
    public void Engine_GroupBySum()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer");

        _output.WriteLine(result.ToFormattedTable());

        var alice = result.Rows.First(r => r["customer"]?.ToString() == "Alice");
        Assert.Equal(175.0, Convert.ToDouble(alice["total"]));

        var charlie = result.Rows.First(r => r["customer"]?.ToString() == "Charlie");
        Assert.Equal(500.0, Convert.ToDouble(charlie["total"]));
    }

    [Fact]
    public void Engine_GroupByAvg()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT customer, AVG(amount) AS avg_amount FROM orders GROUP BY customer");

        var alice = result.Rows.First(r => r["customer"]?.ToString() == "Alice");
        Assert.Equal(87.5, Convert.ToDouble(alice["avg_amount"]));
    }

    [Fact]
    public void Engine_Having()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute(
            "SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer HAVING total > 200");

        _output.WriteLine(result.ToFormattedTable());

        Assert.Equal(2, result.Rows.Count); // Bob=400, Charlie=500
        Assert.DoesNotContain(result.Rows, r => r["customer"]?.ToString() == "Alice");
    }

    [Fact]
    public void Engine_OrderBy()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT * FROM orders ORDER BY amount DESC");

        Assert.Equal(500.0, result.Rows[0]["amount"]);
        Assert.Equal(75.0, result.Rows[^1]["amount"]);
    }

    [Fact]
    public void Engine_Limit()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT * FROM orders LIMIT 2");
        Assert.Equal(2, result.Rows.Count);
    }

    [Fact]
    public void Engine_WhereIn()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT * FROM orders WHERE customer IN ('Alice', 'Charlie')");
        Assert.Equal(3, result.Rows.Count);
    }

    [Fact]
    public void Engine_WhereBetween()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT * FROM orders WHERE amount BETWEEN 100 AND 300");
        Assert.Equal(3, result.Rows.Count);
    }

    [Fact]
    public void Engine_WhereLike()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT * FROM orders WHERE product LIKE 'Wid%'");
        Assert.Equal(3, result.Rows.Count);
    }

    [Fact]
    public void Engine_WhereIsNull()
    {
        var engine = new SqlEngine();
        engine.RegisterStream("data",
        [
            new() { ["name"] = "Alice", ["score"] = 100 },
            new() { ["name"] = "Bob", ["score"] = null },
            new() { ["name"] = "Charlie", ["score"] = 80 },
        ]);

        var result = engine.Execute("SELECT * FROM data WHERE score IS NULL");
        Assert.Single(result.Rows);
        Assert.Equal("Bob", result.Rows[0]["name"]);
    }

    [Fact]
    public void Engine_WhereIsNotNull()
    {
        var engine = new SqlEngine();
        engine.RegisterStream("data",
        [
            new() { ["name"] = "Alice", ["score"] = 100 },
            new() { ["name"] = "Bob", ["score"] = null },
        ]);

        var result = engine.Execute("SELECT * FROM data WHERE score IS NOT NULL");
        Assert.Single(result.Rows);
    }

    [Fact]
    public void Engine_ColumnAlias()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT customer AS name, amount AS total FROM orders LIMIT 1");
        Assert.True(result.Rows[0].ContainsKey("name"));
        Assert.True(result.Rows[0].ContainsKey("total"));
    }

    [Fact]
    public void Engine_ArithmeticExpression()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT customer, amount * 1.1 AS with_tax FROM orders LIMIT 1");
        Assert.Equal(110.0, Convert.ToDouble(result.Rows[0]["with_tax"]), 0.01);
    }

    [Fact]
    public void Engine_ScalarFunctions()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT UPPER(customer) AS name FROM orders LIMIT 1");
        Assert.Equal("ALICE", result.Rows[0]["name"]);
    }

    [Fact]
    public void Engine_CaseExpression()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("""
            SELECT customer,
                   CASE WHEN amount > 200 THEN 'high'
                        WHEN amount > 100 THEN 'medium'
                        ELSE 'low' END AS tier
            FROM orders
        """);

        _output.WriteLine(result.ToFormattedTable());

        var charlie = result.Rows.First(r => r["customer"]?.ToString() == "Charlie");
        Assert.Equal("high", charlie["tier"]);
    }

    [Fact]
    public void Engine_CreateStreamAsSelect()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("CREATE STREAM eu_orders AS SELECT * FROM orders WHERE region = 'EU'");

        Assert.True(result.IsCreateStatement);
        Assert.Equal("eu_orders", result.CreatedName);
        Assert.Equal(3, result.Rows.Count);

        // The new stream is now queryable
        var requery = engine.Execute("SELECT * FROM eu_orders");
        Assert.Equal(3, requery.Rows.Count);
    }

    [Fact]
    public void Engine_CreateTableAsSelect_WithAggregation()
    {
        var engine = CreateOrdersEngine();
        engine.Execute("CREATE TABLE sales_by_customer AS SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer");

        var result = engine.Execute("SELECT * FROM sales_by_customer ORDER BY total DESC");

        _output.WriteLine(result.ToFormattedTable());

        Assert.Equal(3, result.Rows.Count);
        Assert.Equal("Charlie", result.Rows[0]["customer"]);
    }

    [Fact]
    public void Engine_Join()
    {
        var engine = new SqlEngine();
        engine.RegisterStream("orders",
        [
            new() { ["order_id"] = 1, ["customer_id"] = "C1", ["amount"] = 100 },
            new() { ["order_id"] = 2, ["customer_id"] = "C2", ["amount"] = 200 },
        ]);
        engine.RegisterStream("customers",
        [
            new() { ["cid"] = "C1", ["name"] = "Alice" },
            new() { ["cid"] = "C2", ["name"] = "Bob" },
        ]);

        var result = engine.Execute(
            "SELECT o.order_id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.cid");

        _output.WriteLine(result.ToFormattedTable());

        Assert.Equal(2, result.Rows.Count);
    }

    [Fact]
    public void Engine_LeftJoin()
    {
        var engine = new SqlEngine();
        engine.RegisterStream("orders",
        [
            new() { ["order_id"] = 1, ["customer_id"] = "C1", ["amount"] = 100 },
            new() { ["order_id"] = 2, ["customer_id"] = "C9", ["amount"] = 200 },
        ]);
        engine.RegisterStream("customers",
        [
            new() { ["cid"] = "C1", ["name"] = "Alice" },
        ]);

        var result = engine.Execute(
            "SELECT o.order_id, c.name FROM orders o LEFT JOIN customers c ON o.customer_id = c.cid");

        _output.WriteLine(result.ToFormattedTable());

        Assert.Equal(2, result.Rows.Count);
        // Second row has NULL for the customer name (no match for C9)
        var secondRow = result.Rows[1];
        var nameValue = secondRow.TryGetValue("c.name", out var v) ? v : secondRow.GetValueOrDefault("name");
        Assert.Null(nameValue);
    }

    [Fact]
    public void Engine_JsonStream()
    {
        var engine = new SqlEngine();
        engine.RegisterJsonStream("events",
        [
            """{"user": "alice", "action": "click", "count": 5}""",
            """{"user": "bob", "action": "view", "count": 3}""",
            """{"user": "alice", "action": "view", "count": 2}""",
        ]);

        var result = engine.Execute("SELECT user, SUM(count) AS total FROM events GROUP BY user");

        _output.WriteLine(result.ToFormattedTable());

        Assert.Equal(2, result.Rows.Count);
        var alice = result.Rows.First(r => r["user"]?.ToString() == "alice");
        Assert.Equal(7.0, Convert.ToDouble(alice["total"]));
    }

    [Fact]
    public void Engine_FormattedOutput()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT customer, amount FROM orders ORDER BY amount DESC LIMIT 3");

        var table = result.ToFormattedTable();
        _output.WriteLine(table);

        Assert.Contains("Charlie", table);
        Assert.Contains("500", table);
        Assert.Contains("3 row(s)", table);
    }

    [Fact]
    public void Engine_NotBetween()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT * FROM orders WHERE amount NOT BETWEEN 100 AND 200");
        Assert.Equal(3, result.Rows.Count); // 75, 250, 500
    }

    [Fact]
    public void Engine_Coalesce()
    {
        var engine = new SqlEngine();
        engine.RegisterStream("data",
        [
            new() { ["a"] = null, ["b"] = "fallback" },
            new() { ["a"] = "value", ["b"] = "other" },
        ]);

        var result = engine.Execute("SELECT COALESCE(a, b) AS result FROM data");
        Assert.Equal("fallback", result.Rows[0]["result"]);
        Assert.Equal("value", result.Rows[1]["result"]);
    }

    [Fact]
    public void Engine_CountDistinct()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT COUNT(DISTINCT region) AS regions FROM orders GROUP BY region");

        // Each group has 1 distinct region
        Assert.All(result.Rows, r => Assert.Equal(1, Convert.ToInt32(r["regions"])));
    }

    [Fact]
    public void Engine_MultipleGroupByColumns()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute(
            "SELECT region, product, COUNT(*) AS cnt FROM orders GROUP BY region, product");

        _output.WriteLine(result.ToFormattedTable());

        Assert.True(result.Rows.Count >= 3); // At least 3 distinct (region, product) pairs
    }

    [Fact]
    public void Parser_InvalidSql_ThrowsParseException()
    {
        Assert.Throws<SqlParseException>(() => SqlParser.Parse("INVALID SQL"));
    }

    [Fact]
    public void Engine_UnknownSource_ThrowsParseException()
    {
        var engine = new SqlEngine();
        Assert.Throws<SqlParseException>(() => engine.Execute("SELECT * FROM nonexistent"));
    }

    [Fact]
    public void Engine_InsertIntoSelect()
    {
        var engine = CreateOrdersEngine();
        engine.Execute("CREATE STREAM eu_orders AS SELECT * FROM orders WHERE region = 'EU'");
        engine.Execute("INSERT INTO eu_orders SELECT * FROM orders WHERE region = 'US'");

        var result = engine.Execute("SELECT * FROM eu_orders");
        Assert.Equal(5, result.Rows.Count); // 3 EU + 2 US
    }

    [Fact]
    public void Engine_CastFunction()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT CAST(amount AS INT) AS int_amount FROM orders LIMIT 1");
        Assert.Equal(100, result.Rows[0]["int_amount"]);
    }

    [Fact]
    public void Engine_Substring()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT SUBSTRING(customer, 1, 3) AS prefix FROM orders LIMIT 1");
        Assert.Equal("Ali", result.Rows[0]["prefix"]);
    }

    [Fact]
    public void Engine_ConcatFunction()
    {
        var engine = CreateOrdersEngine();
        var result = engine.Execute("SELECT CONCAT(customer, '-', region) AS combined FROM orders LIMIT 1");
        Assert.Equal("Alice-EU", result.Rows[0]["combined"]);
    }

    [Fact]
    public void Lexer_HandlesComments()
    {
        var result = SqlParser.Parse("SELECT * FROM orders -- this is a comment\nWHERE amount > 0");
        var select = Assert.IsType<SelectStatement>(result);
        Assert.NotNull(select.Where);
    }

    [Fact]
    public void Parser_WindowTumble()
    {
        var stmt = SqlParser.Parse(
            "SELECT customer, COUNT(*) FROM orders o LEFT JOIN events e WINDOW TUMBLE(ts, 5 MINUTES) ON o.id = e.id");
        var select = Assert.IsType<SelectStatement>(stmt);
        var join = Assert.IsType<JoinSource>(select.Source);
        Assert.NotNull(join.Window);
        Assert.Equal(WindowType.Tumble, join.Window.Type);
        Assert.Equal(TimeSpan.FromMinutes(5), join.Window.Size);
    }
}
