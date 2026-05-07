using Kuestenlogik.Surgewave.Streams.Sql;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests.Sql;

public sealed class SqlWindowExecutorTests
{
    private readonly ITestOutputHelper _output;
    private readonly SqlExpressionEvaluator _evaluator = new();

    public SqlWindowExecutorTests(ITestOutputHelper output)
    {
        _output = output;
    }

    private static List<Dictionary<string, object?>> CreateTimestampedRows()
    {
        var baseTime = new DateTimeOffset(2026, 1, 15, 10, 0, 0, TimeSpan.Zero);
        return
        [
            new() { ["ts"] = baseTime, ["user"] = "Alice", ["amount"] = 100.0 },
            new() { ["ts"] = baseTime.AddMinutes(1), ["user"] = "Bob", ["amount"] = 200.0 },
            new() { ["ts"] = baseTime.AddMinutes(2), ["user"] = "Alice", ["amount"] = 50.0 },
            new() { ["ts"] = baseTime.AddMinutes(6), ["user"] = "Alice", ["amount"] = 300.0 },
            new() { ["ts"] = baseTime.AddMinutes(7), ["user"] = "Bob", ["amount"] = 150.0 },
            new() { ["ts"] = baseTime.AddMinutes(12), ["user"] = "Alice", ["amount"] = 75.0 },
        ];
    }

    [Fact]
    public void TumblingWindow_GroupsByTimeWindow()
    {
        var rows = CreateTimestampedRows();
        var window = new WindowSpec(WindowType.Tumble, "ts", TimeSpan.FromMinutes(5), null);

        var groupByKeys = new List<SqlExpression> { new ColumnRef(null, "user") };
        var selectItems = new List<SelectItem>
        {
            new ExpressionSelectItem(new ColumnRef(null, "user"), "user"),
            new ExpressionSelectItem(new FunctionCall("SUM", [new ColumnRef(null, "amount")], false), "total"),
            new ExpressionSelectItem(new FunctionCall("COUNT", [], false), "cnt"),
        };

        var results = SqlWindowExecutor.Execute(rows, window, groupByKeys, selectItems, _evaluator);

        foreach (var row in results)
        {
            _output.WriteLine($"window=[{row["window_start"]} - {row["window_end"]}], " +
                              $"user={row["user"]}, total={row["total"]}, cnt={row["cnt"]}");
        }

        // 5-minute tumbling windows: [10:00-10:05), [10:05-10:10), [10:10-10:15)
        // Window [10:00-10:05): Alice=150 (100+50), Bob=200
        // Window [10:05-10:10): Alice=300, Bob=150
        // Window [10:10-10:15): Alice=75
        Assert.True(results.Count >= 3, $"Expected at least 3 window groups, got {results.Count}");

        // Verify aggregates for a specific window
        var aliceFirst = results.FirstOrDefault(r =>
            r["user"]?.ToString() == "Alice" &&
            r.TryGetValue("total", out var t) && Convert.ToDouble(t) == 150.0);
        Assert.NotNull(aliceFirst);
    }

    [Fact]
    public void HoppingWindow_CreatesOverlappingWindows()
    {
        var rows = CreateTimestampedRows();
        // Window size 10 minutes, advance 5 minutes -> overlapping
        var window = new WindowSpec(WindowType.Hop, "ts", TimeSpan.FromMinutes(10), TimeSpan.FromMinutes(5));

        var selectItems = new List<SelectItem>
        {
            new ExpressionSelectItem(new FunctionCall("COUNT", [], false), "cnt"),
            new ExpressionSelectItem(new FunctionCall("SUM", [new ColumnRef(null, "amount")], false), "total"),
        };

        var results = SqlWindowExecutor.Execute(rows, window, null, selectItems, _evaluator);

        foreach (var row in results)
        {
            _output.WriteLine($"window=[{row["window_start"]} - {row["window_end"]}], " +
                              $"cnt={row["cnt"]}, total={row["total"]}");
        }

        // With overlapping windows, some rows should appear in multiple windows
        // Total rows across all windows should be >= total input rows
        var totalRowsInWindows = results.Sum(r => Convert.ToInt32(r["cnt"]));
        Assert.True(totalRowsInWindows >= 6,
            $"Hopping windows should produce at least as many row appearances as input rows, got {totalRowsInWindows}");
    }

    [Fact]
    public void SessionWindow_MergesNearbyEvents()
    {
        var baseTime = new DateTimeOffset(2026, 1, 15, 10, 0, 0, TimeSpan.Zero);
        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["ts"] = baseTime, ["user"] = "Alice", ["amount"] = 10.0 },
            new() { ["ts"] = baseTime.AddSeconds(15), ["user"] = "Alice", ["amount"] = 20.0 },
            new() { ["ts"] = baseTime.AddSeconds(25), ["user"] = "Alice", ["amount"] = 30.0 },
            // Gap > 30 seconds
            new() { ["ts"] = baseTime.AddMinutes(2), ["user"] = "Alice", ["amount"] = 40.0 },
        };

        // 30-second session gap
        var window = new WindowSpec(WindowType.Session, "ts", TimeSpan.FromSeconds(30), null);

        var groupByKeys = new List<SqlExpression> { new ColumnRef(null, "user") };
        var selectItems = new List<SelectItem>
        {
            new ExpressionSelectItem(new ColumnRef(null, "user"), "user"),
            new ExpressionSelectItem(new FunctionCall("COUNT", [], false), "cnt"),
            new ExpressionSelectItem(new FunctionCall("SUM", [new ColumnRef(null, "amount")], false), "total"),
        };

        var results = SqlWindowExecutor.Execute(rows, window, groupByKeys, selectItems, _evaluator);

        foreach (var row in results)
        {
            _output.WriteLine($"window=[{row["window_start"]} - {row["window_end"]}], " +
                              $"user={row["user"]}, cnt={row["cnt"]}, total={row["total"]}");
        }

        // Should have 2 sessions for Alice:
        // Session 1: 3 events (within 30s gap), total=60
        // Session 2: 1 event, total=40
        Assert.Equal(2, results.Count);

        var firstSession = results[0];
        Assert.Equal(3, Convert.ToInt32(firstSession["cnt"]));
        Assert.Equal(60.0, Convert.ToDouble(firstSession["total"]));

        var secondSession = results[1];
        Assert.Equal(1, Convert.ToInt32(secondSession["cnt"]));
        Assert.Equal(40.0, Convert.ToDouble(secondSession["total"]));
    }

    [Fact]
    public void NoGroupBy_WindowOnlyAggregation()
    {
        var rows = CreateTimestampedRows();
        var window = new WindowSpec(WindowType.Tumble, "ts", TimeSpan.FromMinutes(5), null);

        // No GROUP BY — aggregate all rows per window
        var selectItems = new List<SelectItem>
        {
            new ExpressionSelectItem(new FunctionCall("COUNT", [], false), "cnt"),
            new ExpressionSelectItem(new FunctionCall("SUM", [new ColumnRef(null, "amount")], false), "total"),
        };

        var results = SqlWindowExecutor.Execute(rows, window, null, selectItems, _evaluator);

        foreach (var row in results)
        {
            _output.WriteLine($"window=[{row["window_start"]} - {row["window_end"]}], " +
                              $"cnt={row["cnt"]}, total={row["total"]}");
        }

        // 3 tumbling windows: [10:00-10:05), [10:05-10:10), [10:10-10:15)
        Assert.Equal(3, results.Count);

        // All results should have window_start and window_end
        Assert.All(results, r =>
        {
            Assert.True(r.ContainsKey("window_start"));
            Assert.True(r.ContainsKey("window_end"));
        });

        // Total count across all windows should be 6
        var totalCount = results.Sum(r => Convert.ToInt32(r["cnt"]));
        Assert.Equal(6, totalCount);
    }
}
