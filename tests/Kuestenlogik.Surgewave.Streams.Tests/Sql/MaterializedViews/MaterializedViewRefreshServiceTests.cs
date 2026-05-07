using Kuestenlogik.Surgewave.Streams.Sql;
using Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests.Sql.MaterializedViews;

public sealed class MaterializedViewRefreshServiceTests
{
    /// <summary>In-memory raw topic reader for tests.</summary>
    private sealed class FakeTopicReader : IRawTopicReader
    {
        private readonly Dictionary<string, List<RawTopicMessage>> _topics = new(StringComparer.OrdinalIgnoreCase);

        public void Append(string topic, params RawTopicMessage[] messages)
        {
            if (!_topics.TryGetValue(topic, out var list))
                _topics[topic] = list = [];
            list.AddRange(messages);
        }

        public IEnumerable<RawTopicMessage> ReadTopic(string topicName)
            => _topics.TryGetValue(topicName, out var list) ? list : [];
    }

    private static MaterializedViewRefreshService BuildService(
        MaterializedViewRegistry registry,
        IRawTopicReader reader,
        bool enabled = true)
    {
        var options = Options.Create(new MaterializedViewOptions
        {
            Enabled = enabled,
            RefreshInterval = TimeSpan.FromMilliseconds(50),
        });
        return new MaterializedViewRefreshService(
            registry,
            reader,
            options,
            NullLogger<MaterializedViewRefreshService>.Instance);
    }

    [Fact]
    public void RefreshAll_AggregatesGroupBySumFromTopic()
    {
        var registry = new MaterializedViewRegistry();
        var reader = new FakeTopicReader();
        var now = DateTimeOffset.UtcNow;

        reader.Append("orders",
            new RawTopicMessage(0, 0, now, "k1", """{"customer":"alice","amount":10}"""),
            new RawTopicMessage(1, 0, now, "k2", """{"customer":"bob","amount":20}"""),
            new RawTopicMessage(2, 0, now, "k3", """{"customer":"alice","amount":15}"""));

        var def = new ViewDefinition(
            Name: "orders_by_customer",
            OriginalSql: "CREATE MATERIALIZED VIEW orders_by_customer AS SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer",
            SelectSql: "SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer",
            SourceTopics: ["orders"],
            KeyColumns: ["customer"],
            HasAggregation: true,
            IfNotExists: false,
            CreatedAt: now);

        Assert.True(registry.TryRegister(def, out _));

        var service = BuildService(registry, reader);
        service.RefreshAll();

        Assert.True(registry.TryGet("orders_by_customer", out var view));
        var snapshot = view.Snapshot;
        Assert.Equal(2, snapshot.Rows.Count);

        var alice = snapshot.Rows.Single(r => string.Equals(r["customer"]?.ToString(), "alice", StringComparison.Ordinal));
        var bob = snapshot.Rows.Single(r => string.Equals(r["customer"]?.ToString(), "bob", StringComparison.Ordinal));
        Assert.Equal(25.0, Convert.ToDouble(alice["total"]));
        Assert.Equal(20.0, Convert.ToDouble(bob["total"]));
        Assert.Equal(1, snapshot.RefreshCount);
    }

    [Fact]
    public void RefreshAll_PicksUpNewMessages()
    {
        var registry = new MaterializedViewRegistry();
        var reader = new FakeTopicReader();
        var now = DateTimeOffset.UtcNow;

        reader.Append("clicks",
            new RawTopicMessage(0, 0, now, null, """{"page":"home","hits":1}"""));

        registry.TryRegister(new ViewDefinition(
            Name: "click_count",
            OriginalSql: "irrelevant",
            SelectSql: "SELECT page, SUM(hits) AS n FROM clicks GROUP BY page",
            SourceTopics: ["clicks"],
            KeyColumns: ["page"],
            HasAggregation: true,
            IfNotExists: false,
            CreatedAt: now), out _);

        var service = BuildService(registry, reader);
        service.RefreshAll();

        Assert.True(registry.TryGet("click_count", out var view));
        var firstHome = view.Snapshot.Rows.Single(r => string.Equals(r["page"]?.ToString(), "home", StringComparison.Ordinal));
        Assert.Equal(1.0, Convert.ToDouble(firstHome["n"]));

        reader.Append("clicks",
            new RawTopicMessage(1, 0, now, null, """{"page":"home","hits":2}"""),
            new RawTopicMessage(2, 0, now, null, """{"page":"about","hits":5}"""));

        service.RefreshAll();
        Assert.Equal(2, view.Snapshot.Rows.Count);
        var home = view.Snapshot.Rows.Single(r => string.Equals(r["page"]?.ToString(), "home", StringComparison.Ordinal));
        var about = view.Snapshot.Rows.Single(r => string.Equals(r["page"]?.ToString(), "about", StringComparison.Ordinal));
        Assert.Equal(3.0, Convert.ToDouble(home["n"]));
        Assert.Equal(5.0, Convert.ToDouble(about["n"]));
        Assert.Equal(2, view.Snapshot.RefreshCount);
    }

    [Fact]
    public void SqlEngine_SelectFromMaterializedView_ReturnsSnapshot()
    {
        var registry = new MaterializedViewRegistry();
        var reader = new FakeTopicReader();
        var now = DateTimeOffset.UtcNow;

        reader.Append("events",
            new RawTopicMessage(0, 0, now, null, """{"type":"login","count":5}"""),
            new RawTopicMessage(1, 0, now, null, """{"type":"logout","count":3}"""));

        // CREATE MV via the engine bound to the registry
        var engine = new SqlEngine(registry);
        // Pre-bind the source topic so CREATE MV's parser->engine path works.
        // The refresh loop will rebind it via its own engine instance.
        engine.RegisterTopicSource("events", new SqlTopicSource(() => reader.ReadTopic("events")));

        var createResult = engine.Execute(
            "CREATE MATERIALIZED VIEW totals AS SELECT type, SUM(count) AS total FROM events GROUP BY type");
        Assert.True(createResult.IsCreateStatement);

        // Drive a manual refresh
        var service = BuildService(registry, reader);
        service.RefreshAll();

        // SELECT against the view via a fresh engine bound to the registry
        var queryEngine = new SqlEngine(registry);
        var result = queryEngine.Execute("SELECT type, total FROM totals");

        Assert.Equal(2, result.Rows.Count);
        var login = result.Rows.Single(r => string.Equals(r["type"]?.ToString(), "login", StringComparison.Ordinal));
        Assert.Equal(5.0, Convert.ToDouble(login["total"]));
    }

    [Fact]
    public void SqlEngine_DropMaterializedView_RemovesFromRegistry()
    {
        var registry = new MaterializedViewRegistry();
        var reader = new FakeTopicReader();
        reader.Append("t", new RawTopicMessage(0, 0, DateTimeOffset.UtcNow, null, "{}"));

        var engine = new SqlEngine(registry);
        engine.RegisterTopicSource("t", new SqlTopicSource(() => reader.ReadTopic("t")));
        engine.Execute("CREATE MATERIALIZED VIEW v AS SELECT * FROM t");
        Assert.True(registry.Contains("v"));

        engine.Execute("DROP MATERIALIZED VIEW v");
        Assert.False(registry.Contains("v"));
    }

    [Fact]
    public void SqlEngine_DropMissingViewWithoutIfExists_Throws()
    {
        var registry = new MaterializedViewRegistry();
        var engine = new SqlEngine(registry);
        Assert.Throws<SqlParseException>(() => engine.Execute("DROP MATERIALIZED VIEW ghost"));
    }

    [Fact]
    public void SqlEngine_DropMissingViewWithIfExists_Succeeds()
    {
        var registry = new MaterializedViewRegistry();
        var engine = new SqlEngine(registry);
        var result = engine.Execute("DROP MATERIALIZED VIEW IF EXISTS ghost");
        Assert.True(result.IsCreateStatement);
    }

    [Fact]
    public void SqlEngine_CreateMvWithoutRegistry_Throws()
    {
        var engine = new SqlEngine();
        Assert.Throws<InvalidOperationException>(() =>
            engine.Execute("CREATE MATERIALIZED VIEW v AS SELECT * FROM t"));
    }
}
