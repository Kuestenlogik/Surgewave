using Kuestenlogik.Surgewave.Schema.Registry;
using Kuestenlogik.Surgewave.Schema.Registry.Handlers;
using Kuestenlogik.Surgewave.Schema.Registry.Lineage;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Schema.Registry.Tests;

/// <summary>
/// Lineage-driven impact analysis (#13): an incompatible schema change does not abstractly
/// "fail the check" — it breaks the named readers of the topic, and everything downstream of
/// a broken pipeline goes stale. These tests pin three properties: the walk names direct
/// readers and transitive downstream topics; the compatibility verdict NEVER depends on
/// lineage (a missing or crashing source only shortens the name list); and cycles between
/// pipelines terminate.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SchemaImpactAnalyzerTests : IDisposable
{
    private const string OrdersV1 = """
        {"type": "record", "name": "Order", "fields": [
            {"name": "id", "type": "long"}, {"name": "amount", "type": "double"}]}
        """;

    /// <summary>id wechselt long→string: klassisch backward-inkompatibel.</summary>
    private const string OrdersIncompatible = """
        {"type": "record", "name": "Order", "fields": [
            {"name": "id", "type": "string"}, {"name": "amount", "type": "double"}]}
        """;

    /// <summary>Optionales Feld mit Default: backward-kompatibel.</summary>
    private const string OrdersCompatible = """
        {"type": "record", "name": "Order", "fields": [
            {"name": "id", "type": "long"}, {"name": "amount", "type": "double"},
            {"name": "note", "type": ["null", "string"], "default": null}]}
        """;

    private readonly ILoggerFactory _loggerFactory;
    private readonly SchemaStore _store;
    private readonly CompatibilityChecker _checker;

    public SchemaImpactAnalyzerTests()
    {
        _loggerFactory = LoggerFactory.Create(_ => { });
        _store = new SchemaStore(_loggerFactory.CreateLogger<SchemaStore>());
        var registry = new SchemaTypeHandlerRegistry([new AvroSchemaHandler()]);
        _checker = new CompatibilityChecker(_loggerFactory.CreateLogger<CompatibilityChecker>(), registry);
        _store.RegisterSchema("orders-value", OrdersV1, SchemaType.Avro);
    }

    public void Dispose()
    {
        _store.Dispose();
        _loggerFactory.Dispose();
    }

    private SchemaImpactAnalyzer Analyzer(ISchemaLineageSource? lineage) =>
        new(_store, _checker, _loggerFactory.CreateLogger<SchemaImpactAnalyzer>(), lineage);

    /// <summary>orders → Pipeline "enrich" → enriched-orders → Consumer "reporting";
    /// daneben liest "billing" orders direkt.</summary>
    private sealed class FakeLineage : ISchemaLineageSource
    {
        public IReadOnlyList<TopicReader> GetReaders(string topic) => topic switch
        {
            "orders" =>
            [
                new TopicReader("billing", TopicReaderKind.ConsumerGroup),
                new TopicReader("enrich", TopicReaderKind.Pipeline, ["enriched-orders"]),
            ],
            "enriched-orders" => [new TopicReader("reporting", TopicReaderKind.ConsumerGroup)],
            _ => [],
        };
    }

    [Fact]
    public void IncompatibleChange_NamesDirectReaders_AndTransitiveDownstream()
    {
        _store.RegisterSchema("enriched-orders-value", OrdersV1, SchemaType.Avro);

        var impact = Analyzer(new FakeLineage()).Analyze("orders-value", OrdersIncompatible, SchemaType.Avro);

        Assert.False(impact.IsCompatible);
        Assert.NotEmpty(impact.CompatibilityErrors);
        Assert.False(impact.LineageUnavailable);

        Assert.Equal(2, impact.AffectedPipelines.Count);
        Assert.Contains(impact.AffectedPipelines, p => p.Name == "billing" && p.Kind == "consumer-group");
        Assert.Contains(impact.AffectedPipelines, p => p.Name == "enrich" && p.Kind == "pipeline");

        var downstream = Assert.Single(impact.DownstreamTopics);
        Assert.Equal("enriched-orders", downstream.Topic);
        Assert.Equal("enrich", downstream.Via);
        Assert.Equal("enriched-orders-value", downstream.Subject);
        Assert.Equal(1, downstream.LatestVersion);
    }

    [Fact]
    public void CompatibleChange_ReportsCompatible_WithTheSameNames()
    {
        var impact = Analyzer(new FakeLineage()).Analyze("orders-value", OrdersCompatible, SchemaType.Avro);

        Assert.True(impact.IsCompatible);
        Assert.Empty(impact.CompatibilityErrors);
        // Der Report ist Auskunft, kein Urteil: die Leser stehen auch bei Kompatibilität drin.
        Assert.Equal(2, impact.AffectedPipelines.Count);
    }

    [Fact]
    public void WithoutLineageSource_VerdictStands_AndAbsenceIsMarked()
    {
        var impact = Analyzer(lineage: null).Analyze("orders-value", OrdersIncompatible, SchemaType.Avro);

        Assert.False(impact.IsCompatible);
        Assert.True(impact.LineageUnavailable);
        Assert.Empty(impact.AffectedPipelines);
        Assert.Empty(impact.DownstreamTopics);
    }

    private sealed class ThrowingLineage : ISchemaLineageSource
    {
        public IReadOnlyList<TopicReader> GetReaders(string topic) =>
            throw new InvalidOperationException("lineage backend down");
    }

    [Fact]
    public void CrashingLineageSource_NeverBlocksTheVerdict()
    {
        var impact = Analyzer(new ThrowingLineage()).Analyze("orders-value", OrdersCompatible, SchemaType.Avro);

        Assert.True(impact.IsCompatible);
        Assert.False(impact.LineageUnavailable);
        Assert.Empty(impact.AffectedPipelines);
    }

    /// <summary>Pipeline A: a→b, Pipeline B: b→a — der Walk muss terminieren.</summary>
    private sealed class CyclicLineage : ISchemaLineageSource
    {
        public IReadOnlyList<TopicReader> GetReaders(string topic) => topic switch
        {
            "orders" => [new TopicReader("loop-out", TopicReaderKind.Pipeline, ["loop-topic"])],
            "loop-topic" => [new TopicReader("loop-back", TopicReaderKind.Pipeline, ["orders"])],
            _ => [],
        };
    }

    [Fact]
    public void CyclicPipelines_Terminate_AndVisitEachTopicOnce()
    {
        var impact = Analyzer(new CyclicLineage()).Analyze("orders-value", OrdersCompatible, SchemaType.Avro);

        var downstream = Assert.Single(impact.DownstreamTopics);
        Assert.Equal("loop-topic", downstream.Topic);
    }

    [Theory]
    [InlineData("orders-value", "orders")]
    [InlineData("orders-key", "orders")]
    [InlineData("com.example.Order", "com.example.Order")]
    public void SubjectToTopic_InvertsTheTopicNameStrategy(string subject, string expectedTopic)
    {
        Assert.Equal(expectedTopic, SchemaImpactAnalyzer.SubjectToTopic(subject));
    }
}
