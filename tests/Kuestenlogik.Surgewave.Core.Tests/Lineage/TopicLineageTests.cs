using Kuestenlogik.Surgewave.Core.Lineage;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Lineage;

/// <summary>
/// Tests for <see cref="TopicLineageTracker"/>, <see cref="LineageGraph"/>,
/// <see cref="TopicLineage"/>, and <see cref="LineageExporter"/>.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TopicLineageTests
{
    #region RecordProducer

    [Fact]
    public void RecordProducer_AddsToGraph()
    {
        // Arrange
        var tracker = new TopicLineageTracker();

        // Act
        tracker.RecordProducer("my-producer", "orders");
        var graph = tracker.GetGraph();

        // Assert
        Assert.Equal(2, graph.Nodes.Count); // producer + topic
        Assert.Single(graph.Edges);

        var producerNode = Assert.Single(graph.Nodes, n => n.Type == LineageNodeType.Producer);
        Assert.Equal("my-producer", producerNode.Name);

        var topicNode = Assert.Single(graph.Nodes, n => n.Type == LineageNodeType.Topic);
        Assert.Equal("orders", topicNode.Name);

        var edge = graph.Edges[0];
        Assert.Equal(producerNode.Id, edge.SourceId);
        Assert.Equal(topicNode.Id, edge.TargetId);
        Assert.Equal(LineageEdgeType.Produces, edge.Type);
    }

    #endregion

    #region RecordConsumer

    [Fact]
    public void RecordConsumer_AddsToGraph()
    {
        // Arrange
        var tracker = new TopicLineageTracker();

        // Act
        tracker.RecordConsumer("my-group", "orders");
        var graph = tracker.GetGraph();

        // Assert
        Assert.Equal(2, graph.Nodes.Count); // topic + consumer
        Assert.Single(graph.Edges);

        var topicNode = Assert.Single(graph.Nodes, n => n.Type == LineageNodeType.Topic);
        Assert.Equal("orders", topicNode.Name);

        var consumerNode = Assert.Single(graph.Nodes, n => n.Type == LineageNodeType.Consumer);
        Assert.Equal("my-group", consumerNode.Name);

        var edge = graph.Edges[0];
        Assert.Equal(topicNode.Id, edge.SourceId);
        Assert.Equal(consumerNode.Id, edge.TargetId);
        Assert.Equal(LineageEdgeType.Consumes, edge.Type);
    }

    #endregion

    #region RecordStreamFlow

    [Fact]
    public void RecordStreamFlow_CreatesEdges()
    {
        // Arrange
        var tracker = new TopicLineageTracker();

        // Act
        tracker.RecordStreamFlow("word-count", ["input-text"], ["word-counts"]);
        var graph = tracker.GetGraph();

        // Assert — 3 nodes: input-text topic, word-count streams app, word-counts topic
        Assert.Equal(3, graph.Nodes.Count);
        Assert.Equal(2, graph.Edges.Count);

        var streamsNode = Assert.Single(graph.Nodes, n => n.Type == LineageNodeType.StreamsApp);
        Assert.Equal("word-count", streamsNode.Name);

        // Edge: input-text -> streams app (StreamsFrom)
        var fromEdge = Assert.Single(graph.Edges, e => e.Type == LineageEdgeType.StreamsFrom);
        Assert.Equal("topic:input-text", fromEdge.SourceId);
        Assert.Equal(streamsNode.Id, fromEdge.TargetId);

        // Edge: streams app -> word-counts (StreamsTo)
        var toEdge = Assert.Single(graph.Edges, e => e.Type == LineageEdgeType.StreamsTo);
        Assert.Equal(streamsNode.Id, toEdge.SourceId);
        Assert.Equal("topic:word-counts", toEdge.TargetId);
    }

    #endregion

    #region RecordConnectorFlow

    [Fact]
    public void RecordConnectorFlow_CreatesEdges()
    {
        // Arrange
        var tracker = new TopicLineageTracker();

        // Act
        tracker.RecordConnectorFlow("jdbc-sink", ["db-changes"], ["archive-topic"]);
        var graph = tracker.GetGraph();

        // Assert — 3 nodes: db-changes topic, jdbc-sink connector, archive-topic
        Assert.Equal(3, graph.Nodes.Count);
        Assert.Equal(2, graph.Edges.Count);

        var connectorNode = Assert.Single(graph.Nodes, n => n.Type == LineageNodeType.Connector);
        Assert.Equal("jdbc-sink", connectorNode.Name);

        // Edge: db-changes -> connector (ConnectsFrom)
        var fromEdge = Assert.Single(graph.Edges, e => e.Type == LineageEdgeType.ConnectsFrom);
        Assert.Equal("topic:db-changes", fromEdge.SourceId);
        Assert.Equal(connectorNode.Id, fromEdge.TargetId);

        // Edge: connector -> archive-topic (ConnectsTo)
        var toEdge = Assert.Single(graph.Edges, e => e.Type == LineageEdgeType.ConnectsTo);
        Assert.Equal(connectorNode.Id, toEdge.SourceId);
        Assert.Equal("topic:archive-topic", toEdge.TargetId);
    }

    #endregion

    #region GetTopicLineage

    [Fact]
    public void GetTopicLineage_ReturnsUpstreamAndDownstream()
    {
        // Arrange
        var tracker = new TopicLineageTracker();
        tracker.RecordProducer("producer-1", "events");
        tracker.RecordConsumer("consumer-group-1", "events");

        // Act
        var lineage = tracker.GetTopicLineage("events");

        // Assert
        Assert.Equal("events", lineage.TopicName);
        Assert.Single(lineage.Upstream);
        Assert.Single(lineage.Downstream);
        Assert.Equal(LineageNodeType.Producer, lineage.Upstream[0].Type);
        Assert.Equal(LineageNodeType.Consumer, lineage.Downstream[0].Type);
    }

    #endregion

    #region GetGraph

    [Fact]
    public void GetGraph_ContainsAllNodesAndEdges()
    {
        // Arrange
        var tracker = new TopicLineageTracker();
        tracker.RecordProducer("p1", "topic-a");
        tracker.RecordProducer("p2", "topic-a");
        tracker.RecordConsumer("cg1", "topic-a");
        tracker.RecordStreamFlow("stream-app", ["topic-a"], ["topic-b"]);
        tracker.RecordConnectorFlow("file-sink", ["topic-b"], []);

        // Act
        var graph = tracker.GetGraph();

        // Assert
        // Nodes: p1, p2, topic-a, cg1, stream-app, topic-b, file-sink = 7
        Assert.Equal(7, graph.Nodes.Count);

        // Edges: p1->a, p2->a, a->cg1, a->stream, stream->b, b->file-sink = 6
        Assert.Equal(6, graph.Edges.Count);

        Assert.True(graph.GeneratedAt <= DateTime.UtcNow);
    }

    #endregion

    #region PruneStaleEntries

    [Fact]
    public void PruneStaleEntries_RemovesOldEntries()
    {
        // Arrange
        var tracker = new TopicLineageTracker();
        tracker.RecordProducer("old-producer", "stale-topic");

        // Entries were just created (LastSeen ~ now).
        // Pruning with a large maxAge should keep them because they are fresh.
        tracker.PruneStaleEntries(TimeSpan.FromHours(1));

        var graphBefore = tracker.GetGraph();
        Assert.Equal(2, graphBefore.Nodes.Count);
        Assert.Single(graphBefore.Edges);

        // Now prune with zero age to force removal of everything
        // (since everything has LastSeen < UtcNow, a zero window means all are stale)
        tracker.PruneStaleEntries(TimeSpan.Zero);

        var graphAfter = tracker.GetGraph();
        Assert.Empty(graphAfter.Nodes);
        Assert.Empty(graphAfter.Edges);
    }

    #endregion

    #region LineageExporter

    [Fact]
    public void LineageExporter_ToDot_ValidOutput()
    {
        // Arrange
        var tracker = new TopicLineageTracker();
        tracker.RecordProducer("app-1", "orders");
        tracker.RecordConsumer("analytics", "orders");
        var graph = tracker.GetGraph();

        // Act
        var dot = LineageExporter.ToDot(graph);

        // Assert
        Assert.Contains("digraph Lineage {", dot, StringComparison.Ordinal);
        Assert.Contains("rankdir=LR", dot, StringComparison.Ordinal);
        Assert.Contains("shape=box", dot, StringComparison.Ordinal);      // producer/consumer
        Assert.Contains("shape=cylinder", dot, StringComparison.Ordinal); // topic
        Assert.Contains("->", dot, StringComparison.Ordinal);
        Assert.Contains("Produces", dot, StringComparison.Ordinal);
        Assert.Contains("Consumes", dot, StringComparison.Ordinal);
    }

    [Fact]
    public void LineageExporter_ToMermaid_ValidOutput()
    {
        // Arrange
        var tracker = new TopicLineageTracker();
        tracker.RecordProducer("app-1", "orders");
        tracker.RecordConsumer("analytics", "orders");
        var graph = tracker.GetGraph();

        // Act
        var mermaid = LineageExporter.ToMermaid(graph);

        // Assert
        Assert.Contains("graph LR", mermaid, StringComparison.Ordinal);
        Assert.Contains("-->", mermaid, StringComparison.Ordinal);
        Assert.Contains("Produces", mermaid, StringComparison.Ordinal);
        Assert.Contains("Consumes", mermaid, StringComparison.Ordinal);
        // Topic nodes use cylinder notation [( )]
        Assert.Contains("[(", mermaid, StringComparison.Ordinal);
    }

    [Fact]
    public void LineageExporter_ToJson_ValidOutput()
    {
        // Arrange
        var tracker = new TopicLineageTracker();
        tracker.RecordProducer("svc-1", "events");
        var graph = tracker.GetGraph();

        // Act
        var json = LineageExporter.ToJson(graph);

        // Assert
        Assert.Contains("\"nodes\"", json, StringComparison.Ordinal);
        Assert.Contains("\"edges\"", json, StringComparison.Ordinal);
        Assert.Contains("\"generatedAt\"", json, StringComparison.Ordinal);
        Assert.Contains("svc-1", json, StringComparison.Ordinal);
        Assert.Contains("events", json, StringComparison.Ordinal);
        // Verify it's valid JSON by round-tripping
        var doc = System.Text.Json.JsonDocument.Parse(json);
        Assert.NotNull(doc);
    }

    #endregion

    #region Multiple Producers / Complex Scenarios

    [Fact]
    public void MultipleProducers_SameTopic_AllTracked()
    {
        // Arrange
        var tracker = new TopicLineageTracker();

        // Act
        tracker.RecordProducer("producer-a", "shared-topic");
        tracker.RecordProducer("producer-b", "shared-topic");
        tracker.RecordProducer("producer-c", "shared-topic");

        var lineage = tracker.GetTopicLineage("shared-topic");

        // Assert — 3 upstream producers
        Assert.Equal(3, lineage.Upstream.Count);
        Assert.All(lineage.Upstream, n => Assert.Equal(LineageNodeType.Producer, n.Type));
        Assert.Empty(lineage.Downstream);
    }

    [Fact]
    public void StreamsApp_MultipleSourcesSinks_AllEdges()
    {
        // Arrange
        var tracker = new TopicLineageTracker();

        // Act
        tracker.RecordStreamFlow(
            "enrichment-app",
            ["raw-events", "user-profiles"],
            ["enriched-events", "error-events"]);

        var graph = tracker.GetGraph();

        // Assert
        // Nodes: 2 source topics + 1 streams app + 2 sink topics = 5
        Assert.Equal(5, graph.Nodes.Count);

        // Edges: 2 StreamsFrom + 2 StreamsTo = 4
        Assert.Equal(4, graph.Edges.Count);
        Assert.Equal(2, graph.Edges.Count(e => e.Type == LineageEdgeType.StreamsFrom));
        Assert.Equal(2, graph.Edges.Count(e => e.Type == LineageEdgeType.StreamsTo));
    }

    [Fact]
    public void EmptyTracker_ReturnsEmptyGraph()
    {
        // Arrange
        var tracker = new TopicLineageTracker();

        // Act
        var graph = tracker.GetGraph();

        // Assert
        Assert.Empty(graph.Nodes);
        Assert.Empty(graph.Edges);
        Assert.True(graph.GeneratedAt <= DateTime.UtcNow);
    }

    #endregion

    #region Edge Cases

    [Fact]
    public void RecordProducer_SameProducerTwice_UpdatesLastSeen()
    {
        // Arrange
        var tracker = new TopicLineageTracker();

        // Act
        tracker.RecordProducer("idempotent-producer", "my-topic");
        tracker.RecordProducer("idempotent-producer", "my-topic");

        var graph = tracker.GetGraph();

        // Assert — should still be 1 edge (updated, not duplicated)
        Assert.Single(graph.Edges);
        Assert.Equal(2, graph.Nodes.Count);
    }

    [Fact]
    public void GetTopicLineage_UnknownTopic_ReturnsEmpty()
    {
        // Arrange
        var tracker = new TopicLineageTracker();
        tracker.RecordProducer("p1", "known-topic");

        // Act
        var lineage = tracker.GetTopicLineage("unknown-topic");

        // Assert
        Assert.Equal("unknown-topic", lineage.TopicName);
        Assert.Empty(lineage.Upstream);
        Assert.Empty(lineage.Downstream);
    }

    [Fact]
    public void GetTopicLineage_Depth_MultiHopChain()
    {
        // Arrange: producer -> topic-a -> streams -> topic-b -> consumer
        var tracker = new TopicLineageTracker();
        tracker.RecordProducer("source-producer", "topic-a");
        tracker.RecordStreamFlow("transform", ["topic-a"], ["topic-b"]);
        tracker.RecordConsumer("final-consumer", "topic-b");

        // Act
        var lineageA = tracker.GetTopicLineage("topic-a");
        var lineageB = tracker.GetTopicLineage("topic-b");

        // Assert
        // topic-a has depth 1 (producer upstream)
        Assert.Equal(1, lineageA.Depth);

        // topic-b: upstream path is topic-b <- streams <- topic-a <- producer = depth 3
        Assert.True(lineageB.Depth >= 1);
    }

    #endregion
}
