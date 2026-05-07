using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Core.Lineage;

/// <summary>
/// Thread-safe central tracker that records data flow relationships between
/// producers, consumers, streams applications, connectors, and topics.
/// </summary>
public sealed class TopicLineageTracker
{
    private readonly ConcurrentDictionary<string, LineageNode> _nodes = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, LineageEdge> _edges = new(StringComparer.Ordinal);

    /// <summary>
    /// Records that a producer writes to a topic.
    /// </summary>
    public void RecordProducer(string clientId, string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        var now = DateTime.UtcNow;

        var producerNodeId = $"producer:{clientId}";
        var topicNodeId = $"topic:{topic}";

        EnsureNode(producerNodeId, clientId, LineageNodeType.Producer);
        EnsureNode(topicNodeId, topic, LineageNodeType.Topic);

        var edgeKey = $"{producerNodeId}->{topicNodeId}";
        _edges.AddOrUpdate(
            edgeKey,
            _ => new LineageEdge
            {
                SourceId = producerNodeId,
                TargetId = topicNodeId,
                Type = LineageEdgeType.Produces,
                FirstSeen = now,
                LastSeen = now
            },
            (_, existing) =>
            {
                existing.LastSeen = now;
                return existing;
            });
    }

    /// <summary>
    /// Records that a consumer group reads from a topic.
    /// </summary>
    public void RecordConsumer(string groupId, string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        var now = DateTime.UtcNow;

        var topicNodeId = $"topic:{topic}";
        var consumerNodeId = $"consumer:{groupId}";

        EnsureNode(topicNodeId, topic, LineageNodeType.Topic);
        EnsureNode(consumerNodeId, groupId, LineageNodeType.Consumer);

        var edgeKey = $"{topicNodeId}->{consumerNodeId}";
        _edges.AddOrUpdate(
            edgeKey,
            _ => new LineageEdge
            {
                SourceId = topicNodeId,
                TargetId = consumerNodeId,
                Type = LineageEdgeType.Consumes,
                FirstSeen = now,
                LastSeen = now
            },
            (_, existing) =>
            {
                existing.LastSeen = now;
                return existing;
            });
    }

    /// <summary>
    /// Records that a Streams application reads from source topics and writes to sink topics.
    /// </summary>
    public void RecordStreamFlow(string applicationId, IEnumerable<string> sourceTopics, IEnumerable<string> sinkTopics)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(applicationId);
        ArgumentNullException.ThrowIfNull(sourceTopics);
        ArgumentNullException.ThrowIfNull(sinkTopics);

        var now = DateTime.UtcNow;

        var streamsNodeId = $"streams:{applicationId}";
        EnsureNode(streamsNodeId, applicationId, LineageNodeType.StreamsApp);

        foreach (var source in sourceTopics)
        {
            var topicNodeId = $"topic:{source}";
            EnsureNode(topicNodeId, source, LineageNodeType.Topic);

            var edgeKey = $"{topicNodeId}->{streamsNodeId}";
            _edges.AddOrUpdate(
                edgeKey,
                _ => new LineageEdge
                {
                    SourceId = topicNodeId,
                    TargetId = streamsNodeId,
                    Type = LineageEdgeType.StreamsFrom,
                    FirstSeen = now,
                    LastSeen = now
                },
                (_, existing) =>
                {
                    existing.LastSeen = now;
                    return existing;
                });
        }

        foreach (var sink in sinkTopics)
        {
            var topicNodeId = $"topic:{sink}";
            EnsureNode(topicNodeId, sink, LineageNodeType.Topic);

            var edgeKey = $"{streamsNodeId}->{topicNodeId}";
            _edges.AddOrUpdate(
                edgeKey,
                _ => new LineageEdge
                {
                    SourceId = streamsNodeId,
                    TargetId = topicNodeId,
                    Type = LineageEdgeType.StreamsTo,
                    FirstSeen = now,
                    LastSeen = now
                },
                (_, existing) =>
                {
                    existing.LastSeen = now;
                    return existing;
                });
        }
    }

    /// <summary>
    /// Records that a Connect connector reads from source topics and/or writes to sink topics.
    /// </summary>
    public void RecordConnectorFlow(string connectorName, IEnumerable<string> sourceTopics, IEnumerable<string> sinkTopics)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectorName);
        ArgumentNullException.ThrowIfNull(sourceTopics);
        ArgumentNullException.ThrowIfNull(sinkTopics);

        var now = DateTime.UtcNow;

        var connectorNodeId = $"connector:{connectorName}";
        EnsureNode(connectorNodeId, connectorName, LineageNodeType.Connector);

        foreach (var source in sourceTopics)
        {
            var topicNodeId = $"topic:{source}";
            EnsureNode(topicNodeId, source, LineageNodeType.Topic);

            var edgeKey = $"{topicNodeId}->{connectorNodeId}";
            _edges.AddOrUpdate(
                edgeKey,
                _ => new LineageEdge
                {
                    SourceId = topicNodeId,
                    TargetId = connectorNodeId,
                    Type = LineageEdgeType.ConnectsFrom,
                    FirstSeen = now,
                    LastSeen = now
                },
                (_, existing) =>
                {
                    existing.LastSeen = now;
                    return existing;
                });
        }

        foreach (var sink in sinkTopics)
        {
            var topicNodeId = $"topic:{sink}";
            EnsureNode(topicNodeId, sink, LineageNodeType.Topic);

            var edgeKey = $"{connectorNodeId}->{topicNodeId}";
            _edges.AddOrUpdate(
                edgeKey,
                _ => new LineageEdge
                {
                    SourceId = connectorNodeId,
                    TargetId = topicNodeId,
                    Type = LineageEdgeType.ConnectsTo,
                    FirstSeen = now,
                    LastSeen = now
                },
                (_, existing) =>
                {
                    existing.LastSeen = now;
                    return existing;
                });
        }
    }

    /// <summary>
    /// Returns the full lineage graph with all tracked nodes and edges.
    /// </summary>
    public LineageGraph GetGraph()
    {
        return new LineageGraph
        {
            Nodes = _nodes.Values.ToList(),
            Edges = _edges.Values.ToList(),
            GeneratedAt = DateTime.UtcNow
        };
    }

    /// <summary>
    /// Returns the upstream/downstream lineage for a specific topic.
    /// </summary>
    public TopicLineage GetTopicLineage(string topicName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);

        var topicNodeId = $"topic:{topicName}";

        // Upstream: nodes that have an edge pointing TO this topic
        var upstream = _edges.Values
            .Where(e => e.TargetId == topicNodeId)
            .Select(e => _nodes.GetValueOrDefault(e.SourceId))
            .Where(n => n is not null)
            .ToList();

        // Downstream: nodes that this topic has edges pointing TO
        var downstream = _edges.Values
            .Where(e => e.SourceId == topicNodeId)
            .Select(e => _nodes.GetValueOrDefault(e.TargetId))
            .Where(n => n is not null)
            .ToList();

        var depth = ComputeDepth(topicNodeId);

        return new TopicLineage
        {
            TopicName = topicName,
            Upstream = upstream!,
            Downstream = downstream!,
            Depth = depth
        };
    }

    /// <summary>
    /// Removes edges that have not been seen within the specified time window.
    /// Nodes that become orphaned (no remaining edges) are also removed.
    /// </summary>
    public void PruneStaleEntries(TimeSpan maxAge)
    {
        var cutoff = DateTime.UtcNow - maxAge;

        // Remove stale edges
        var staleEdgeKeys = _edges
            .Where(kvp => kvp.Value.LastSeen < cutoff)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var key in staleEdgeKeys)
        {
            _edges.TryRemove(key, out _);
        }

        // Remove orphaned nodes (nodes with no remaining edges)
        var referencedNodeIds = _edges.Values
            .SelectMany(e => new[] { e.SourceId, e.TargetId })
            .ToHashSet(StringComparer.Ordinal);

        var orphanedNodeKeys = _nodes.Keys
            .Where(id => !referencedNodeIds.Contains(id))
            .ToList();

        foreach (var key in orphanedNodeKeys)
        {
            _nodes.TryRemove(key, out _);
        }
    }

    /// <summary>
    /// Computes the maximum upstream depth for a node in the DAG using BFS.
    /// </summary>
    private int ComputeDepth(string nodeId)
    {
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var queue = new Queue<(string Id, int Depth)>();
        queue.Enqueue((nodeId, 0));
        var maxDepth = 0;

        while (queue.Count > 0)
        {
            var (currentId, currentDepth) = queue.Dequeue();

            if (!visited.Add(currentId))
                continue;

            if (currentDepth > maxDepth)
                maxDepth = currentDepth;

            // Walk upstream: find edges that point TO currentId
            foreach (var edge in _edges.Values)
            {
                if (edge.TargetId == currentId && !visited.Contains(edge.SourceId))
                {
                    queue.Enqueue((edge.SourceId, currentDepth + 1));
                }
            }
        }

        return maxDepth;
    }

    /// <summary>
    /// Ensures a node exists in the graph. Thread-safe via ConcurrentDictionary.
    /// </summary>
    private void EnsureNode(string nodeId, string name, LineageNodeType type)
    {
        _nodes.GetOrAdd(nodeId, _ => new LineageNode
        {
            Id = nodeId,
            Name = name,
            Type = type
        });
    }
}
