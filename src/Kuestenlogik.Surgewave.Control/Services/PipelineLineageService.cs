using Kuestenlogik.Surgewave.Control.Models.Pipeline;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Builds a cross-pipeline lineage graph showing how pipelines are connected through shared topics.
/// </summary>
public sealed class PipelineLineageService
{
    /// <summary>
    /// Analyzes all pipelines and builds a graph of topic-based connections between them.
    /// </summary>
    public PipelineLineageGraph BuildGraph(IReadOnlyList<PipelineDefinition> pipelines)
    {
        var entries = new List<PipelineLineageEntry>();
        var connections = new List<TopicConnection>();

        // Extract source/sink topics per pipeline
        foreach (var pipeline in pipelines)
        {
            var sourceTopics = new List<string>();
            var sinkTopics = new List<string>();

            var targetNodeIds = pipeline.Connections
                .Select(c => c.TargetNodeId)
                .ToHashSet(StringComparer.Ordinal);
            var sourceNodeIds = pipeline.Connections
                .Select(c => c.SourceNodeId)
                .ToHashSet(StringComparer.Ordinal);

            foreach (var node in pipeline.Nodes)
            {
                // Entry-point nodes (no incoming connections) → source topics
                if (!targetNodeIds.Contains(node.Id))
                {
                    var topic = ExtractTopic(node.Config);
                    if (topic != null) sourceTopics.Add(topic);
                }

                // Exit-point nodes (no outgoing connections) → sink topics
                if (!sourceNodeIds.Contains(node.Id))
                {
                    var topic = ExtractTopic(node.Config);
                    if (topic != null) sinkTopics.Add(topic);
                }
            }

            entries.Add(new PipelineLineageEntry
            {
                PipelineId = pipeline.Id ?? "",
                Name = pipeline.Name,
                Status = pipeline.Status,
                NodeCount = pipeline.Nodes.Count,
                SourceTopics = sourceTopics.Distinct().ToList(),
                SinkTopics = sinkTopics.Distinct().ToList()
            });
        }

        // Find connections: Pipeline A sink topic == Pipeline B source topic
        for (var i = 0; i < entries.Count; i++)
        {
            for (var j = 0; j < entries.Count; j++)
            {
                if (i == j) continue;

                var producer = entries[i];
                var consumer = entries[j];

                foreach (var sinkTopic in producer.SinkTopics)
                {
                    if (consumer.SourceTopics.Contains(sinkTopic))
                    {
                        connections.Add(new TopicConnection
                        {
                            TopicName = sinkTopic,
                            ProducerPipelineId = producer.PipelineId,
                            ConsumerPipelineId = consumer.PipelineId
                        });
                    }
                }
            }
        }

        return new PipelineLineageGraph
        {
            Pipelines = entries,
            Connections = connections
        };
    }

    private static string? ExtractTopic(Dictionary<string, string> config)
    {
        // Try common config key patterns for topics
        foreach (var key in config.Keys)
        {
            if (key.Contains("topic", StringComparison.OrdinalIgnoreCase) &&
                !string.IsNullOrWhiteSpace(config[key]))
            {
                return config[key];
            }
        }
        return null;
    }
}

public sealed record PipelineLineageGraph
{
    public List<PipelineLineageEntry> Pipelines { get; init; } = [];
    public List<TopicConnection> Connections { get; init; } = [];
}

public sealed record PipelineLineageEntry
{
    public required string PipelineId { get; init; }
    public required string Name { get; init; }
    public PipelineStatus Status { get; init; }
    public int NodeCount { get; init; }
    public List<string> SourceTopics { get; init; } = [];
    public List<string> SinkTopics { get; init; } = [];
}

public sealed record TopicConnection
{
    public required string TopicName { get; init; }
    public required string ProducerPipelineId { get; init; }
    public required string ConsumerPipelineId { get; init; }
}
