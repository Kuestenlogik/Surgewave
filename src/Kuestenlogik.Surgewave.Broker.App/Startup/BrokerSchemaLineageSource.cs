using Kuestenlogik.Surgewave.Broker.StreamsGroups;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Kuestenlogik.Surgewave.Schema.Registry.Lineage;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// The broker host's answer to "who reads this topic?" (#13) — assembled on demand from live
/// control-plane state, never recorded per request:
///
/// <list type="bullet">
/// <item>Consumer groups from committed offsets (<see cref="OffsetStore"/>) — the same source
/// the lag calculator uses; both protocol coordinators commit into it, so this is
/// protocol-agnostic. A group appears after its first commit, which is exactly the population
/// an impact analysis cares about.</item>
/// <item>Streams apps from <see cref="StreamsGroupCoordinator.ListApplications"/> — source
/// topics only; the wire protocol does not carry sink topics.</item>
/// <item>Connect pipelines from <see cref="PipelineOrchestratorHolder"/> — entry nodes are
/// sources, exit nodes are sinks (same heuristic the Control UI's lineage view uses), which
/// is what makes the transitive downstream walk possible.</item>
/// </list>
/// </summary>
internal sealed class BrokerSchemaLineageSource : ISchemaLineageSource
{
    private readonly OffsetStore _offsetStore;
    private readonly StreamsGroupCoordinator _streamsGroups;

    public BrokerSchemaLineageSource(OffsetStore offsetStore, StreamsGroupCoordinator streamsGroups)
    {
        _offsetStore = offsetStore;
        _streamsGroups = streamsGroups;
    }

    public IReadOnlyList<TopicReader> GetReaders(string topic)
    {
        var readers = new List<TopicReader>();

        foreach (var groupId in _offsetStore.GetGroupIds())
        {
            // Offset-Keys sind "topic:partition"; Topic-Namen dürfen ':' nicht enthalten,
            // der letzte Doppelpunkt trennt (gleiches Muster wie LoadAllOffsets).
            foreach (var key in _offsetStore.GetAllOffsets(groupId).Keys)
            {
                var sep = key.LastIndexOf(':');
                if (sep > 0 && key.AsSpan(0, sep).SequenceEqual(topic))
                {
                    readers.Add(new TopicReader(groupId, TopicReaderKind.ConsumerGroup));
                    break;
                }
            }
        }

        foreach (var (groupId, sourceTopics) in _streamsGroups.ListApplications())
        {
            if (sourceTopics.Contains(topic, StringComparer.Ordinal))
            {
                readers.Add(new TopicReader(groupId, TopicReaderKind.StreamsApp));
            }
        }

        var orchestrator = PipelineOrchestratorHolder.Instance;
        if (orchestrator is not null)
        {
            foreach (var pipeline in orchestrator.GetAll())
            {
                var (sources, sinks) = ExtractTopics(pipeline);
                if (sources.Contains(topic, StringComparer.Ordinal))
                {
                    readers.Add(new TopicReader(
                        pipeline.Name ?? pipeline.Id ?? "pipeline",
                        TopicReaderKind.Pipeline,
                        sinks));
                }
            }
        }

        return readers;
    }

    /// <summary>Entry-Knoten (ohne eingehende Verbindung) lesen, Exit-Knoten (ohne
    /// ausgehende) schreiben — die Heuristik der Control-Lineage-Ansicht.</summary>
    private static (List<string> Sources, List<string> Sinks) ExtractTopics(
        PipelineDefinition pipeline)
    {
        var targets = pipeline.Connections.Select(c => c.TargetNodeId).ToHashSet(StringComparer.Ordinal);
        var origins = pipeline.Connections.Select(c => c.SourceNodeId).ToHashSet(StringComparer.Ordinal);
        var sources = new List<string>();
        var sinks = new List<string>();

        foreach (var node in pipeline.Nodes)
        {
            var topic = ExtractTopic(node.Config);
            if (topic is null)
            {
                continue;
            }

            if (!targets.Contains(node.Id))
            {
                sources.Add(topic);
            }

            if (!origins.Contains(node.Id))
            {
                sinks.Add(topic);
            }
        }

        return (sources, sinks);
    }

    private static string? ExtractTopic(Dictionary<string, string> config)
    {
        foreach (var (key, value) in config)
        {
            if (key.Contains("topic", StringComparison.OrdinalIgnoreCase) &&
                !string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
        }

        return null;
    }
}
