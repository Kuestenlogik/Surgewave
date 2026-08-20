using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Schema.Registry.Lineage;

/// <summary>
/// Turns "is this schema compatible?" into "what breaks if it ships?" (#13). The compatibility
/// verdict comes from the same store/checker pair the register endpoint already uses; lineage
/// adds the names: direct readers of the subject's topic, and the topics transitively fed
/// through readers whose sink side is visible (Connect pipelines/connectors — plain consumer
/// groups and wire-visible Streams topologies only expose their source side).
/// </summary>
public sealed class SchemaImpactAnalyzer
{
    private readonly SchemaStore _store;
    private readonly CompatibilityChecker _checker;
    private readonly ISchemaLineageSource? _lineage;
    private readonly ILogger<SchemaImpactAnalyzer> _logger;

    public SchemaImpactAnalyzer(
        SchemaStore store,
        CompatibilityChecker checker,
        ILogger<SchemaImpactAnalyzer> logger,
        ISchemaLineageSource? lineage = null)
    {
        _store = store;
        _checker = checker;
        _logger = logger;
        _lineage = lineage;
    }

    public SchemaChangeImpact Analyze(string subject, string schemaString, SchemaType schemaType)
    {
        var mode = _store.GetCompatibility(subject);
        var existing = _store.GetSchemasForCompatibilityCheck(subject, mode);
        var compat = _checker.CheckCompatibility(schemaString, schemaType, existing, mode);

        var topic = SubjectToTopic(subject);
        var (affected, downstream) = WalkDownstream(topic);

        return new SchemaChangeImpact
        {
            Subject = subject,
            Topic = topic,
            IsCompatible = compat.IsCompatible,
            CompatibilityErrors = compat.Messages ?? [],
            AffectedPipelines = affected,
            DownstreamTopics = downstream,
            LineageUnavailable = _lineage is null,
        };
    }

    /// <summary>
    /// TopicNameStrategy inverse: "orders-value"/"orders-key" → "orders". Subjects from the
    /// record-name strategies don't map to a topic; they keep their own name, which simply
    /// yields no lineage readers.
    /// </summary>
    internal static string SubjectToTopic(string subject)
    {
        if (subject.EndsWith("-value", StringComparison.Ordinal))
        {
            return subject[..^"-value".Length];
        }

        if (subject.EndsWith("-key", StringComparison.Ordinal))
        {
            return subject[..^"-key".Length];
        }

        return subject;
    }

    private (IReadOnlyList<AffectedPipeline> Affected, IReadOnlyList<DownstreamTopic> Downstream)
        WalkDownstream(string rootTopic)
    {
        if (_lineage is null)
        {
            return ([], []);
        }

        var affected = new List<AffectedPipeline>();
        var downstream = new List<DownstreamTopic>();
        var visitedTopics = new HashSet<string>(StringComparer.Ordinal) { rootTopic };
        var queue = new Queue<string>();
        queue.Enqueue(rootTopic);

        while (queue.Count > 0)
        {
            var topic = queue.Dequeue();

            IReadOnlyList<TopicReader> readers;
            try
            {
                readers = _lineage.GetReaders(topic);
            }
            catch (Exception ex)
            {
                // Lineage ist Auskunft, nicht Gate-Bedingung: eine kaputte Quelle darf die
                // Registrierung nicht blockieren, nur die Namensliste verkürzen.
                _logger.LogWarning(ex, "Lineage source failed for topic {Topic}; impact list is incomplete", topic);
                continue;
            }

            foreach (var reader in readers)
            {
                if (topic == rootTopic)
                {
                    affected.Add(new AffectedPipeline(reader.Name, KindName(reader.Kind)));
                }

                foreach (var sink in reader.SinkTopics)
                {
                    if (!visitedTopics.Add(sink))
                    {
                        continue;
                    }

                    var sinkSubject = sink + "-value";
                    var latest = _store.GetLatestSchema(sinkSubject);
                    downstream.Add(new DownstreamTopic(
                        sink,
                        reader.Name,
                        latest is null ? null : sinkSubject,
                        latest?.Version));
                    queue.Enqueue(sink);
                }
            }
        }

        return (affected, downstream);
    }

    private static string KindName(TopicReaderKind kind) => kind switch
    {
        TopicReaderKind.ConsumerGroup => "consumer-group",
        TopicReaderKind.StreamsApp => "streams-app",
        TopicReaderKind.Connector => "connector",
        TopicReaderKind.Pipeline => "pipeline",
        _ => kind.ToString().ToLowerInvariant(),
    };
}
