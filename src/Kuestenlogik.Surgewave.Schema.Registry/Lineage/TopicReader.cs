namespace Kuestenlogik.Surgewave.Schema.Registry.Lineage;

/// <summary>What kind of thing reads a topic.</summary>
public enum TopicReaderKind
{
    ConsumerGroup,
    StreamsApp,
    Connector,
    Pipeline,
}

/// <summary>
/// One reader of a topic, as reported by an <see cref="ISchemaLineageSource"/>.
/// <paramref name="SinkTopics"/> lists where this reader writes onward, when its topology
/// makes that visible (Connect pipelines and connectors do; plain consumer groups do not,
/// and Streams apps expose only their source side over the wire).
/// </summary>
public sealed record TopicReader(
    string Name,
    TopicReaderKind Kind,
    IReadOnlyList<string> SinkTopics)
{
    public TopicReader(string name, TopicReaderKind kind)
        : this(name, kind, [])
    {
    }
}
