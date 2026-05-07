namespace Kuestenlogik.Surgewave.Linq;

/// <summary>
/// Describes a Surgewave topic as a queryable data source.
/// </summary>
internal sealed class TopicQuerySource<T> where T : class
{
    public string Topic { get; }
    public SurgewaveQueryOptions Options { get; }
    public int? Partition { get; init; }
    public long FromOffset { get; init; }
    public long? ToOffset { get; init; }

    public TopicQuerySource(string topic, SurgewaveQueryOptions options)
    {
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));
        Options = options ?? throw new ArgumentNullException(nameof(options));
    }
}
