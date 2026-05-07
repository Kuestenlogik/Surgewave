namespace Kuestenlogik.Surgewave.Linq;

/// <summary>
/// Entry point for LINQ queries against Surgewave topics.
/// </summary>
public sealed class SurgewaveQueryContext
{
    private readonly SurgewaveQueryOptions _options;

    public SurgewaveQueryContext(SurgewaveQueryOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public SurgewaveQueryContext(string bootstrapServers)
        : this(new SurgewaveQueryOptions { BootstrapServers = bootstrapServers })
    {
    }

    /// <summary>
    /// Creates a queryable source for a Surgewave topic.
    /// Messages are deserialized as JSON by default.
    /// </summary>
    public IQueryable<T> Query<T>(string topic) where T : class
    {
        var source = new TopicQuerySource<T>(topic, _options);
        return new SurgewaveQueryable<T>(new SurgewaveQueryProvider(source), source);
    }

    /// <summary>
    /// Creates a queryable source for a specific partition and offset range.
    /// </summary>
    public IQueryable<T> Query<T>(string topic, int partition, long fromOffset = 0, long? toOffset = null) where T : class
    {
        var source = new TopicQuerySource<T>(topic, _options)
        {
            Partition = partition,
            FromOffset = fromOffset,
            ToOffset = toOffset
        };
        return new SurgewaveQueryable<T>(new SurgewaveQueryProvider(source), source);
    }
}
