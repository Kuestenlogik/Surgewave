using Kuestenlogik.Surgewave.Streams.Processors;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// A named sub-topology that groups related processing logic.
/// Multiple named topologies can run independently in a single StreamsApplication.
/// </summary>
public sealed class NamedTopology
{
    /// <summary>
    /// Name of this topology.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// The underlying topology.
    /// </summary>
    public Topology Topology { get; }

    /// <summary>
    /// Source topics consumed by this topology.
    /// </summary>
    public IReadOnlyList<string> SourceTopics { get; }

    public NamedTopology(string name, Topology topology)
    {
        Name = name;
        Topology = topology;

        // Extract source topics
        var topics = new List<string>();
        foreach (var source in topology.Sources)
        {
            var topicProp = source.GetType().GetProperty("TopicPattern");
            if (topicProp?.GetValue(source)?.ToString() is string topic)
            {
                topics.Add(topic);
            }
        }
        SourceTopics = topics;
    }
}

/// <summary>
/// Builder for creating named topologies within a StreamsApplication.
/// </summary>
public sealed class NamedTopologyBuilder
{
    private readonly string _name;
    private readonly StreamsBuilder _innerBuilder;

    public NamedTopologyBuilder(string name)
    {
        _name = name;
        _innerBuilder = new StreamsBuilder { ApplicationId = name };
    }

    /// <summary>
    /// Name of this topology.
    /// </summary>
    public string Name => _name;

    /// <summary>
    /// Creates a Stream from a topic within this named topology.
    /// </summary>
    public IStream<TKey, TValue> Stream<TKey, TValue>(string topic)
        => _innerBuilder.Stream<TKey, TValue>(topic);

    /// <summary>
    /// Creates a Stream from a topic with specific serdes.
    /// </summary>
    public IStream<TKey, TValue> Stream<TKey, TValue>(string topic, Consumed<TKey, TValue> consumed)
        => _innerBuilder.Stream<TKey, TValue>(topic, consumed);

    /// <summary>
    /// Creates a Table from a topic within this named topology.
    /// </summary>
    public ITable<TKey, TValue> Table<TKey, TValue>(string topic)
        where TKey : notnull
        => _innerBuilder.Table<TKey, TValue>(topic);

    /// <summary>
    /// Adds a state store to this named topology.
    /// </summary>
    public NamedTopologyBuilder AddStateStore<TStore>(IStoreSupplier<TStore> storeSupplier)
        where TStore : IStateStore
    {
        _innerBuilder.AddStateStore(storeSupplier);
        return this;
    }

    /// <summary>
    /// Builds the named topology.
    /// </summary>
    public NamedTopology Build()
    {
        return new NamedTopology(_name, _innerBuilder.Build());
    }
}
