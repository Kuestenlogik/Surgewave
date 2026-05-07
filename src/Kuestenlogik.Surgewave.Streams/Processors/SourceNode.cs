namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Non-generic interface exposing the topic pattern of a source node.
/// Used for routing tombstone records without knowing the value type at compile time.
/// </summary>
internal interface ITopicSource
{
    /// <summary>Gets the topic name or pattern this source node subscribes to.</summary>
    string TopicPattern { get; }
}

/// <summary>
/// Source node that reads from a topic.
/// </summary>
public sealed class SourceNode<TKey, TValue> : ProcessorNode, ITopicSource
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    public string TopicPattern { get; }

    public SourceNode(string name, string topicPattern, ISerde<TKey> keySerde, ISerde<TValue> valueSerde)
        : base(name)
    {
        TopicPattern = topicPattern;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
    }

    public override void Init(ProcessorContext context) => Context = context;

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        // Source nodes simply forward to children
        ForwardToChildren(key, value, timestamp);
    }

    public override void Close() { }

    public TKey DeserializeKey(byte[] data) => _keySerde.Deserialize(data);
    public TValue DeserializeValue(byte[] data) => _valueSerde.Deserialize(data);
}
