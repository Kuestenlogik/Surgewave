namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Sink node that routes records to dynamically computed topics.
/// The topic is determined per-record by evaluating the topicExtractor function.
/// </summary>
internal sealed class DynamicSinkNode<TKey, TValue> : ProcessorNode
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly Func<TKey, TValue, string> _topicExtractor;
    public Action<string, byte[], byte[], long>? OutputHandler { get; set; }

    public DynamicSinkNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        Func<TKey, TValue, string> topicExtractor)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _topicExtractor = topicExtractor;
    }

    public override void Init(ProcessorContext context) => Context = context;

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var v = _valueSerde.Deserialize(value);
        var topic = _topicExtractor(k, v);
        OutputHandler?.Invoke(topic, key, value, timestamp);
    }

    public override void Close() { }
}
