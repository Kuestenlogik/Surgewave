namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Sink node that writes to a topic.
/// </summary>
public sealed class SinkNode<TKey, TValue> : ProcessorNode
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    public string Topic { get; }
    public Action<string, byte[], byte[], long>? OutputHandler { get; set; }

    public SinkNode(string name, string topic, ISerde<TKey> keySerde, ISerde<TValue> valueSerde)
        : base(name)
    {
        Topic = topic;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
    }

    public override void Init(ProcessorContext context) => Context = context;

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        OutputHandler?.Invoke(Topic, key, value, timestamp);
    }

    public override void Close() { }
}
