namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Repartition node that writes records to an internal repartition topic with a new key.
/// This enables re-partitioning of data when the key changes (e.g., after SelectKey or GroupBy).
/// The repartition topic is named: {applicationId}-{nodeName}-repartition
/// </summary>
public sealed class RepartitionNode<TKeyIn, TValueIn, TKeyOut> : ProcessorNode
{
    private readonly ISerde<TKeyIn> _keyInSerde;
    private readonly ISerde<TValueIn> _valueSerde;
    private readonly ISerde<TKeyOut> _keyOutSerde;
    private readonly Func<TKeyIn, TValueIn, TKeyOut>? _keySelector;

    /// <summary>
    /// The internal repartition topic name.
    /// </summary>
    public string RepartitionTopic { get; }

    /// <summary>
    /// Handler for outputting records to the repartition topic.
    /// Called with (topic, key, value, timestamp).
    /// </summary>
    public Action<string, byte[], byte[], long>? OutputHandler { get; set; }

    /// <summary>
    /// Creates a repartition node that preserves the key type.
    /// </summary>
    public RepartitionNode(
        string name,
        string applicationId,
        ISerde<TKeyIn> keyInSerde,
        ISerde<TValueIn> valueSerde,
        ISerde<TKeyOut> keyOutSerde)
        : this(name, applicationId, keyInSerde, valueSerde, keyOutSerde, null)
    {
    }

    /// <summary>
    /// Creates a repartition node with a key transformation function.
    /// </summary>
    public RepartitionNode(
        string name,
        string applicationId,
        ISerde<TKeyIn> keyInSerde,
        ISerde<TValueIn> valueSerde,
        ISerde<TKeyOut> keyOutSerde,
        Func<TKeyIn, TValueIn, TKeyOut>? keySelector)
        : base(name)
    {
        _keyInSerde = keyInSerde;
        _valueSerde = valueSerde;
        _keyOutSerde = keyOutSerde;
        _keySelector = keySelector;

        // Internal topic naming convention: {appId}-{nodeName}-repartition
        RepartitionTopic = $"{applicationId}-{name}-repartition";
    }

    public override void Init(ProcessorContext context) => Context = context;

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        byte[] outputKey;

        if (_keySelector != null)
        {
            // Deserialize input, apply key selector, serialize with new key
            var keyIn = _keyInSerde.Deserialize(key);
            var valueIn = _valueSerde.Deserialize(value);
            var newKey = _keySelector(keyIn, valueIn);
            outputKey = _keyOutSerde.Serialize(newKey);
        }
        else
        {
            // Preserve the key as-is (just re-serialize if types match)
            outputKey = key;
        }

        // Write to repartition topic
        OutputHandler?.Invoke(RepartitionTopic, outputKey, value, timestamp);

        // Forward to children (for in-memory processing without actual topic)
        ForwardToChildren(outputKey, value, timestamp);
    }

    public override void Close() { }
}

/// <summary>
/// Repartition node where input and output key types are the same.
/// </summary>
public sealed class RepartitionNode<TKey, TValue> : ProcessorNode
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;

    /// <summary>
    /// The internal repartition topic name.
    /// </summary>
    public string RepartitionTopic { get; }

    /// <summary>
    /// Handler for outputting records to the repartition topic.
    /// </summary>
    public Action<string, byte[], byte[], long>? OutputHandler { get; set; }

    public RepartitionNode(
        string name,
        string applicationId,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        RepartitionTopic = $"{applicationId}-{name}-repartition";
    }

    public override void Init(ProcessorContext context) => Context = context;

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        // Write to repartition topic (same key, for partition redistribution)
        OutputHandler?.Invoke(RepartitionTopic, key, value, timestamp);

        // Forward to children
        ForwardToChildren(key, value, timestamp);
    }

    public override void Close() { }
}
