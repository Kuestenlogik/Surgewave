namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Processor node that transforms values using a stateful transformer with key access.
/// </summary>
internal sealed class TransformValuesNode<TKey, TValue, TNewValue> : ProcessorNode
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly ISerde<TNewValue> _newValueSerde;
    private readonly IValueTransformerWithKey<TKey, TValue, TNewValue> _transformer;
    private readonly string[] _stateStoreNames;

    public TransformValuesNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        ISerde<TNewValue> newValueSerde,
        IValueTransformerWithKey<TKey, TValue, TNewValue> transformer,
        string[] stateStoreNames)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _newValueSerde = newValueSerde;
        _transformer = transformer;
        _stateStoreNames = stateStoreNames;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _transformer.Init(context);
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var v = _valueSerde.Deserialize(value);

        var newValue = _transformer.Transform(k, v);

        var newValueBytes = _newValueSerde.Serialize(newValue);
        ForwardToChildren(key, newValueBytes, timestamp);
    }

    public override void Close()
    {
        _transformer.Close();
    }
}
