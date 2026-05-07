namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Processor node that applies a transformation.
/// </summary>
public sealed class ProcessorNodeImpl<TKeyIn, TValueIn, TKeyOut, TValueOut> : ProcessorNode
{
    private readonly ISerde<TKeyIn> _keyInSerde;
    private readonly ISerde<TValueIn> _valueInSerde;
    private readonly ISerde<TKeyOut> _keyOutSerde;
    private readonly ISerde<TValueOut> _valueOutSerde;
    private readonly Func<TKeyIn, TValueIn, IEnumerable<KeyValue<TKeyOut, TValueOut>>> _processor;

    public ProcessorNodeImpl(
        string name,
        ISerde<TKeyIn> keyInSerde,
        ISerde<TValueIn> valueInSerde,
        ISerde<TKeyOut> keyOutSerde,
        ISerde<TValueOut> valueOutSerde,
        Func<TKeyIn, TValueIn, IEnumerable<KeyValue<TKeyOut, TValueOut>>> processor)
        : base(name)
    {
        _keyInSerde = keyInSerde;
        _valueInSerde = valueInSerde;
        _keyOutSerde = keyOutSerde;
        _valueOutSerde = valueOutSerde;
        _processor = processor;
    }

    public override void Init(ProcessorContext context) => Context = context;

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var keyIn = _keyInSerde.Deserialize(key);
        // Empty value bytes represent a tombstone (delete). Pass default(TValueIn) to the processor.
        var valueIn = value.Length > 0 ? _valueInSerde.Deserialize(value) : default!;

        foreach (var result in _processor(keyIn, valueIn))
        {
            var keyOut = _keyOutSerde.Serialize(result.Key);
            var valueOut = _valueOutSerde.Serialize(result.Value);
            ForwardToChildren(keyOut, valueOut, timestamp);
        }
    }

    public override void Close() { }
}
