namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Processor node that performs a stream-globalTable join.
/// For each stream record, extracts a global key via keySelector,
/// looks up the value in the global table's state store, and applies the joiner.
/// </summary>
internal sealed class GlobalTableJoinNode<TKey, TValue, TGlobalKey, TGlobalValue, TResult> : ProcessorNode
    where TGlobalKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly ISerde<TResult> _resultSerde;
    private readonly string _globalStoreName;
    private readonly Func<TKey, TValue, TGlobalKey> _keySelector;
    private readonly Func<TValue, TGlobalValue, TResult> _joiner;
    private IKeyValueStore<TGlobalKey, TGlobalValue>? _globalStore;

    public GlobalTableJoinNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        ISerde<TResult> resultSerde,
        string globalStoreName,
        Func<TKey, TValue, TGlobalKey> keySelector,
        Func<TValue, TGlobalValue, TResult> joiner)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _resultSerde = resultSerde;
        _globalStoreName = globalStoreName;
        _keySelector = keySelector;
        _joiner = joiner;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _globalStore = context.GetStateStore<IKeyValueStore<TGlobalKey, TGlobalValue>>(_globalStoreName);
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var v = _valueSerde.Deserialize(value);

        var globalKey = _keySelector(k, v);

        if (_globalStore == null)
            return;

        var globalValue = _globalStore.Get(globalKey);
        if (EqualityComparer<TGlobalValue>.Default.Equals(globalValue!, default!))
            return;

        var result = _joiner(v, globalValue!);
        var resultBytes = _resultSerde.Serialize(result);
        ForwardToChildren(key, resultBytes, timestamp);
    }

    public override void Close() { }
}
