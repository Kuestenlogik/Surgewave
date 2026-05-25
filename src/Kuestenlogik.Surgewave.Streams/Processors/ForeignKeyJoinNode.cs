namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Processor node for foreign key table-table joins.
/// Enables joining tables where the primary table references the foreign table
/// via a key extractor (e.g., orders.Join(customers, o => o.CustomerId)).
/// </summary>
internal sealed class ForeignKeyJoinNode<TPrimaryKey, TPrimaryValue, TForeignKey, TForeignValue, TResult>
    : ProcessorNode
    where TPrimaryKey : notnull
    where TForeignKey : notnull
{
    private readonly ISerde<TPrimaryKey> _primaryKeySerde;
    private readonly ISerde<TPrimaryValue> _primaryValueSerde;
    private readonly ISerde<TForeignKey> _foreignKeySerde;
    private readonly ISerde<TForeignValue> _foreignValueSerde;
    private readonly ISerde<TResult> _resultSerde;
    private readonly Func<TPrimaryValue, TForeignKey> _foreignKeyExtractor;
    private readonly Func<TPrimaryValue, TForeignValue?, TResult> _joiner;
    private readonly bool _leftJoin;
    private readonly string _primaryStoreName;
    private readonly string _foreignStoreName;

    private IKeyValueStore<TPrimaryKey, TPrimaryValue>? _primaryStore;
    private IKeyValueStore<TForeignKey, TForeignValue>? _foreignStore;
    private readonly ForeignKeySubscriptionStore<TPrimaryKey, TForeignKey> _subscriptionStore = new();

    public string PrimaryStoreName => _primaryStoreName;
    public string ForeignStoreName => _foreignStoreName;

    public ForeignKeyJoinNode(
        string name,
        ISerde<TPrimaryKey> primaryKeySerde,
        ISerde<TPrimaryValue> primaryValueSerde,
        ISerde<TForeignKey> foreignKeySerde,
        ISerde<TForeignValue> foreignValueSerde,
        ISerde<TResult> resultSerde,
        Func<TPrimaryValue, TForeignKey> foreignKeyExtractor,
        Func<TPrimaryValue, TForeignValue?, TResult> joiner,
        bool leftJoin,
        string primaryStoreName,
        string foreignStoreName)
        : base(name)
    {
        _primaryKeySerde = primaryKeySerde;
        _primaryValueSerde = primaryValueSerde;
        _foreignKeySerde = foreignKeySerde;
        _foreignValueSerde = foreignValueSerde;
        _resultSerde = resultSerde;
        _foreignKeyExtractor = foreignKeyExtractor;
        _joiner = joiner;
        _leftJoin = leftJoin;
        _primaryStoreName = primaryStoreName;
        _foreignStoreName = foreignStoreName;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _primaryStore = context.GetStateStore<IKeyValueStore<TPrimaryKey, TPrimaryValue>>(_primaryStoreName);
        _foreignStore = context.GetStateStore<IKeyValueStore<TForeignKey, TForeignValue>>(_foreignStoreName);
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        // Default: treat as primary side
        var pk = _primaryKeySerde.Deserialize(key);
        var pv = value.Length > 0 ? _primaryValueSerde.Deserialize(value) : default;
        ProcessPrimary(pk, pv, timestamp);
    }

    /// <summary>
    /// Process an update from the primary table.
    /// </summary>
    public void ProcessPrimary(TPrimaryKey pk, TPrimaryValue? value, long timestamp)
    {
        if (_primaryStore == null || _foreignStore == null)
            return;

        var oldFk = _subscriptionStore.GetForeignKey(pk);

        if (value == null)
        {
            // Tombstone: unsubscribe and delete
            _subscriptionStore.Unsubscribe(pk, oldFk);
            _primaryStore.Delete(pk);
            EmitTombstone(pk, timestamp);
            return;
        }

        // Store primary value
        _primaryStore.Put(pk, value);

        // Extract FK and update subscription
        var newFk = _foreignKeyExtractor(value);
        _subscriptionStore.UpdateSubscription(pk, oldFk, newFk);

        // Lookup foreign value and emit
        var foreignValue = _foreignStore.Get(newFk);
        if (foreignValue != null || _leftJoin)
        {
            var result = _joiner(value, foreignValue);
            EmitResult(pk, result, timestamp);
        }
    }

    /// <summary>
    /// Process an update from the foreign table.
    /// </summary>
    public void ProcessForeign(TForeignKey fk, TForeignValue? value, long timestamp)
    {
        if (_primaryStore == null || _foreignStore == null)
            return;

        if (value == null)
        {
            _foreignStore.Delete(fk);
        }
        else
        {
            _foreignStore.Put(fk, value);
        }

        // Re-join all primary keys that reference this FK
        var subscribers = _subscriptionStore.GetSubscribers(fk);
        foreach (var pk in subscribers)
        {
            var primaryValue = _primaryStore.Get(pk);
            if (primaryValue == null)
                continue;

            if (value != null || _leftJoin)
            {
                var result = _joiner(primaryValue, value);
                EmitResult(pk, result, timestamp);
            }
            else
            {
                // Foreign deleted, inner join → tombstone
                EmitTombstone(pk, timestamp);
            }
        }
    }

    private void EmitResult(TPrimaryKey pk, TResult result, long timestamp)
    {
        var keyBytes = _primaryKeySerde.Serialize(pk);
        var resultBytes = _resultSerde.Serialize(result);
        ForwardToChildren(keyBytes, resultBytes, timestamp);
    }

    private void EmitTombstone(TPrimaryKey pk, long timestamp)
    {
        var keyBytes = _primaryKeySerde.Serialize(pk);
        ForwardToChildren(keyBytes, [], timestamp);
    }

    public override void Close()
    {
        _primaryStore?.Close();
        _foreignStore?.Close();
    }
}

/// <summary>
/// Input node routing records to the primary side of a foreign key join.
/// </summary>
internal sealed class ForeignKeyJoinPrimaryInputNode<TPrimaryKey, TPrimaryValue, TForeignKey, TForeignValue, TResult>
    : ProcessorNode
    where TPrimaryKey : notnull
    where TForeignKey : notnull
{
    private readonly ISerde<TPrimaryKey> _keySerde;
    private readonly ISerde<TPrimaryValue> _valueSerde;
    private readonly ForeignKeyJoinNode<TPrimaryKey, TPrimaryValue, TForeignKey, TForeignValue, TResult> _joinNode;

    public ForeignKeyJoinPrimaryInputNode(
        string name,
        ISerde<TPrimaryKey> keySerde,
        ISerde<TPrimaryValue> valueSerde,
        ForeignKeyJoinNode<TPrimaryKey, TPrimaryValue, TForeignKey, TForeignValue, TResult> joinNode)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _joinNode = joinNode;
    }

    public override void Init(ProcessorContext context) => Context = context;

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var v = value.Length > 0 ? _valueSerde.Deserialize(value) : default;
        _joinNode.ProcessPrimary(k, v, timestamp);
    }

    public override void Close() { }
}

/// <summary>
/// Input node routing records to the foreign side of a foreign key join.
/// </summary>
internal sealed class ForeignKeyJoinForeignInputNode<TPrimaryKey, TPrimaryValue, TForeignKey, TForeignValue, TResult>
    : ProcessorNode
    where TPrimaryKey : notnull
    where TForeignKey : notnull
{
    private readonly ISerde<TForeignKey> _keySerde;
    private readonly ISerde<TForeignValue> _valueSerde;
    private readonly ForeignKeyJoinNode<TPrimaryKey, TPrimaryValue, TForeignKey, TForeignValue, TResult> _joinNode;

    public ForeignKeyJoinForeignInputNode(
        string name,
        ISerde<TForeignKey> keySerde,
        ISerde<TForeignValue> valueSerde,
        ForeignKeyJoinNode<TPrimaryKey, TPrimaryValue, TForeignKey, TForeignValue, TResult> joinNode)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _joinNode = joinNode;
    }

    public override void Init(ProcessorContext context) => Context = context;

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var v = value.Length > 0 ? _valueSerde.Deserialize(value) : default;
        _joinNode.ProcessForeign(k, v, timestamp);
    }

    public override void Close() { }
}
