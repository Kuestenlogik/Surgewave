namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Processor node for table-table joins.
/// Uses key-value stores on both sides to join records by key.
/// Handles update semantics: when a key is updated, the old join result
/// is replaced with a new one.
/// </summary>
internal sealed class TableTableJoinNode<TKey, TLeftValue, TRightValue, TResult> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TLeftValue> _leftValueSerde;
    private readonly ISerde<TRightValue> _rightValueSerde;
    private readonly ISerde<TResult> _resultSerde;
    private readonly Func<TLeftValue?, TRightValue?, TResult> _joiner;
    private readonly JoinType _joinType;
    private readonly string _leftStoreName;
    private readonly string _rightStoreName;

    private IKeyValueStore<TKey, TLeftValue>? _leftStore;
    private IKeyValueStore<TKey, TRightValue>? _rightStore;

    public TableTableJoinNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TLeftValue> leftValueSerde,
        ISerde<TRightValue> rightValueSerde,
        ISerde<TResult> resultSerde,
        Func<TLeftValue?, TRightValue?, TResult> joiner,
        JoinType joinType,
        string leftStoreName,
        string rightStoreName)
        : base(name)
    {
        _keySerde = keySerde;
        _leftValueSerde = leftValueSerde;
        _rightValueSerde = rightValueSerde;
        _resultSerde = resultSerde;
        _joiner = joiner;
        _joinType = joinType;
        _leftStoreName = leftStoreName;
        _rightStoreName = rightStoreName;
    }

    public string LeftStoreName => _leftStoreName;
    public string RightStoreName => _rightStoreName;

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _leftStore = context.GetStateStore<IKeyValueStore<TKey, TLeftValue>>(_leftStoreName);
        _rightStore = context.GetStateStore<IKeyValueStore<TKey, TRightValue>>(_rightStoreName);
    }

    /// <summary>
    /// Process an update from the left table.
    /// </summary>
    public void ProcessLeft(TKey key, TLeftValue? value, long timestamp)
    {
        if (_leftStore == null || _rightStore == null)
            return;

        // Get old left value for comparison
        var oldLeftValue = _leftStore.Get(key);
        var hasOldLeft = oldLeftValue != null;

        if (value == null)
        {
            // Tombstone - delete from left store
            _leftStore.Delete(key);
        }
        else
        {
            // Store the new left value
            _leftStore.Put(key, value);
        }

        // Look up corresponding right value
        var rightValue = _rightStore.Get(key);
        var hasRight = rightValue != null;

        // Determine if we should emit a result
        var shouldEmit = _joinType switch
        {
            JoinType.Inner => value != null && hasRight,
            JoinType.Left => value != null,
            JoinType.Outer => true,
            _ => false
        };

        if (shouldEmit)
        {
            if (value == null && _joinType == JoinType.Outer && !hasRight)
            {
                // Both sides are null - emit tombstone
                EmitTombstone(key, timestamp);
            }
            else if (value == null && !hasRight)
            {
                // Left deleted, no right - emit tombstone for left/outer join
                EmitTombstone(key, timestamp);
            }
            else
            {
                EmitJoinResult(key, value, rightValue, timestamp);
            }
        }
        else if (hasOldLeft && _joinType == JoinType.Inner && (value == null || !hasRight))
        {
            // Was previously joined but now isn't - emit tombstone
            EmitTombstone(key, timestamp);
        }
    }

    /// <summary>
    /// Process an update from the right table.
    /// </summary>
    public void ProcessRight(TKey key, TRightValue? value, long timestamp)
    {
        if (_leftStore == null || _rightStore == null)
            return;

        // Get old right value for comparison
        var oldRightValue = _rightStore.Get(key);
        var hasOldRight = oldRightValue != null;

        if (value == null)
        {
            // Tombstone - delete from right store
            _rightStore.Delete(key);
        }
        else
        {
            // Store the new right value
            _rightStore.Put(key, value);
        }

        // Look up corresponding left value
        var leftValue = _leftStore.Get(key);
        var hasLeft = leftValue != null;

        // Determine if we should emit a result
        var shouldEmit = _joinType switch
        {
            JoinType.Inner => value != null && hasLeft,
            JoinType.Left => hasLeft, // Left join only emits if left side exists
            JoinType.Outer => true,
            _ => false
        };

        if (shouldEmit)
        {
            if (value == null && !hasLeft && _joinType == JoinType.Outer)
            {
                // Both sides are null - emit tombstone
                EmitTombstone(key, timestamp);
            }
            else if (_joinType == JoinType.Left && !hasLeft)
            {
                // Left join: don't emit if no left side
            }
            else
            {
                EmitJoinResult(key, leftValue, value, timestamp);
            }
        }
        else if (hasOldRight && _joinType == JoinType.Inner && (value == null || !hasLeft))
        {
            // Was previously joined but now isn't - emit tombstone
            EmitTombstone(key, timestamp);
        }
    }

    /// <summary>
    /// Process a record - determines which side based on deserialization.
    /// In practice, we use separate input nodes that call ProcessLeft/ProcessRight directly.
    /// </summary>
    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);

        // Try to deserialize as left value first
        try
        {
            var leftValue = _leftValueSerde.Deserialize(value);
            ProcessLeft(k, leftValue, timestamp);
        }
        catch
        {
            // Ignore - this record might be from the right side
        }
    }

    private void EmitJoinResult(TKey key, TLeftValue? leftValue, TRightValue? rightValue, long timestamp)
    {
        try
        {
            var result = _joiner(leftValue, rightValue);
            var keyBytes = _keySerde.Serialize(key);
            var resultBytes = _resultSerde.Serialize(result);
            ForwardToChildren(keyBytes, resultBytes, timestamp);
        }
        catch
        {
            // Joiner may throw if values are null and it doesn't handle that
            // In that case, skip emitting this result
        }
    }

    private void EmitTombstone(TKey key, long timestamp)
    {
        var keyBytes = _keySerde.Serialize(key);
        ForwardToChildren(keyBytes, [], timestamp);
    }

    public override void Close()
    {
        _leftStore?.Close();
        _rightStore?.Close();
    }
}

/// <summary>
/// Input node that routes records to the left side of a table-table join.
/// </summary>
internal sealed class TableJoinLeftInputNode<TKey, TLeftValue, TRightValue, TResult> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TLeftValue> _valueSerde;
    private readonly TableTableJoinNode<TKey, TLeftValue, TRightValue, TResult> _joinNode;

    public TableJoinLeftInputNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TLeftValue> valueSerde,
        TableTableJoinNode<TKey, TLeftValue, TRightValue, TResult> joinNode)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _joinNode = joinNode;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var v = value.Length > 0 ? _valueSerde.Deserialize(value) : default;
        _joinNode.ProcessLeft(k, v, timestamp);
    }

    public override void Close() { }
}

/// <summary>
/// Input node that routes records to the right side of a table-table join.
/// </summary>
internal sealed class TableJoinRightInputNode<TKey, TLeftValue, TRightValue, TResult> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TRightValue> _valueSerde;
    private readonly TableTableJoinNode<TKey, TLeftValue, TRightValue, TResult> _joinNode;

    public TableJoinRightInputNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TRightValue> valueSerde,
        TableTableJoinNode<TKey, TLeftValue, TRightValue, TResult> joinNode)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _joinNode = joinNode;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var v = value.Length > 0 ? _valueSerde.Deserialize(value) : default;
        _joinNode.ProcessRight(k, v, timestamp);
    }

    public override void Close() { }
}
