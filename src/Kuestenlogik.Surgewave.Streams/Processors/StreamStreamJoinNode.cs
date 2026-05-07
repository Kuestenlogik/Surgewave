using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Type of stream-stream join.
/// </summary>
public enum JoinType
{
    Inner,
    Left,
    Outer
}

/// <summary>
/// Processor node for stream-stream joins.
/// Uses window stores on both sides to join records within a time window.
/// </summary>
internal sealed class StreamStreamJoinNode<TKey, TLeftValue, TRightValue, TResult> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TLeftValue> _leftValueSerde;
    private readonly ISerde<TRightValue> _rightValueSerde;
    private readonly ISerde<TResult> _resultSerde;
    private readonly JoinWindows _joinWindows;
    private readonly Func<TLeftValue, TRightValue, TResult> _joiner;
    private readonly JoinType _joinType;
    private readonly string _leftStoreName;
    private readonly string _rightStoreName;

    private IWindowStore<TKey, TLeftValue>? _leftStore;
    private IWindowStore<TKey, TRightValue>? _rightStore;

    public StreamStreamJoinNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TLeftValue> leftValueSerde,
        ISerde<TRightValue> rightValueSerde,
        ISerde<TResult> resultSerde,
        JoinWindows joinWindows,
        Func<TLeftValue, TRightValue, TResult> joiner,
        JoinType joinType,
        string leftStoreName,
        string rightStoreName)
        : base(name)
    {
        _keySerde = keySerde;
        _leftValueSerde = leftValueSerde;
        _rightValueSerde = rightValueSerde;
        _resultSerde = resultSerde;
        _joinWindows = joinWindows;
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
        _leftStore = context.GetStateStore<IWindowStore<TKey, TLeftValue>>(_leftStoreName);
        _rightStore = context.GetStateStore<IWindowStore<TKey, TRightValue>>(_rightStoreName);
    }

    /// <summary>
    /// Process a record from the left stream.
    /// </summary>
    public void ProcessLeft(TKey key, TLeftValue value, long timestamp)
    {
        if (_leftStore == null || _rightStore == null)
            return;

        // Store the left value
        _leftStore.Put(key, value, timestamp);

        // Probe the right store for matching records within the window
        var timeFrom = timestamp - _joinWindows.BeforeMs;
        var timeTo = timestamp + _joinWindows.AfterMs;

        var rightMatches = _rightStore.Fetch(key, timeFrom, timeTo).ToList();

        if (rightMatches.Count > 0)
        {
            // Emit join results for each match
            foreach (var rightMatch in rightMatches)
            {
                EmitJoinResult(key, value, rightMatch.Value, timestamp);
            }
        }
        else if (_joinType is JoinType.Left or JoinType.Outer)
        {
            // For left/outer join, emit with null right side
            EmitJoinResult(key, value, default!, timestamp);
        }
    }

    /// <summary>
    /// Process a record from the right stream.
    /// </summary>
    public void ProcessRight(TKey key, TRightValue value, long timestamp)
    {
        if (_leftStore == null || _rightStore == null)
            return;

        // Store the right value
        _rightStore.Put(key, value, timestamp);

        // Probe the left store for matching records within the window
        var timeFrom = timestamp - _joinWindows.AfterMs;
        var timeTo = timestamp + _joinWindows.BeforeMs;

        var leftMatches = _leftStore.Fetch(key, timeFrom, timeTo).ToList();

        if (leftMatches.Count > 0)
        {
            // Emit join results for each match
            foreach (var leftMatch in leftMatches)
            {
                EmitJoinResult(key, leftMatch.Value, value, timestamp);
            }
        }
        else if (_joinType == JoinType.Outer)
        {
            // For outer join, emit with null left side
            EmitJoinResult(key, default!, value, timestamp);
        }
        // Note: For left join, we don't emit when only right side arrives
    }

    /// <summary>
    /// Process a record - determines which side based on isLeft flag in the value wrapper.
    /// </summary>
    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        // This is called when processing through the topology
        // The actual left/right routing is handled by JoinInputNode
        var k = _keySerde.Deserialize(key);

        // Try to deserialize as left value first
        // In practice, we use separate input nodes that call ProcessLeft/ProcessRight directly
        try
        {
            var leftValue = _leftValueSerde.Deserialize(value);
            ProcessLeft(k, leftValue, timestamp);
        }
        catch
        {
            // Ignore - this record is from the right side
        }
    }

    private void EmitJoinResult(TKey key, TLeftValue? leftValue, TRightValue? rightValue, long timestamp)
    {
        try
        {
            var result = _joiner(leftValue!, rightValue!);
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

    public override void Close()
    {
        _leftStore?.Close();
        _rightStore?.Close();
    }
}

/// <summary>
/// Input node that routes records to the left side of a stream-stream join.
/// </summary>
internal sealed class JoinLeftInputNode<TKey, TLeftValue, TRightValue, TResult> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TLeftValue> _valueSerde;
    private readonly StreamStreamJoinNode<TKey, TLeftValue, TRightValue, TResult> _joinNode;

    public JoinLeftInputNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TLeftValue> valueSerde,
        StreamStreamJoinNode<TKey, TLeftValue, TRightValue, TResult> joinNode)
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
        var v = _valueSerde.Deserialize(value);
        _joinNode.ProcessLeft(k, v, timestamp);
    }

    public override void Close() { }
}

/// <summary>
/// Input node that routes records to the right side of a stream-stream join.
/// </summary>
internal sealed class JoinRightInputNode<TKey, TLeftValue, TRightValue, TResult> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TRightValue> _valueSerde;
    private readonly StreamStreamJoinNode<TKey, TLeftValue, TRightValue, TResult> _joinNode;

    public JoinRightInputNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TRightValue> valueSerde,
        StreamStreamJoinNode<TKey, TLeftValue, TRightValue, TResult> joinNode)
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
        var v = _valueSerde.Deserialize(value);
        _joinNode.ProcessRight(k, v, timestamp);
    }

    public override void Close() { }
}
