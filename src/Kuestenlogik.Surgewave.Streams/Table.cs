using Kuestenlogik.Surgewave.Streams.Processors;
using Kuestenlogik.Surgewave.Streams.Resilience;
using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// An abstraction of a changelog stream (table) of key-value pairs.
/// Tables maintain the latest value for each key and support transformations, joins, and grouping.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface ITable<TKey, TValue>
{
    /// <summary>Filters table entries, keeping only those matching the predicate.</summary>
    /// <param name="predicate">A function that returns true for entries to keep.</param>
    /// <returns>A filtered table.</returns>
    ITable<TKey, TValue> Filter(Func<TKey, TValue, bool> predicate);

    /// <summary>Filters table entries, removing those matching the predicate.</summary>
    /// <param name="predicate">A function that returns true for entries to remove.</param>
    /// <returns>A filtered table.</returns>
    ITable<TKey, TValue> FilterNot(Func<TKey, TValue, bool> predicate);

    /// <summary>Transforms the value of each entry, keeping the key unchanged.</summary>
    /// <typeparam name="TNewValue">The new value type.</typeparam>
    /// <param name="mapper">A function to transform the value.</param>
    /// <returns>A table with transformed values.</returns>
    ITable<TKey, TNewValue> MapValues<TNewValue>(Func<TValue, TNewValue> mapper);

    /// <summary>Transforms the value of each entry using both key and value.</summary>
    /// <typeparam name="TNewValue">The new value type.</typeparam>
    /// <param name="mapper">A function that takes key and value and returns a new value.</param>
    /// <returns>A table with transformed values.</returns>
    ITable<TKey, TNewValue> MapValues<TNewValue>(Func<TKey, TValue, TNewValue> mapper);

    /// <summary>Re-keys each entry using a key selector function.</summary>
    /// <typeparam name="TNewKey">The new key type.</typeparam>
    /// <param name="mapper">A function to select a new key from the current key and value.</param>
    /// <returns>A table with new keys.</returns>
    ITable<TNewKey, TValue> SelectKey<TNewKey>(Func<TKey, TValue, TNewKey> mapper);

    /// <summary>Converts this table to a stream of change events.</summary>
    /// <returns>A stream of key-value records.</returns>
    IStream<TKey, TValue> ToStream();

    /// <summary>Converts this table to a stream with re-keyed records.</summary>
    /// <typeparam name="TNewKey">The new key type.</typeparam>
    /// <param name="keyMapper">A function to select a new key for each record.</param>
    /// <returns>A stream of re-keyed records.</returns>
    IStream<TNewKey, TValue> ToStream<TNewKey>(Func<TKey, TValue, TNewKey> keyMapper);

    /// <summary>Groups the table entries by a new key for aggregation.</summary>
    /// <typeparam name="TNewKey">The new grouping key type.</typeparam>
    /// <param name="keyValueMapper">A function that maps each entry to a new key-value pair.</param>
    /// <returns>A grouped table supporting count, reduce, and aggregate operations.</returns>
    IGroupedTable<TNewKey, TValue> GroupBy<TNewKey>(Func<TKey, TValue, KeyValue<TNewKey, TValue>> keyValueMapper);

    /// <summary>Performs an inner join between this table and another table on matching keys.</summary>
    /// <typeparam name="TOther">The value type of the other table.</typeparam>
    /// <typeparam name="TResult">The join result type.</typeparam>
    /// <param name="other">The other table to join with.</param>
    /// <param name="joiner">A function that combines matching values from both tables.</param>
    /// <returns>A table of joined results.</returns>
    ITable<TKey, TResult> Join<TOther, TResult>(
        ITable<TKey, TOther> other,
        Func<TValue, TOther, TResult> joiner);

    /// <summary>Performs a left join between this table and another table.</summary>
    /// <typeparam name="TOther">The value type of the other table.</typeparam>
    /// <typeparam name="TResult">The join result type.</typeparam>
    /// <param name="other">The other table to join with.</param>
    /// <param name="joiner">A function that combines values; the right value may be null.</param>
    /// <returns>A table of joined results.</returns>
    ITable<TKey, TResult> LeftJoin<TOther, TResult>(
        ITable<TKey, TOther> other,
        Func<TValue, TOther?, TResult> joiner);

    /// <summary>Performs an outer join between this table and another table.</summary>
    /// <typeparam name="TOther">The value type of the other table.</typeparam>
    /// <typeparam name="TResult">The join result type.</typeparam>
    /// <param name="other">The other table to join with.</param>
    /// <param name="joiner">A function that combines values; either value may be null.</param>
    /// <returns>A table of joined results.</returns>
    ITable<TKey, TResult> OuterJoin<TOther, TResult>(
        ITable<TKey, TOther> other,
        Func<TValue?, TOther?, TResult> joiner);

    /// <summary>Performs a foreign key join with another table.</summary>
    /// <typeparam name="TForeignKey">The key type of the foreign table.</typeparam>
    /// <typeparam name="TForeignValue">The value type of the foreign table.</typeparam>
    /// <typeparam name="TResult">The join result type.</typeparam>
    /// <param name="foreignTable">The foreign table to join with.</param>
    /// <param name="foreignKeyExtractor">A function to extract the foreign key from this table's value.</param>
    /// <param name="joiner">A function that combines values from both tables.</param>
    /// <returns>A table of joined results.</returns>
    ITable<TKey, TResult> Join<TForeignKey, TForeignValue, TResult>(
        ITable<TForeignKey, TForeignValue> foreignTable,
        Func<TValue, TForeignKey> foreignKeyExtractor,
        Func<TValue, TForeignValue, TResult> joiner) where TForeignKey : notnull;

    /// <summary>Performs a foreign key left join with another table.</summary>
    /// <typeparam name="TForeignKey">The key type of the foreign table.</typeparam>
    /// <typeparam name="TForeignValue">The value type of the foreign table.</typeparam>
    /// <typeparam name="TResult">The join result type.</typeparam>
    /// <param name="foreignTable">The foreign table to join with.</param>
    /// <param name="foreignKeyExtractor">A function to extract the foreign key from this table's value.</param>
    /// <param name="joiner">A function that combines values; the foreign value may be null.</param>
    /// <returns>A table of joined results.</returns>
    ITable<TKey, TResult> LeftJoin<TForeignKey, TForeignValue, TResult>(
        ITable<TForeignKey, TForeignValue> foreignTable,
        Func<TValue, TForeignKey> foreignKeyExtractor,
        Func<TValue, TForeignValue?, TResult> joiner) where TForeignKey : notnull;

    /// <summary>Gets the name of the underlying state store for interactive queries.</summary>
    string QueryableStoreName { get; }

    /// <summary>Suppresses intermediate updates, emitting only final results based on the suppression strategy.</summary>
    /// <param name="suppressed">The suppression configuration.</param>
    /// <returns>A table with suppressed updates.</returns>
    ITable<TKey, TValue> Suppress(Suppressed<TKey> suppressed);
}

/// <summary>
/// A global table that is fully replicated on every application instance.
/// Global tables are useful for small reference data that needs to be available on all nodes.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface IGlobalTable<TKey, TValue>
{
    /// <summary>Gets the name of the underlying state store for interactive queries.</summary>
    string QueryableStoreName { get; }
}

/// <summary>
/// A grouped table that supports aggregation operations with both add and subtract semantics.
/// Created by calling <see cref="ITable{TKey,TValue}.GroupBy{TNewKey}"/>.
/// </summary>
/// <typeparam name="TKey">The grouping key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface IGroupedTable<TKey, TValue>
{
    /// <summary>Counts the number of entries per key.</summary>
    /// <returns>A table mapping each key to its count.</returns>
    ITable<TKey, long> Count();

    /// <summary>Combines values with the same key, supporting both addition and subtraction on updates.</summary>
    /// <param name="adder">A function to add a new value to the aggregate.</param>
    /// <param name="subtractor">A function to subtract an old value from the aggregate on key changes.</param>
    /// <returns>A table mapping each key to the reduced value.</returns>
    ITable<TKey, TValue> Reduce(Func<TValue, TValue, TValue> adder, Func<TValue, TValue, TValue> subtractor);

    /// <summary>Aggregates values per key with add/subtract semantics for changelog processing.</summary>
    /// <typeparam name="TAggregate">The aggregate result type.</typeparam>
    /// <param name="initializer">A function that provides the initial aggregate value.</param>
    /// <param name="adder">A function to add a new value to the aggregate.</param>
    /// <param name="subtractor">A function to subtract an old value from the aggregate on key changes.</param>
    /// <returns>A table mapping each key to its aggregate.</returns>
    ITable<TKey, TAggregate> Aggregate<TAggregate>(
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> adder,
        Func<TKey, TValue, TAggregate, TAggregate> subtractor);
}

/// <summary>
/// Implementation of Stream.
/// </summary>
internal sealed class StreamImpl<TKey, TValue> : IStream<TKey, TValue>
{
    private readonly StreamsBuilder _builder;
    private readonly string _name;
    private readonly ProcessorNode _sourceNode;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;

    internal StreamImpl(
        StreamsBuilder builder,
        string name,
        ProcessorNode sourceNode,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde)
    {
        _builder = builder;
        _name = name;
        _sourceNode = sourceNode;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
    }

    internal ProcessorNode SourceNode => _sourceNode;

    public IStream<TKey, TValue> Filter(Func<TKey, TValue, bool> predicate)
    {
        var nodeName = _builder.NextNodeName("FILTER");
        var node = new ProcessorNodeImpl<TKey, TValue, TKey, TValue>(
            nodeName, _keySerde, _valueSerde, _keySerde, _valueSerde,
            (k, v) => predicate(k, v) ? [new KeyValue<TKey, TValue>(k, v)] : []);
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TValue>(_builder, nodeName, node, _keySerde, _valueSerde);
    }

    public IStream<TKey, TValue> FilterNot(Func<TKey, TValue, bool> predicate)
        => Filter((k, v) => !predicate(k, v));

    public IStream<TKey, TNewValue> MapValues<TNewValue>(Func<TValue, TNewValue> mapper)
    {
        var newValueSerde = Serdes.Json<TNewValue>();
        var nodeName = _builder.NextNodeName("MAPVALUES");
        var node = new ProcessorNodeImpl<TKey, TValue, TKey, TNewValue>(
            nodeName, _keySerde, _valueSerde, _keySerde, newValueSerde,
            (k, v) => [new KeyValue<TKey, TNewValue>(k, mapper(v))]);
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TNewValue>(_builder, nodeName, node, _keySerde, newValueSerde);
    }

    public IStream<TKey, TNewValue> MapValues<TNewValue>(Func<TKey, TValue, TNewValue> mapper)
    {
        var newValueSerde = Serdes.Json<TNewValue>();
        var nodeName = _builder.NextNodeName("MAPVALUES");
        var node = new ProcessorNodeImpl<TKey, TValue, TKey, TNewValue>(
            nodeName, _keySerde, _valueSerde, _keySerde, newValueSerde,
            (k, v) => [new KeyValue<TKey, TNewValue>(k, mapper(k, v))]);
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TNewValue>(_builder, nodeName, node, _keySerde, newValueSerde);
    }

    public IStream<TNewKey, TNewValue> Map<TNewKey, TNewValue>(Func<TKey, TValue, KeyValue<TNewKey, TNewValue>> mapper)
    {
        var newKeySerde = Serdes.Json<TNewKey>();
        var newValueSerde = Serdes.Json<TNewValue>();
        var nodeName = _builder.NextNodeName("MAP");
        var node = new ProcessorNodeImpl<TKey, TValue, TNewKey, TNewValue>(
            nodeName, _keySerde, _valueSerde, newKeySerde, newValueSerde,
            (k, v) => [mapper(k, v)]);
        _sourceNode.AddChild(node);
        return new StreamImpl<TNewKey, TNewValue>(_builder, nodeName, node, newKeySerde, newValueSerde);
    }

    public IStream<TNewKey, TValue> SelectKey<TNewKey>(Func<TKey, TValue, TNewKey> mapper)
    {
        var newKeySerde = Serdes.Json<TNewKey>();
        var nodeName = _builder.NextNodeName("SELECTKEY");
        var node = new ProcessorNodeImpl<TKey, TValue, TNewKey, TValue>(
            nodeName, _keySerde, _valueSerde, newKeySerde, _valueSerde,
            (k, v) => [new KeyValue<TNewKey, TValue>(mapper(k, v), v)]);
        _sourceNode.AddChild(node);
        return new StreamImpl<TNewKey, TValue>(_builder, nodeName, node, newKeySerde, _valueSerde);
    }

    public IStream<TKey, TNewValue> FlatMapValues<TNewValue>(Func<TValue, IEnumerable<TNewValue>> mapper)
    {
        var newValueSerde = Serdes.Json<TNewValue>();
        var nodeName = _builder.NextNodeName("FLATMAPVALUES");
        var node = new ProcessorNodeImpl<TKey, TValue, TKey, TNewValue>(
            nodeName, _keySerde, _valueSerde, _keySerde, newValueSerde,
            (k, v) => mapper(v).Select(newV => new KeyValue<TKey, TNewValue>(k, newV)));
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TNewValue>(_builder, nodeName, node, _keySerde, newValueSerde);
    }

    public IStream<TNewKey, TNewValue> FlatMap<TNewKey, TNewValue>(Func<TKey, TValue, IEnumerable<KeyValue<TNewKey, TNewValue>>> mapper)
    {
        var newKeySerde = Serdes.Json<TNewKey>();
        var newValueSerde = Serdes.Json<TNewValue>();
        var nodeName = _builder.NextNodeName("FLATMAP");
        var node = new ProcessorNodeImpl<TKey, TValue, TNewKey, TNewValue>(
            nodeName, _keySerde, _valueSerde, newKeySerde, newValueSerde,
            mapper);
        _sourceNode.AddChild(node);
        return new StreamImpl<TNewKey, TNewValue>(_builder, nodeName, node, newKeySerde, newValueSerde);
    }

    public IStream<TKey, TNewValue> TransformValues<TNewValue>(
        IValueTransformerWithKey<TKey, TValue, TNewValue> transformer,
        params string[] stateStoreNames)
    {
        var newValueSerde = Serdes.Json<TNewValue>();
        var nodeName = _builder.NextNodeName("TRANSFORMVALUES");
        var node = new TransformValuesNode<TKey, TValue, TNewValue>(
            nodeName, _keySerde, _valueSerde, newValueSerde,
            transformer, stateStoreNames);
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TNewValue>(_builder, nodeName, node, _keySerde, newValueSerde);
    }

    public IStream<TKey, TValue> Peek(Action<TKey, TValue> action)
    {
        var nodeName = _builder.NextNodeName("PEEK");
        var node = new ProcessorNodeImpl<TKey, TValue, TKey, TValue>(
            nodeName, _keySerde, _valueSerde, _keySerde, _valueSerde,
            (k, v) =>
            {
                action(k, v);
                return [new KeyValue<TKey, TValue>(k, v)];
            });
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TValue>(_builder, nodeName, node, _keySerde, _valueSerde);
    }

    public IStream<TKey, TValue>[] Branch(params Func<TKey, TValue, bool>[] predicates)
    {
        var branches = new IStream<TKey, TValue>[predicates.Length];
        for (int i = 0; i < predicates.Length; i++)
        {
            var predicate = predicates[i];
            var nodeName = _builder.NextNodeName($"BRANCH-{i}");
            var node = new ProcessorNodeImpl<TKey, TValue, TKey, TValue>(
                nodeName, _keySerde, _valueSerde, _keySerde, _valueSerde,
                (k, v) => predicate(k, v) ? [new KeyValue<TKey, TValue>(k, v)] : []);
            _sourceNode.AddChild(node);
            branches[i] = new StreamImpl<TKey, TValue>(_builder, nodeName, node, _keySerde, _valueSerde);
        }
        return branches;
    }

    public IStream<TKey, TValue> Merge(IStream<TKey, TValue> other)
    {
        var otherImpl = (StreamImpl<TKey, TValue>)other;
        var nodeName = _builder.NextNodeName("MERGE");

        // Create a pass-through node that both sources feed into
        var mergeNode = new ProcessorNodeImpl<TKey, TValue, TKey, TValue>(
            nodeName, _keySerde, _valueSerde, _keySerde, _valueSerde,
            (k, v) => [new KeyValue<TKey, TValue>(k, v)]);

        _sourceNode.AddChild(mergeNode);
        otherImpl.SourceNode.AddChild(mergeNode);

        return new StreamImpl<TKey, TValue>(_builder, nodeName, mergeNode, _keySerde, _valueSerde);
    }

    public IStream<TKey, TValue> Repartition()
    {
        var nodeName = _builder.NextNodeName("REPARTITION");

        // Create a repartition node that writes to an internal topic
        var repartitionNode = new RepartitionNode<TKey, TValue>(
            nodeName,
            _builder.ApplicationId,
            _keySerde,
            _valueSerde);

        _sourceNode.AddChild(repartitionNode);
        _builder.AddRepartitionNode(repartitionNode);

        return new StreamImpl<TKey, TValue>(_builder, nodeName, repartitionNode, _keySerde, _valueSerde);
    }

    public IStream<TNewKey, TValue> Repartition<TNewKey>(Func<TKey, TValue, TNewKey> partitioner)
    {
        var newKeySerde = Serdes.Json<TNewKey>();
        var nodeName = _builder.NextNodeName("REPARTITION");

        // Create a repartition node with key transformation
        var repartitionNode = new RepartitionNode<TKey, TValue, TNewKey>(
            nodeName,
            _builder.ApplicationId,
            _keySerde,
            _valueSerde,
            newKeySerde,
            partitioner);

        _sourceNode.AddChild(repartitionNode);
        _builder.AddRepartitionNode(repartitionNode);

        return new StreamImpl<TNewKey, TValue>(_builder, nodeName, repartitionNode, newKeySerde, _valueSerde);
    }

    public IGroupedStream<TKey, TValue> GroupByKey()
        => new GroupedStreamImpl<TKey, TValue>(_builder, this, _keySerde, _valueSerde);

    public IGroupedStream<TNewKey, TValue> GroupBy<TNewKey>(Func<TKey, TValue, TNewKey> keySelector)
    {
        var rekeyed = SelectKey(keySelector);
        var newKeySerde = Serdes.Json<TNewKey>();
        return new GroupedStreamImpl<TNewKey, TValue>(_builder, (StreamImpl<TNewKey, TValue>)rekeyed, newKeySerde, _valueSerde);
    }

    public ITable<TKey, TValue> ToTable()
    {
        var storeName = _builder.NextStoreName("TABLE");
        return new TableImpl<TKey, TValue>(_builder, storeName, this, _keySerde, _valueSerde);
    }

    public IStream<TKey, TResult> Join<TOther, TResult>(
        IStream<TKey, TOther> other,
        Func<TValue, TOther, TResult> joiner,
        JoinWindows windows)
    {
        return CreateStreamStreamJoin(other, joiner, windows, JoinType.Inner);
    }

    public IStream<TKey, TResult> LeftJoin<TOther, TResult>(
        IStream<TKey, TOther> other,
        Func<TValue, TOther?, TResult> joiner,
        JoinWindows windows)
    {
        return CreateStreamStreamJoin(other, joiner!, windows, JoinType.Left);
    }

    public IStream<TKey, TResult> OuterJoin<TOther, TResult>(
        IStream<TKey, TOther> other,
        Func<TValue?, TOther?, TResult> joiner,
        JoinWindows windows)
    {
        return CreateStreamStreamJoin(other, joiner!, windows, JoinType.Outer);
    }

    private StreamImpl<TKey, TResult> CreateStreamStreamJoin<TOther, TResult>(
        IStream<TKey, TOther> other,
        Func<TValue, TOther, TResult> joiner,
        JoinWindows windows,
        JoinType joinType)
    {
        var otherImpl = (StreamImpl<TKey, TOther>)other;
        var otherValueSerde = Serdes.Json<TOther>();
        var resultSerde = Serdes.Json<TResult>();
        var nodeName = _builder.NextNodeName($"{joinType.ToString().ToUpperInvariant()}JOIN");

        // Create window stores for both sides
        var leftStoreName = _builder.NextStoreName($"{nodeName}-LEFT-STORE");
        var rightStoreName = _builder.NextStoreName($"{nodeName}-RIGHT-STORE");

        var windowSize = TimeSpan.FromMilliseconds(windows.Size);
        var retentionPeriod = windowSize + windows.GracePeriod;

        _builder.AddStateStore(Stores.WindowStore<TKey, TValue>(leftStoreName, windowSize, retentionPeriod));
        _builder.AddStateStore(Stores.WindowStore<TKey, TOther>(rightStoreName, windowSize, retentionPeriod));

        // Create the join processor node
        var joinNode = new StreamStreamJoinNode<TKey, TValue, TOther, TResult>(
            nodeName,
            _keySerde,
            _valueSerde,
            otherValueSerde,
            resultSerde,
            windows,
            joiner,
            joinType,
            leftStoreName,
            rightStoreName);

        // Create input nodes that route to the join node
        var leftInputName = _builder.NextNodeName($"{nodeName}-LEFT-INPUT");
        var rightInputName = _builder.NextNodeName($"{nodeName}-RIGHT-INPUT");

        var leftInputNode = new JoinLeftInputNode<TKey, TValue, TOther, TResult>(
            leftInputName, _keySerde, _valueSerde, joinNode);
        var rightInputNode = new JoinRightInputNode<TKey, TValue, TOther, TResult>(
            rightInputName, _keySerde, otherValueSerde, joinNode);

        // Wire up the topology: both source streams feed into their input nodes,
        // which route to the join node
        _sourceNode.AddChild(leftInputNode);
        otherImpl.SourceNode.AddChild(rightInputNode);

        // The join node is the parent of downstream processors
        leftInputNode.AddChild(joinNode);

        return new StreamImpl<TKey, TResult>(_builder, nodeName, joinNode, _keySerde, resultSerde);
    }

    public IStream<TKey, TResult> Join<TTableValue, TResult>(
        ITable<TKey, TTableValue> table,
        Func<TValue, TTableValue, TResult> joiner)
    {
        var tableImpl = (TableImpl<TKey, TTableValue>)table;
        var resultSerde = Serdes.Json<TResult>();
        var nodeName = _builder.NextNodeName("STREAMTABLEJOIN");
        var node = new StreamTableJoinNode<TKey, TValue, TTableValue, TResult>(
            nodeName, _keySerde, _valueSerde, resultSerde, tableImpl.StoreName, joiner, leftJoin: false);
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TResult>(_builder, nodeName, node, _keySerde, resultSerde);
    }

    public IStream<TKey, TResult> LeftJoin<TTableValue, TResult>(
        ITable<TKey, TTableValue> table,
        Func<TValue, TTableValue?, TResult> joiner)
    {
        var tableImpl = (TableImpl<TKey, TTableValue>)table;
        var resultSerde = Serdes.Json<TResult>();
        var nodeName = _builder.NextNodeName("STREAMTABLELEFTJOIN");
        var node = new StreamTableJoinNode<TKey, TValue, TTableValue, TResult>(
            nodeName, _keySerde, _valueSerde, resultSerde, tableImpl.StoreName, joiner!, leftJoin: true);
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TResult>(_builder, nodeName, node, _keySerde, resultSerde);
    }

    public IStream<TKey, TResult> Join<TGlobalKey, TGlobalValue, TResult>(
        IGlobalTable<TGlobalKey, TGlobalValue> globalTable,
        Func<TKey, TValue, TGlobalKey> keySelector,
        Func<TValue, TGlobalValue, TResult> joiner)
    {
        var globalTableImpl = (GlobalTableImpl<TGlobalKey, TGlobalValue>)globalTable;
        var resultSerde = Serdes.Json<TResult>();
        var nodeName = _builder.NextNodeName("GLOBALJOIN");
        var node = new GlobalTableJoinNode<TKey, TValue, TGlobalKey, TGlobalValue, TResult>(
            nodeName, _keySerde, _valueSerde, resultSerde,
            globalTableImpl.QueryableStoreName, keySelector, joiner);
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TResult>(_builder, nodeName, node, _keySerde, resultSerde);
    }

    public IStream<TKey, TValue> WithRetry(int maxRetries = 3)
    {
        var nodeName = _builder.NextNodeName("RETRY");
        var retryConfig = new StreamsRetryConfig
        {
            Enabled = true,
            MaxRetries = maxRetries,
            BackoffStrategy = BackoffStrategy.ExponentialWithJitter
        };
        var node = new RetryNode<TKey, TValue>(nodeName, _keySerde, _valueSerde, retryConfig);
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TValue>(_builder, nodeName, node, _keySerde, _valueSerde);
    }

    public IStream<TKey, TValue> RateLimit(int recordsPerSecond)
    {
        var nodeName = _builder.NextNodeName("RATELIMIT");
        var node = new RateLimitNode<TKey, TValue>(
            nodeName, _keySerde, _valueSerde, recordsPerSecond);
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TValue>(_builder, nodeName, node, _keySerde, _valueSerde);
    }

    public IStream<TKey, TValue> Parallel(int degreeOfParallelism)
    {
        var nodeName = _builder.NextNodeName("PARALLEL");
        var node = new ParallelProcessorNode<TKey, TValue, TKey, TValue>(
            nodeName, _keySerde, _valueSerde, _keySerde, _valueSerde,
            (k, v) => [new KeyValue<TKey, TValue>(k, v)],
            degreeOfParallelism);
        _sourceNode.AddChild(node);
        return new StreamImpl<TKey, TValue>(_builder, nodeName, node, _keySerde, _valueSerde);
    }

    public void To(string topic)
    {
        var sinkName = _builder.NextNodeName("SINK");
        var sink = new SinkNode<TKey, TValue>(sinkName, topic, _keySerde, _valueSerde);
        _sourceNode.AddChild(sink);
        _builder.AddSink(sink);
    }

    public void To(Func<TKey, TValue, string> topicExtractor)
    {
        var sinkName = _builder.NextNodeName("DYNAMIC-SINK");
        var sink = new DynamicSinkNode<TKey, TValue>(sinkName, _keySerde, _valueSerde, topicExtractor);
        _sourceNode.AddChild(sink);
    }

    public void ForEach(Action<TKey, TValue> action)
    {
        var nodeName = _builder.NextNodeName("FOREACH");
        var node = new ProcessorNodeImpl<TKey, TValue, TKey, TValue>(
            nodeName, _keySerde, _valueSerde, _keySerde, _valueSerde,
            (k, v) =>
            {
                action(k, v);
                return [];
            });
        _sourceNode.AddChild(node);
    }

    public void Print()
    {
        ForEach((k, v) => Console.WriteLine($"[{k}]: {v}"));
    }
}

/// <summary>
/// Stream-Table join processor node.
/// </summary>
internal sealed class StreamTableJoinNode<TKey, TStreamValue, TTableValue, TResult> : ProcessorNode
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TStreamValue> _streamValueSerde;
    private readonly ISerde<TResult> _resultSerde;
    private readonly string _tableStoreName;
    private readonly Func<TStreamValue, TTableValue, TResult> _joiner;
    private readonly bool _leftJoin;
    private IKeyValueStore<TKey, TTableValue>? _tableStore;

    public StreamTableJoinNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TStreamValue> streamValueSerde,
        ISerde<TResult> resultSerde,
        string tableStoreName,
        Func<TStreamValue, TTableValue, TResult> joiner,
        bool leftJoin)
        : base(name)
    {
        _keySerde = keySerde;
        _streamValueSerde = streamValueSerde;
        _resultSerde = resultSerde;
        _tableStoreName = tableStoreName;
        _joiner = joiner;
        _leftJoin = leftJoin;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _tableStore = context.GetStateStore<IKeyValueStore<TKey, TTableValue>>(_tableStoreName);
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var streamValue = _streamValueSerde.Deserialize(value);

        if (_tableStore != null)
        {
            var tableValue = _tableStore.Get(k);
            if (tableValue != null || _leftJoin)
            {
                var result = _joiner(streamValue, tableValue!);
                var resultBytes = _resultSerde.Serialize(result);
                ForwardToChildren(key, resultBytes, timestamp);
            }
        }
        else if (_leftJoin)
        {
            var result = _joiner(streamValue, default!);
            var resultBytes = _resultSerde.Serialize(result);
            ForwardToChildren(key, resultBytes, timestamp);
        }
    }

    public override void Close() { }
}

/// <summary>
/// Implementation of Table.
/// </summary>
internal sealed class TableImpl<TKey, TValue> : ITable<TKey, TValue>
    where TKey : notnull
{
    private readonly StreamsBuilder _builder;
    private readonly StreamImpl<TKey, TValue> _sourceStream;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;

    internal string StoreName { get; }

    internal TableImpl(
        StreamsBuilder builder,
        string storeName,
        StreamImpl<TKey, TValue> sourceStream,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde)
    {
        _builder = builder;
        StoreName = storeName;
        _sourceStream = sourceStream;
        _keySerde = keySerde;
        _valueSerde = valueSerde;

        // Register state store for the table
        builder.AddStateStore(Stores.KeyValueStore<TKey, TValue>(storeName));
    }

    public string QueryableStoreName => StoreName;

    public ITable<TKey, TValue> Filter(Func<TKey, TValue, bool> predicate)
    {
        var filtered = _sourceStream.Filter(predicate);
        var newStoreName = _builder.NextStoreName("FILTER-TABLE");
        return new TableImpl<TKey, TValue>(_builder, newStoreName, (StreamImpl<TKey, TValue>)filtered, _keySerde, _valueSerde);
    }

    public ITable<TKey, TValue> FilterNot(Func<TKey, TValue, bool> predicate)
        => Filter((k, v) => !predicate(k, v));

    public ITable<TKey, TNewValue> MapValues<TNewValue>(Func<TValue, TNewValue> mapper)
    {
        var mapped = _sourceStream.MapValues(mapper);
        var newStoreName = _builder.NextStoreName("MAPVALUES-TABLE");
        var newValueSerde = Serdes.Json<TNewValue>();
        return new TableImpl<TKey, TNewValue>(_builder, newStoreName, (StreamImpl<TKey, TNewValue>)mapped, _keySerde, newValueSerde);
    }

    public ITable<TKey, TNewValue> MapValues<TNewValue>(Func<TKey, TValue, TNewValue> mapper)
    {
        var mapped = _sourceStream.MapValues(mapper);
        var newStoreName = _builder.NextStoreName("MAPVALUES-TABLE");
        var newValueSerde = Serdes.Json<TNewValue>();
        return new TableImpl<TKey, TNewValue>(_builder, newStoreName, (StreamImpl<TKey, TNewValue>)mapped, _keySerde, newValueSerde);
    }

    public ITable<TNewKey, TValue> SelectKey<TNewKey>(Func<TKey, TValue, TNewKey> mapper)
        where TNewKey : notnull
    {
        var rekeyed = _sourceStream.SelectKey(mapper);
        var newStoreName = _builder.NextStoreName("SELECTKEY-TABLE");
        var newKeySerde = Serdes.Json<TNewKey>();
        return new TableImpl<TNewKey, TValue>(_builder, newStoreName, (StreamImpl<TNewKey, TValue>)rekeyed, newKeySerde, _valueSerde);
    }

    public IStream<TKey, TValue> ToStream() => _sourceStream;

    public IStream<TNewKey, TValue> ToStream<TNewKey>(Func<TKey, TValue, TNewKey> keyMapper)
        => _sourceStream.SelectKey(keyMapper);

    public IGroupedTable<TNewKey, TValue> GroupBy<TNewKey>(Func<TKey, TValue, KeyValue<TNewKey, TValue>> keyValueMapper)
        where TNewKey : notnull
    {
        var newKeySerde = Serdes.Json<TNewKey>();
        return new GroupedTableImpl<TNewKey, TValue>(_builder, this, newKeySerde, _valueSerde, keyValueMapper);
    }

    public ITable<TKey, TResult> Join<TOther, TResult>(ITable<TKey, TOther> other, Func<TValue, TOther, TResult> joiner)
    {
        return CreateTableTableJoin(other, (l, r) => joiner(l!, r!), JoinType.Inner);
    }

    public ITable<TKey, TResult> LeftJoin<TOther, TResult>(ITable<TKey, TOther> other, Func<TValue, TOther?, TResult> joiner)
    {
        return CreateTableTableJoin(other, (l, r) => joiner(l!, r), JoinType.Left);
    }

    public ITable<TKey, TResult> OuterJoin<TOther, TResult>(ITable<TKey, TOther> other, Func<TValue?, TOther?, TResult> joiner)
    {
        return CreateTableTableJoin(other, joiner, JoinType.Outer);
    }

    private TableImpl<TKey, TResult> CreateTableTableJoin<TOther, TResult>(
        ITable<TKey, TOther> other,
        Func<TValue?, TOther?, TResult> joiner,
        JoinType joinType)
    {
        var otherImpl = (TableImpl<TKey, TOther>)other;
        var otherValueSerde = Serdes.Json<TOther>();
        var resultSerde = Serdes.Json<TResult>();
        var nodeName = _builder.NextNodeName($"TABLE{joinType.ToString().ToUpperInvariant()}JOIN");

        // Create stores for both sides of the join
        var leftStoreName = _builder.NextStoreName($"{nodeName}-LEFT-STORE");
        var rightStoreName = _builder.NextStoreName($"{nodeName}-RIGHT-STORE");

        _builder.AddStateStore(Stores.KeyValueStore<TKey, TValue>(leftStoreName));
        _builder.AddStateStore(Stores.KeyValueStore<TKey, TOther>(rightStoreName));

        // Create the join processor node
        var joinNode = new TableTableJoinNode<TKey, TValue, TOther, TResult>(
            nodeName,
            _keySerde,
            _valueSerde,
            otherValueSerde,
            resultSerde,
            joiner,
            joinType,
            leftStoreName,
            rightStoreName);

        // Create input nodes that route to the join node
        var leftInputName = _builder.NextNodeName($"{nodeName}-LEFT-INPUT");
        var rightInputName = _builder.NextNodeName($"{nodeName}-RIGHT-INPUT");

        var leftInputNode = new TableJoinLeftInputNode<TKey, TValue, TOther, TResult>(
            leftInputName, _keySerde, _valueSerde, joinNode);
        var rightInputNode = new TableJoinRightInputNode<TKey, TValue, TOther, TResult>(
            rightInputName, _keySerde, otherValueSerde, joinNode);

        // Wire up the topology
        _sourceStream.SourceNode.AddChild(leftInputNode);
        otherImpl._sourceStream.SourceNode.AddChild(rightInputNode);
        leftInputNode.AddChild(joinNode);

        // Create the result table with a new store
        var resultStoreName = _builder.NextStoreName($"{nodeName}-RESULT");
        var resultStream = new StreamImpl<TKey, TResult>(_builder, nodeName, joinNode, _keySerde, resultSerde);
        return new TableImpl<TKey, TResult>(_builder, resultStoreName, resultStream, _keySerde, resultSerde);
    }

    public ITable<TKey, TResult> Join<TForeignKey, TForeignValue, TResult>(
        ITable<TForeignKey, TForeignValue> foreignTable,
        Func<TValue, TForeignKey> foreignKeyExtractor,
        Func<TValue, TForeignValue, TResult> joiner)
        where TForeignKey : notnull
    {
        return CreateForeignKeyJoin(foreignTable, foreignKeyExtractor,
            (pv, fv) => joiner(pv, fv!), leftJoin: false);
    }

    public ITable<TKey, TResult> LeftJoin<TForeignKey, TForeignValue, TResult>(
        ITable<TForeignKey, TForeignValue> foreignTable,
        Func<TValue, TForeignKey> foreignKeyExtractor,
        Func<TValue, TForeignValue?, TResult> joiner)
        where TForeignKey : notnull
    {
        return CreateForeignKeyJoin(foreignTable, foreignKeyExtractor, joiner, leftJoin: true);
    }

    private TableImpl<TKey, TResult> CreateForeignKeyJoin<TForeignKey, TForeignValue, TResult>(
        ITable<TForeignKey, TForeignValue> foreignTable,
        Func<TValue, TForeignKey> foreignKeyExtractor,
        Func<TValue, TForeignValue?, TResult> joiner,
        bool leftJoin)
        where TForeignKey : notnull
    {
        var foreignImpl = (TableImpl<TForeignKey, TForeignValue>)foreignTable;
        var foreignKeySerde = Serdes.Json<TForeignKey>();
        var foreignValueSerde = Serdes.Json<TForeignValue>();
        var resultSerde = Serdes.Json<TResult>();
        var joinTypeName = leftJoin ? "FKLEFTJOIN" : "FKJOIN";
        var nodeName = _builder.NextNodeName(joinTypeName);

        // Create stores for primary and foreign sides
        var primaryStoreName = _builder.NextStoreName($"{nodeName}-PRIMARY");
        var foreignStoreName = _builder.NextStoreName($"{nodeName}-FOREIGN");

        _builder.AddStateStore(Stores.KeyValueStore<TKey, TValue>(primaryStoreName));
        _builder.AddStateStore(Stores.KeyValueStore<TForeignKey, TForeignValue>(foreignStoreName));

        // Create the FK join processor node
        var joinNode = new ForeignKeyJoinNode<TKey, TValue, TForeignKey, TForeignValue, TResult>(
            nodeName,
            _keySerde,
            _valueSerde,
            foreignKeySerde,
            foreignValueSerde,
            resultSerde,
            foreignKeyExtractor,
            joiner,
            leftJoin,
            primaryStoreName,
            foreignStoreName);

        // Create input nodes
        var primaryInputName = _builder.NextNodeName($"{nodeName}-PRI-INPUT");
        var foreignInputName = _builder.NextNodeName($"{nodeName}-FOR-INPUT");

        var primaryInputNode = new ForeignKeyJoinPrimaryInputNode<TKey, TValue, TForeignKey, TForeignValue, TResult>(
            primaryInputName, _keySerde, _valueSerde, joinNode);
        var foreignInputNode = new ForeignKeyJoinForeignInputNode<TKey, TValue, TForeignKey, TForeignValue, TResult>(
            foreignInputName, foreignKeySerde, foreignValueSerde, joinNode);

        // Wire up topology
        _sourceStream.SourceNode.AddChild(primaryInputNode);
        foreignImpl._sourceStream.SourceNode.AddChild(foreignInputNode);
        primaryInputNode.AddChild(joinNode);

        // Create result table
        var resultStoreName = _builder.NextStoreName($"{nodeName}-RESULT");
        var resultStream = new StreamImpl<TKey, TResult>(_builder, nodeName, joinNode, _keySerde, resultSerde);
        return new TableImpl<TKey, TResult>(_builder, resultStoreName, resultStream, _keySerde, resultSerde);
    }

    public ITable<TKey, TValue> Suppress(Suppressed<TKey> suppressed)
    {
        var nodeName = _builder.NextNodeName("SUPPRESS");
        var suppressStoreName = _builder.NextStoreName($"{nodeName}-BUFFER");

        var suppressNode = new SuppressNode<TKey, TValue>(
            nodeName,
            _keySerde,
            _valueSerde,
            suppressed,
            suppressStoreName);

        _sourceStream.SourceNode.AddChild(suppressNode);

        var resultStoreName = _builder.NextStoreName($"{nodeName}-TABLE");
        var resultStream = new StreamImpl<TKey, TValue>(_builder, nodeName, suppressNode, _keySerde, _valueSerde);
        return new TableImpl<TKey, TValue>(_builder, resultStoreName, resultStream, _keySerde, _valueSerde);
    }
}

/// <summary>
/// Implementation of GlobalTable.
/// </summary>
internal sealed class GlobalTableImpl<TKey, TValue> : IGlobalTable<TKey, TValue>
{
    public string QueryableStoreName { get; }
    internal string? SourceTopic { get; init; }

    internal GlobalTableImpl(string storeName)
    {
        QueryableStoreName = storeName;
    }
}
