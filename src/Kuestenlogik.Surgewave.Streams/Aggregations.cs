using Kuestenlogik.Surgewave.Streams.EventTime;
using Kuestenlogik.Surgewave.Streams.Processors;
using Kuestenlogik.Surgewave.Streams.Windows;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Implementation of grouped stream for aggregations.
/// </summary>
internal sealed class GroupedStreamImpl<TKey, TValue> : IGroupedStream<TKey, TValue>
    where TKey : notnull
{
    private readonly StreamsBuilder _builder;
    private readonly StreamImpl<TKey, TValue> _sourceStream;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;

    internal GroupedStreamImpl(
        StreamsBuilder builder,
        StreamImpl<TKey, TValue> sourceStream,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde)
    {
        _builder = builder;
        _sourceStream = sourceStream;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
    }

    public ITable<TKey, long> Count()
    {
        return Aggregate(
            () => 0L,
            (key, value, count) => count + 1);
    }

    public ITable<TKey, TValue> Reduce(Func<TValue, TValue, TValue> reducer)
    {
        var storeName = _builder.NextStoreName("REDUCE");
        var store = Stores.KeyValueStore<TKey, TValue>(storeName);
        _builder.AddStateStore(store);

        var nodeName = _builder.NextNodeName("REDUCE");
        var node = new AggregateNode<TKey, TValue, TValue>(
            nodeName, _keySerde, _valueSerde, _valueSerde,
            storeName,
            () => default!,
            (k, v, agg) => agg == null ? v : reducer(agg, v));

        _sourceStream.SourceNode.AddChild(node);
        return new TableImpl<TKey, TValue>(_builder, storeName,
            new StreamImpl<TKey, TValue>(_builder, nodeName, node, _keySerde, _valueSerde),
            _keySerde, _valueSerde);
    }

    public ITable<TKey, TAggregate> Aggregate<TAggregate>(
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> aggregator)
    {
        var aggSerde = Serdes.Json<TAggregate>();
        var storeName = _builder.NextStoreName("AGGREGATE");
        var store = Stores.KeyValueStore<TKey, TAggregate>(storeName);
        _builder.AddStateStore(store);

        var nodeName = _builder.NextNodeName("AGGREGATE");
        var node = new AggregateNode<TKey, TValue, TAggregate>(
            nodeName, _keySerde, _valueSerde, aggSerde,
            storeName, initializer, aggregator);

        _sourceStream.SourceNode.AddChild(node);
        return new TableImpl<TKey, TAggregate>(_builder, storeName,
            new StreamImpl<TKey, TAggregate>(_builder, nodeName, node, _keySerde, aggSerde),
            _keySerde, aggSerde);
    }

    public ITimeWindowedStream<TKey, TValue> WindowedBy(TumblingWindows windows)
    {
        return new TimeWindowedStreamImpl<TKey, TValue>(_builder, _sourceStream, _keySerde, _valueSerde, windows);
    }

    public ITimeWindowedStream<TKey, TValue> WindowedBy(HoppingWindows windows)
    {
        return new TimeWindowedStreamImpl<TKey, TValue>(_builder, _sourceStream, _keySerde, _valueSerde, windows);
    }

    public ITimeWindowedStream<TKey, TValue> WindowedBy(SlidingWindows windows)
    {
        return new TimeWindowedStreamImpl<TKey, TValue>(_builder, _sourceStream, _keySerde, _valueSerde, windows);
    }

    public ISessionWindowedStream<TKey, TValue> WindowedBy(SessionWindows windows)
    {
        return new SessionWindowedStreamImpl<TKey, TValue>(_builder, _sourceStream, _keySerde, _valueSerde, windows);
    }
}

/// <summary>
/// Aggregate processor node.
/// </summary>
internal sealed class AggregateNode<TKey, TValue, TAggregate> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly ISerde<TAggregate> _aggSerde;
    private readonly string _storeName;
    private readonly Func<TAggregate> _initializer;
    private readonly Func<TKey, TValue, TAggregate, TAggregate> _aggregator;
    private IKeyValueStore<TKey, TAggregate>? _store;

    public AggregateNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        ISerde<TAggregate> aggSerde,
        string storeName,
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> aggregator)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _aggSerde = aggSerde;
        _storeName = storeName;
        _initializer = initializer;
        _aggregator = aggregator;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _store = context.GetStateStore<IKeyValueStore<TKey, TAggregate>>(_storeName);
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var v = _valueSerde.Deserialize(value);

        TAggregate currentAgg;
        if (_store != null)
        {
            var stored = _store.Get(k);
            currentAgg = stored != null ? stored : _initializer();
        }
        else
        {
            currentAgg = _initializer();
        }

        var newAgg = _aggregator(k, v, currentAgg);
        _store?.Put(k, newAgg);

        var aggBytes = _aggSerde.Serialize(newAgg);
        ForwardToChildren(key, aggBytes, timestamp);
    }

    public override void Close() { }
}

/// <summary>
/// Time-windowed stream implementation.
/// </summary>
internal sealed class TimeWindowedStreamImpl<TKey, TValue> : ITimeWindowedStream<TKey, TValue>
    where TKey : notnull
{
    private readonly StreamsBuilder _builder;
    private readonly StreamImpl<TKey, TValue> _sourceStream;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly Kuestenlogik.Surgewave.Streams.Windows.Windows _windows;

    internal TimeWindowedStreamImpl(
        StreamsBuilder builder,
        StreamImpl<TKey, TValue> sourceStream,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        Kuestenlogik.Surgewave.Streams.Windows.Windows windows)
    {
        _builder = builder;
        _sourceStream = sourceStream;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _windows = windows;
    }

    public ITable<Windowed<TKey>, long> Count()
    {
        return Aggregate(() => 0L, (k, v, count) => count + 1);
    }

    public ITable<Windowed<TKey>, TValue> Reduce(Func<TValue, TValue, TValue> reducer)
    {
        var storeName = _builder.NextStoreName("WINDOWED-REDUCE");
        var windowedKeySerde = Serdes.Json<Windowed<TKey>>();

        var nodeName = _builder.NextNodeName("WINDOWED-REDUCE");
        var node = new WindowedAggregateNode<TKey, TValue, TValue>(
            nodeName, _keySerde, _valueSerde, _valueSerde,
            storeName, _windows,
            () => default!,
            (k, v, agg) => agg == null ? v : reducer(agg, v));

        _sourceStream.SourceNode.AddChild(node);

        return new TableImpl<Windowed<TKey>, TValue>(_builder, storeName,
            new StreamImpl<Windowed<TKey>, TValue>(_builder, nodeName, node, windowedKeySerde, _valueSerde),
            windowedKeySerde, _valueSerde);
    }

    public ITable<Windowed<TKey>, TAggregate> Aggregate<TAggregate>(
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> aggregator)
    {
        return Aggregate(initializer, aggregator, Materialized<TKey, TAggregate>.As(_builder.NextStoreName("WINDOWED-AGG")));
    }

    public ITable<Windowed<TKey>, TAggregate> Aggregate<TAggregate>(
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> aggregator,
        Materialized<TKey, TAggregate> materialized)
    {
        var aggSerde = Serdes.Json<TAggregate>();
        var windowedKeySerde = Serdes.Json<Windowed<TKey>>();
        var storeName = materialized.StoreName ?? _builder.NextStoreName("WINDOWED-AGG");

        _builder.AddStateStore(Stores.WindowStore<TKey, TAggregate>(
            storeName,
            TimeSpan.FromMilliseconds(_windows.Size),
            materialized.Retention ?? TimeSpan.FromDays(1)));

        var nodeName = _builder.NextNodeName("WINDOWED-AGGREGATE");
        var node = new WindowedAggregateNode<TKey, TValue, TAggregate>(
            nodeName, _keySerde, _valueSerde, aggSerde,
            storeName, _windows, initializer, aggregator);

        _sourceStream.SourceNode.AddChild(node);

        return new TableImpl<Windowed<TKey>, TAggregate>(_builder, storeName,
            new StreamImpl<Windowed<TKey>, TAggregate>(_builder, nodeName, node, windowedKeySerde, aggSerde),
            windowedKeySerde, aggSerde);
    }
}

/// <summary>
/// Windowed aggregate processor node.
/// </summary>
internal sealed class WindowedAggregateNode<TKey, TValue, TAggregate> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly ISerde<TAggregate> _aggSerde;
    private readonly string _storeName;
    private readonly Kuestenlogik.Surgewave.Streams.Windows.Windows _windows;
    private readonly Func<TAggregate> _initializer;
    private readonly Func<TKey, TValue, TAggregate, TAggregate> _aggregator;
    private IWindowStore<TKey, TAggregate>? _store;

    public WindowedAggregateNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        ISerde<TAggregate> aggSerde,
        string storeName,
        Kuestenlogik.Surgewave.Streams.Windows.Windows windows,
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> aggregator)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _aggSerde = aggSerde;
        _storeName = storeName;
        _windows = windows;
        _initializer = initializer;
        _aggregator = aggregator;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _store = context.GetStateStore<IWindowStore<TKey, TAggregate>>(_storeName);
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var v = _valueSerde.Deserialize(value);

        foreach (var window in _windows.WindowsFor(timestamp))
        {
            // Grace period enforcement: use watermark if available, fallback to stream time
            var gracePeriodMs = (long)_windows.GracePeriod.TotalMilliseconds;
            var gracePeriodEnd = window.EndMs + gracePeriodMs;
            var watermark = Context?.CurrentWatermark ?? EventTime.Watermark.None;
            var streamTime = !watermark.IsNone ? watermark.Timestamp : (Context?.CurrentStreamTimeMs() ?? timestamp);

            if (gracePeriodMs > 0 && streamTime > gracePeriodEnd)
            {
                Context?.Metrics.RecordLateRecord();
                Context?.Logger.LogDebug(
                    "Dropping late record for window [{Start}, {End}), grace period ended at {GraceEnd}",
                    window.StartMs, window.EndMs, gracePeriodEnd);
                continue;
            }

            TAggregate currentAgg;
            if (_store != null)
            {
                var stored = _store.Fetch(k, window.StartMs);
                currentAgg = stored != null ? stored : _initializer();
            }
            else
            {
                currentAgg = _initializer();
            }

            var newAgg = _aggregator(k, v, currentAgg);
            _store?.Put(k, newAgg, window.StartMs);

            var windowedKey = new Windowed<TKey>(k, window);
            var windowedKeyBytes = Serdes.Json<Windowed<TKey>>().Serialize(windowedKey);
            var aggBytes = _aggSerde.Serialize(newAgg);
            ForwardToChildren(windowedKeyBytes, aggBytes, timestamp);
        }
    }

    public override void Close() { }
}

/// <summary>
/// Session-windowed stream implementation.
/// </summary>
internal sealed class SessionWindowedStreamImpl<TKey, TValue> : ISessionWindowedStream<TKey, TValue>
    where TKey : notnull
{
    private readonly StreamsBuilder _builder;
    private readonly StreamImpl<TKey, TValue> _sourceStream;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly SessionWindows _windows;

    internal SessionWindowedStreamImpl(
        StreamsBuilder builder,
        StreamImpl<TKey, TValue> sourceStream,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        SessionWindows windows)
    {
        _builder = builder;
        _sourceStream = sourceStream;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _windows = windows;
    }

    public ITable<Windowed<TKey>, long> Count()
    {
        return Aggregate(
            () => 0L,
            (k, v, count) => count + 1,
            (k, agg1, agg2) => agg1 + agg2);
    }

    public ITable<Windowed<TKey>, TValue> Reduce(Func<TValue, TValue, TValue> reducer)
    {
        return Aggregate<TValue>(
            () => default!,
            (k, v, agg) => agg is null ? v : reducer(agg, v),
            (k, agg1, agg2) => reducer(agg1, agg2));
    }

    public ITable<Windowed<TKey>, TAggregate> Aggregate<TAggregate>(
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> aggregator,
        Func<TKey, TAggregate, TAggregate, TAggregate> merger)
    {
        var aggSerde = Serdes.Json<TAggregate>();
        var windowedKeySerde = Serdes.Json<Windowed<TKey>>();
        var storeName = _builder.NextStoreName("SESSION-AGG");

        _builder.AddStateStore(Stores.SessionStore<TKey, TAggregate>(
            storeName, TimeSpan.FromDays(1)));

        var nodeName = _builder.NextNodeName("SESSION-AGGREGATE");
        var node = new SessionAggregateNode<TKey, TValue, TAggregate>(
            nodeName, _keySerde, _valueSerde, aggSerde,
            storeName, _windows, initializer, aggregator, merger);

        _sourceStream.SourceNode.AddChild(node);

        return new TableImpl<Windowed<TKey>, TAggregate>(_builder, storeName,
            new StreamImpl<Windowed<TKey>, TAggregate>(_builder, nodeName, node, windowedKeySerde, aggSerde),
            windowedKeySerde, aggSerde);
    }
}

/// <summary>
/// Session aggregate processor node.
/// </summary>
internal sealed class SessionAggregateNode<TKey, TValue, TAggregate> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly ISerde<TAggregate> _aggSerde;
    private readonly string _storeName;
    private readonly SessionWindows _windows;
    private readonly Func<TAggregate> _initializer;
    private readonly Func<TKey, TValue, TAggregate, TAggregate> _aggregator;
    private readonly Func<TKey, TAggregate, TAggregate, TAggregate> _merger;
    private ISessionStore<TKey, TAggregate>? _store;

    public SessionAggregateNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        ISerde<TAggregate> aggSerde,
        string storeName,
        SessionWindows windows,
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> aggregator,
        Func<TKey, TAggregate, TAggregate, TAggregate> merger)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _aggSerde = aggSerde;
        _storeName = storeName;
        _windows = windows;
        _initializer = initializer;
        _aggregator = aggregator;
        _merger = merger;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _store = context.GetStateStore<ISessionStore<TKey, TAggregate>>(_storeName);
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var v = _valueSerde.Deserialize(value);

        var gap = _windows.InactivityGap;

        // Find overlapping sessions
        var sessions = _store?.FindSessions(k, timestamp - gap, timestamp + gap).ToList()
                       ?? new List<KeyValue<Windowed<TKey>, TAggregate>>();

        // Grace period enforcement: use watermark if available, fallback to stream time
        var gracePeriodMs = (long)_windows.GracePeriod.TotalMilliseconds;
        var watermark = Context?.CurrentWatermark ?? EventTime.Watermark.None;
        var streamTime = !watermark.IsNone ? watermark.Timestamp : (Context?.CurrentStreamTimeMs() ?? timestamp);

        if (gracePeriodMs > 0 && sessions.Count > 0)
        {
            // For session windows, check if all matching sessions are past grace period
            var latestSessionEnd = sessions.Max(s => s.Key.Window.EndMs);
            var gracePeriodEnd = latestSessionEnd + gap + gracePeriodMs;

            if (streamTime > gracePeriodEnd)
            {
                Context?.Metrics.RecordLateRecord();
                Context?.Logger.LogDebug(
                    "Dropping late record for session ending at {SessionEnd}, grace period ended at {GraceEnd}",
                    latestSessionEnd, gracePeriodEnd);
                return;
            }
        }

        TAggregate newAgg;
        Window newWindow;

        if (sessions.Count == 0)
        {
            // New session
            newAgg = _aggregator(k, v, _initializer());
            newWindow = new Window(timestamp, timestamp);
        }
        else
        {
            // Merge overlapping sessions
            var minStart = Math.Min(timestamp, sessions.Min(s => s.Key.Window.StartMs));
            var maxEnd = Math.Max(timestamp, sessions.Max(s => s.Key.Window.EndMs));
            newWindow = new Window(minStart, maxEnd);

            // Remove old sessions and merge
            newAgg = _initializer();
            foreach (var session in sessions)
            {
                _store?.Remove(session.Key);
                newAgg = _merger(k, newAgg, session.Value);
            }
            newAgg = _aggregator(k, v, newAgg);
        }

        var windowedKey = new Windowed<TKey>(k, newWindow);
        _store?.Put(windowedKey, newAgg);

        var windowedKeyBytes = Serdes.Json<Windowed<TKey>>().Serialize(windowedKey);
        var aggBytes = _aggSerde.Serialize(newAgg);
        ForwardToChildren(windowedKeyBytes, aggBytes, timestamp);
    }

    public override void Close() { }
}

/// <summary>
/// Implementation of grouped table for aggregations.
/// </summary>
internal sealed class GroupedTableImpl<TKey, TValue> : IGroupedTable<TKey, TValue>
    where TKey : notnull
{
    private readonly StreamsBuilder _builder;
    private readonly object _sourceTable;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly object _keyValueMapper;

    internal GroupedTableImpl(
        StreamsBuilder builder,
        object sourceTable,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        object keyValueMapper)
    {
        _builder = builder;
        _sourceTable = sourceTable;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _keyValueMapper = keyValueMapper;
    }

    public ITable<TKey, long> Count()
    {
        return Aggregate(
            () => 0L,
            (k, v, count) => count + 1,
            (k, v, count) => count - 1);
    }

    public ITable<TKey, TValue> Reduce(Func<TValue, TValue, TValue> adder, Func<TValue, TValue, TValue> subtractor)
    {
        var storeName = _builder.NextStoreName("TABLE-REDUCE");
        var store = Stores.KeyValueStore<TKey, TValue>(storeName);
        _builder.AddStateStore(store);

        var nodeName = _builder.NextNodeName("TABLE-REDUCE");
        var node = new TableReduceNode<TKey, TValue>(
            nodeName, _keySerde, _valueSerde, storeName, adder, subtractor);

        // Connect to source - the keyValueMapper is the re-keying function
        ConnectToSourceTable(node);

        return new TableImpl<TKey, TValue>(_builder, storeName,
            new StreamImpl<TKey, TValue>(_builder, nodeName, node, _keySerde, _valueSerde),
            _keySerde, _valueSerde);
    }

    public ITable<TKey, TAggregate> Aggregate<TAggregate>(
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> adder,
        Func<TKey, TValue, TAggregate, TAggregate> subtractor)
    {
        var aggSerde = Serdes.Json<TAggregate>();
        var storeName = _builder.NextStoreName("TABLE-AGGREGATE");
        var store = Stores.KeyValueStore<TKey, TAggregate>(storeName);
        _builder.AddStateStore(store);

        var nodeName = _builder.NextNodeName("TABLE-AGGREGATE");
        var node = new TableAggregateNode<TKey, TValue, TAggregate>(
            nodeName, _keySerde, _valueSerde, aggSerde, storeName,
            initializer, adder, subtractor);

        // Connect to source
        ConnectToSourceTable(node);

        return new TableImpl<TKey, TAggregate>(_builder, storeName,
            new StreamImpl<TKey, TAggregate>(_builder, nodeName, node, _keySerde, aggSerde),
            _keySerde, aggSerde);
    }

    private void ConnectToSourceTable(ProcessorNode node)
    {
        // Use reflection to access the internal source stream from TableImpl
        var sourceTableType = _sourceTable.GetType();
        var sourceStreamField = sourceTableType.GetField("_sourceStream",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        if (sourceStreamField?.GetValue(_sourceTable) is StreamImpl<TKey, TValue> sourceStream)
        {
            sourceStream.SourceNode.AddChild(node);
        }
    }
}

/// <summary>
/// Table reduce processor node with add/subtract semantics for table updates.
/// </summary>
internal sealed class TableReduceNode<TKey, TValue> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly string _storeName;
    private readonly Func<TValue, TValue, TValue> _adder;
    private readonly Func<TValue, TValue, TValue> _subtractor;
    private IKeyValueStore<TKey, TValue>? _store;
    private IKeyValueStore<TKey, TValue>? _oldValueStore;

    public TableReduceNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        string storeName,
        Func<TValue, TValue, TValue> adder,
        Func<TValue, TValue, TValue> subtractor)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _storeName = storeName;
        _adder = adder;
        _subtractor = subtractor;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _store = context.GetStateStore<IKeyValueStore<TKey, TValue>>(_storeName);
        // Old value store is optional - used for tracking previous values
        _oldValueStore = context.GetStateStore<IKeyValueStore<TKey, TValue>>(_storeName + "-oldvalues");
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var hasNewValue = value.Length > 0;
        var newValue = hasNewValue ? _valueSerde.Deserialize(value) : default!;

        // Get current aggregate
        TValue? currentAgg = _store != null ? _store.Get(k) : default;

        // Get old value for this key (if tracking)
        TValue? oldValue = _oldValueStore != null ? _oldValueStore.Get(k) : default;

        TValue resultAgg;

        if (oldValue != null && currentAgg != null)
        {
            // Subtract old value, then add new value
            var subtracted = _subtractor(currentAgg, oldValue);
            resultAgg = hasNewValue ? _adder(subtracted, newValue) : subtracted;
        }
        else if (hasNewValue)
        {
            // Just add new value
            resultAgg = currentAgg != null ? _adder(currentAgg, newValue) : newValue;
        }
        else
        {
            // Value deleted with no old value tracking - keep current or use default
            resultAgg = currentAgg!;
        }

        // Store new aggregate
        _store?.Put(k, resultAgg);

        // Track old value for future updates
        if (hasNewValue)
        {
            _oldValueStore?.Put(k, newValue);
        }
        else
        {
            _oldValueStore?.Delete(k);
        }

        // Forward result
        var aggBytes = _valueSerde.Serialize(resultAgg);
        ForwardToChildren(key, aggBytes, timestamp);
    }

    public override void Close() { }
}

/// <summary>
/// Table aggregate processor node with add/subtract semantics for table updates.
/// </summary>
internal sealed class TableAggregateNode<TKey, TValue, TAggregate> : ProcessorNode
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly ISerde<TAggregate> _aggSerde;
    private readonly string _storeName;
    private readonly Func<TAggregate> _initializer;
    private readonly Func<TKey, TValue, TAggregate, TAggregate> _adder;
    private readonly Func<TKey, TValue, TAggregate, TAggregate> _subtractor;
    private IKeyValueStore<TKey, TAggregate>? _store;
    private IKeyValueStore<TKey, TValue>? _oldValueStore;

    public TableAggregateNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        ISerde<TAggregate> aggSerde,
        string storeName,
        Func<TAggregate> initializer,
        Func<TKey, TValue, TAggregate, TAggregate> adder,
        Func<TKey, TValue, TAggregate, TAggregate> subtractor)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _aggSerde = aggSerde;
        _storeName = storeName;
        _initializer = initializer;
        _adder = adder;
        _subtractor = subtractor;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _store = context.GetStateStore<IKeyValueStore<TKey, TAggregate>>(_storeName);
        // Old value store is optional - used for tracking previous values
        _oldValueStore = context.GetStateStore<IKeyValueStore<TKey, TValue>>(_storeName + "-oldvalues");
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var k = _keySerde.Deserialize(key);
        var hasNewValue = value.Length > 0;
        var newValue = hasNewValue ? _valueSerde.Deserialize(value) : default!;

        // Get current aggregate - use initializer if store is null or key not found
        TAggregate currentAgg;
        if (_store != null)
        {
            var stored = _store.Get(k);
            currentAgg = stored != null ? stored : _initializer();
        }
        else
        {
            currentAgg = _initializer();
        }

        // Get old value for this key (if tracking)
        TValue? oldValue = _oldValueStore != null ? _oldValueStore.Get(k) : default;

        if (oldValue != null)
        {
            // Subtract old value first
            currentAgg = _subtractor(k, oldValue, currentAgg);
        }

        TAggregate newAgg;
        if (hasNewValue)
        {
            // Add new value
            newAgg = _adder(k, newValue, currentAgg);
        }
        else
        {
            // Value deleted - just use subtracted result
            newAgg = currentAgg;
        }

        // Store new aggregate
        _store?.Put(k, newAgg);

        // Track old value for future updates
        if (hasNewValue)
        {
            _oldValueStore?.Put(k, newValue);
        }
        else
        {
            _oldValueStore?.Delete(k);
        }

        // Forward result
        var aggBytes = _aggSerde.Serialize(newAgg);
        ForwardToChildren(key, aggBytes, timestamp);
    }

    public override void Close() { }
}
