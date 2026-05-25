using Kuestenlogik.Surgewave.Streams.Processors;

namespace Kuestenlogik.Surgewave.Streams.EventTime;

/// <summary>
/// Extension methods for adding event time semantics to streams.
/// </summary>
public static class EventTimeExtensions
{
    /// <summary>
    /// Assigns timestamps and watermarks to the stream.
    /// Injects a WatermarkProcessorNode into the topology for proper watermark propagation.
    /// </summary>
    public static IEventTimeStream<TKey, TValue> AssignTimestampsAndWatermarks<TKey, TValue>(
        this IStream<TKey, TValue> stream,
        WatermarkStrategy<TValue> strategy)
    {
        // Inject a WatermarkProcessorNode into the topology if we have access to StreamImpl internals
        if (stream is StreamImpl<TKey, TValue> streamImpl)
        {
            var keySerde = Serdes.Json<TKey>();
            var valueSerde = Serdes.Json<TValue>();
            var generator = strategy.CreateWatermarkGenerator();
            var assigner = strategy.GetTimestampAssigner();

            var watermarkNode = new WatermarkProcessorNode<TKey, TValue>(
                $"WATERMARK-{Guid.NewGuid():N}",
                keySerde, valueSerde, assigner, generator);

            streamImpl.SourceNode.AddChild(watermarkNode);

            // Return an event time stream backed by the watermark node
            return new EventTimeStreamImpl<TKey, TValue>(stream, strategy, generator);
        }

        return new EventTimeStreamImpl<TKey, TValue>(stream, strategy);
    }

    /// <summary>
    /// Sets a processing time characteristic (no event time semantics).
    /// </summary>
    public static IStream<TKey, TValue> WithProcessingTime<TKey, TValue>(
        this IStream<TKey, TValue> stream)
    {
        return stream;
    }
}

/// <summary>
/// A stream with event time semantics.
/// </summary>
public interface IEventTimeStream<TKey, TValue> : IStream<TKey, TValue>
{
    /// <summary>
    /// Gets the current watermark.
    /// </summary>
    Watermark CurrentWatermark { get; }

    /// <summary>
    /// Sets the allowed lateness for windows.
    /// </summary>
    IEventTimeStream<TKey, TValue> AllowedLateness(TimeSpan lateness);

    /// <summary>
    /// Sets a side output for late elements.
    /// </summary>
    IEventTimeStream<TKey, TValue> SideOutputLateData(string outputTag);

    /// <summary>
    /// Gets late elements that arrived after the watermark.
    /// </summary>
    IStream<TKey, TValue>? GetLateDataStream(string outputTag);
}

/// <summary>
/// Implementation of event time stream.
/// </summary>
internal sealed class EventTimeStreamImpl<TKey, TValue> : IEventTimeStream<TKey, TValue>
{
    private readonly IStream<TKey, TValue> _source;
    private readonly WatermarkStrategy<TValue> _strategy;
    private readonly IWatermarkGenerator<TValue> _generator;
    private readonly ITimestampAssigner<TValue> _timestampAssigner;
    private TimeSpan _allowedLateness = TimeSpan.Zero;
    private readonly Dictionary<string, List<(TKey, TValue)>> _lateData = new();

    public EventTimeStreamImpl(IStream<TKey, TValue> source, WatermarkStrategy<TValue> strategy)
    {
        _source = source;
        _strategy = strategy;
        _generator = strategy.CreateWatermarkGenerator();
        _timestampAssigner = strategy.GetTimestampAssigner();
    }

    internal EventTimeStreamImpl(IStream<TKey, TValue> source, WatermarkStrategy<TValue> strategy, IWatermarkGenerator<TValue> generator)
    {
        _source = source;
        _strategy = strategy;
        _generator = generator;
        _timestampAssigner = strategy.GetTimestampAssigner();
    }

    public Watermark CurrentWatermark => _generator.GetCurrentWatermark();

    public IEventTimeStream<TKey, TValue> AllowedLateness(TimeSpan lateness)
    {
        _allowedLateness = lateness;
        return this;
    }

    public IEventTimeStream<TKey, TValue> SideOutputLateData(string outputTag)
    {
        _lateData[outputTag] = new List<(TKey, TValue)>();
        return this;
    }

    public IStream<TKey, TValue>? GetLateDataStream(string outputTag)
    {
        // Would return a stream backed by the late data collection
        return null;
    }

    // Delegate all IStream methods to the source with timestamp/watermark handling
    public IStream<TKey, TValue> Filter(Func<TKey, TValue, bool> predicate) =>
        Wrap(_source.Filter(predicate));

    public IStream<TKey, TValue> FilterNot(Func<TKey, TValue, bool> predicate) =>
        Wrap(_source.FilterNot(predicate));

    public IStream<TKey, TNewValue> MapValues<TNewValue>(Func<TValue, TNewValue> mapper) =>
        _source.MapValues(mapper);

    public IStream<TKey, TNewValue> MapValues<TNewValue>(Func<TKey, TValue, TNewValue> mapper) =>
        _source.MapValues(mapper);

    public IStream<TNewKey, TNewValue> Map<TNewKey, TNewValue>(
        Func<TKey, TValue, KeyValue<TNewKey, TNewValue>> mapper) =>
        _source.Map(mapper);

    public IStream<TNewKey, TValue> SelectKey<TNewKey>(Func<TKey, TValue, TNewKey> mapper) =>
        _source.SelectKey(mapper);

    public IStream<TKey, TNewValue> FlatMapValues<TNewValue>(Func<TValue, IEnumerable<TNewValue>> mapper) =>
        _source.FlatMapValues(mapper);

    public IStream<TNewKey, TNewValue> FlatMap<TNewKey, TNewValue>(
        Func<TKey, TValue, IEnumerable<KeyValue<TNewKey, TNewValue>>> mapper) =>
        _source.FlatMap(mapper);

    public IStream<TKey, TNewValue> TransformValues<TNewValue>(
        Processors.IValueTransformerWithKey<TKey, TValue, TNewValue> transformer,
        params string[] stateStoreNames) =>
        _source.TransformValues(transformer, stateStoreNames);

    public IStream<TKey, TValue> Peek(Action<TKey, TValue> action) =>
        Wrap(_source.Peek(action));

    public IStream<TKey, TValue>[] Branch(params Func<TKey, TValue, bool>[] predicates) =>
        _source.Branch(predicates);

    public IStream<TKey, TValue> Merge(IStream<TKey, TValue> other) =>
        Wrap(_source.Merge(other));

    public IStream<TKey, TValue> Repartition() => Wrap(_source.Repartition());

    public IStream<TNewKey, TValue> Repartition<TNewKey>(Func<TKey, TValue, TNewKey> partitioner) =>
        _source.Repartition(partitioner);

    public IGroupedStream<TKey, TValue> GroupByKey() => _source.GroupByKey();

    public IGroupedStream<TNewKey, TValue> GroupBy<TNewKey>(Func<TKey, TValue, TNewKey> keySelector) =>
        _source.GroupBy(keySelector);

    public ITable<TKey, TValue> ToTable() => _source.ToTable();

    public IStream<TKey, TResult> Join<TOther, TResult>(
        IStream<TKey, TOther> other,
        Func<TValue, TOther, TResult> joiner,
        Windows.JoinWindows windows) =>
        _source.Join(other, joiner, windows);

    public IStream<TKey, TResult> LeftJoin<TOther, TResult>(
        IStream<TKey, TOther> other,
        Func<TValue, TOther?, TResult> joiner,
        Windows.JoinWindows windows) =>
        _source.LeftJoin(other, joiner, windows);

    public IStream<TKey, TResult> OuterJoin<TOther, TResult>(
        IStream<TKey, TOther> other,
        Func<TValue?, TOther?, TResult> joiner,
        Windows.JoinWindows windows) =>
        _source.OuterJoin(other, joiner, windows);

    public IStream<TKey, TResult> Join<TTableValue, TResult>(
        ITable<TKey, TTableValue> table,
        Func<TValue, TTableValue, TResult> joiner) =>
        _source.Join(table, joiner);

    public IStream<TKey, TResult> LeftJoin<TTableValue, TResult>(
        ITable<TKey, TTableValue> table,
        Func<TValue, TTableValue?, TResult> joiner) =>
        _source.LeftJoin(table, joiner);

    public IStream<TKey, TResult> Join<TGlobalKey, TGlobalValue, TResult>(
        IGlobalTable<TGlobalKey, TGlobalValue> globalTable,
        Func<TKey, TValue, TGlobalKey> keySelector,
        Func<TValue, TGlobalValue, TResult> joiner) =>
        _source.Join(globalTable, keySelector, joiner);

    public IStream<TKey, TValue> WithRetry(int maxRetries = 3) => _source.WithRetry(maxRetries);
    public IStream<TKey, TValue> RateLimit(int recordsPerSecond) => _source.RateLimit(recordsPerSecond);
    public IStream<TKey, TValue> Parallel(int degreeOfParallelism) => _source.Parallel(degreeOfParallelism);

    public void To(string topic) => _source.To(topic);
    public void To(Func<TKey, TValue, string> topicExtractor) => _source.To(topicExtractor);
    public void ForEach(Action<TKey, TValue> action) => _source.ForEach(action);
    public void Print() => _source.Print();

    private IEventTimeStream<TKey, TValue> Wrap(IStream<TKey, TValue> stream)
    {
        return new EventTimeStreamImpl<TKey, TValue>(stream, _strategy);
    }
}
