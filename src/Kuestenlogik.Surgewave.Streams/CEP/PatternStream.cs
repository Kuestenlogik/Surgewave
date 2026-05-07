using Kuestenlogik.Surgewave.Streams.Processors;

namespace Kuestenlogik.Surgewave.Streams.CEP;

/// <summary>
/// A stream of pattern matches.
/// </summary>
/// <typeparam name="T">The event type</typeparam>
public interface IPatternStream<T>
{
    /// <summary>
    /// Processes pattern matches.
    /// </summary>
    IStream<TKey, TValue> Process<TKey, TValue>(
        Func<PatternMatch<T>, KeyValue<TKey, TValue>> processor);

    /// <summary>
    /// Processes pattern matches with access to timed-out patterns.
    /// </summary>
    IStream<TKey, TValue> Process<TKey, TValue>(
        Func<PatternMatch<T>, KeyValue<TKey, TValue>> matchProcessor,
        Func<PatternMatch<T>, KeyValue<TKey, TValue>> timeoutProcessor);

    /// <summary>
    /// Gets the underlying stream of matches as key-value pairs.
    /// </summary>
    IStream<string, PatternMatch<T>> AsStream();

    /// <summary>
    /// Selects results from pattern matches.
    /// </summary>
    IStream<TKey, TValue> Select<TKey, TValue>(
        Func<PatternMatch<T>, IEnumerable<KeyValue<TKey, TValue>>> selector);

    /// <summary>
    /// Flattens pattern matches into individual events.
    /// </summary>
    IStream<TKey, TValue> FlatSelect<TKey, TValue>(
        Func<PatternMatch<T>, IEnumerable<KeyValue<TKey, TValue>>> selector);

    /// <summary>
    /// Processes pattern matches using a topology-integrated CEP processor node
    /// that uses event time and state store backing for NFA state recovery.
    /// </summary>
    IStream<TKey, TValue> ProcessWithTopology<TKey, TValue>(
        Func<PatternMatch<T>, KeyValue<TKey, TValue>> processor);
}

/// <summary>
/// Extension methods for CEP pattern matching on streams.
/// </summary>
public static class CEP
{
    /// <summary>
    /// Applies a pattern to a stream and returns a stream of matches.
    /// </summary>
    public static IPatternStream<TValue> Pattern<TKey, TValue>(
        this IStream<TKey, TValue> stream,
        Pattern<TValue> pattern)
    {
        return new PatternStreamImpl<TKey, TValue>(stream, pattern);
    }
}

/// <summary>
/// Implementation of pattern stream using NFA-based matching.
/// </summary>
internal sealed class PatternStreamImpl<TKey, TValue> : IPatternStream<TValue>
{
    private readonly IStream<TKey, TValue> _source;
    private readonly Pattern<TValue> _pattern;
    private readonly List<NFAState<TValue>> _activeStates = new();
    private readonly IReadOnlyList<Pattern<TValue>> _patternSequence;

    public PatternStreamImpl(IStream<TKey, TValue> source, Pattern<TValue> pattern)
    {
        _source = source;
        _pattern = pattern;
        _patternSequence = pattern.GetPatternSequence();
    }

    public IStream<TResultKey, TResultValue> Process<TResultKey, TResultValue>(
        Func<PatternMatch<TValue>, KeyValue<TResultKey, TResultValue>> processor)
    {
        return Process(processor, _ => default!);
    }

    public IStream<TResultKey, TResultValue> Process<TResultKey, TResultValue>(
        Func<PatternMatch<TValue>, KeyValue<TResultKey, TResultValue>> matchProcessor,
        Func<PatternMatch<TValue>, KeyValue<TResultKey, TResultValue>> timeoutProcessor)
    {
        // Transform the source stream, applying pattern matching
        return _source.FlatMap<TResultKey, TResultValue>((key, value) =>
        {
            var results = new List<KeyValue<TResultKey, TResultValue>>();
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Start a new NFA state for each incoming event
            _activeStates.Add(new NFAState<TValue>(_pattern, timestamp));

            // Process all active states
            var nextStates = new List<NFAState<TValue>>();
            foreach (var state in _activeStates)
            {
                var result = state.TryMatch(value, timestamp, _patternSequence);

                if (result.CompletedMatch != null)
                {
                    var processed = matchProcessor(result.CompletedMatch);
                    if (processed.Key != null || processed.Value != null)
                        results.Add(processed);

                    // Handle skip strategy
                    if (_pattern.SkipStrategy != AfterMatchSkipStrategy.NoSkip)
                        continue;
                }

                if (result.TimedOut && timeoutProcessor != null)
                {
                    var processed = timeoutProcessor(state.PartialMatch);
                    if (processed.Key != null || processed.Value != null)
                        results.Add(processed);
                    continue;
                }

                if (!result.Invalidated)
                {
                    nextStates.AddRange(result.Continuations);
                }
            }

            _activeStates.Clear();
            _activeStates.AddRange(nextStates);

            return results;
        });
    }

    public IStream<string, PatternMatch<TValue>> AsStream()
    {
        return Process<string, PatternMatch<TValue>>(match =>
            new KeyValue<string, PatternMatch<TValue>>(
                string.Join("_", match.PatternNames),
                match));
    }

    public IStream<TResultKey, TResultValue> Select<TResultKey, TResultValue>(
        Func<PatternMatch<TValue>, IEnumerable<KeyValue<TResultKey, TResultValue>>> selector)
    {
        return _source.FlatMap<TResultKey, TResultValue>((key, value) =>
        {
            var results = new List<KeyValue<TResultKey, TResultValue>>();
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            _activeStates.Add(new NFAState<TValue>(_pattern, timestamp));

            var nextStates = new List<NFAState<TValue>>();
            foreach (var state in _activeStates)
            {
                var result = state.TryMatch(value, timestamp, _patternSequence);

                if (result.CompletedMatch != null)
                {
                    results.AddRange(selector(result.CompletedMatch));
                }

                if (!result.Invalidated && !result.TimedOut)
                {
                    nextStates.AddRange(result.Continuations);
                }
            }

            _activeStates.Clear();
            _activeStates.AddRange(nextStates);

            return results;
        });
    }

    public IStream<TResultKey, TResultValue> FlatSelect<TResultKey, TResultValue>(
        Func<PatternMatch<TValue>, IEnumerable<KeyValue<TResultKey, TResultValue>>> selector)
    {
        return Select(selector);
    }

    public IStream<TResultKey, TResultValue> ProcessWithTopology<TResultKey, TResultValue>(
        Func<PatternMatch<TValue>, KeyValue<TResultKey, TResultValue>> processor)
    {
        if (_source is StreamImpl<TKey, TValue> streamImpl)
        {
            var keyOutSerde = Serdes.Json<TResultKey>();
            var valueOutSerde = Serdes.Json<TResultValue>();
            var keyInSerde = Serdes.Json<TKey>();
            var valueInSerde = Serdes.Json<TValue>();

            var nodeName = $"CEP-{Guid.NewGuid():N}";
            var stateStoreName = $"{nodeName}-STATE";

            var node = new CEPProcessorNode<TKey, TValue, TResultKey, TResultValue>(
                nodeName, keyInSerde, valueInSerde, keyOutSerde, valueOutSerde,
                _pattern, processor, stateStoreName);

            streamImpl.SourceNode.AddChild(node);

            return new StreamImpl<TResultKey, TResultValue>(
                GetBuilder(streamImpl), nodeName, node, keyOutSerde, valueOutSerde);
        }

        // Fallback to FlatMap-based implementation
        return Process(processor);
    }

    private static StreamsBuilder GetBuilder(StreamImpl<TKey, TValue> stream)
    {
        // Access the builder via reflection since it's internal
        var field = typeof(StreamImpl<TKey, TValue>).GetField("_builder",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        return (StreamsBuilder)field!.GetValue(stream)!;
    }
}
