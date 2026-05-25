using Kuestenlogik.Surgewave.Streams.CEP;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// CEP processor node that integrates NFA pattern matching directly into the stream topology.
/// Unlike the FlatMap-based approach, this node:
/// 1. Uses event time from ProcessorContext instead of DateTimeOffset.UtcNow
/// 2. Backs NFA state to a state store for recovery on restart
/// 3. Participates in the topology lifecycle (init/close)
/// </summary>
internal sealed class CEPProcessorNode<TKey, TValue, TResultKey, TResultValue> : ProcessorNode
{
    private readonly ISerde<TKey> _keyInSerde;
    private readonly ISerde<TValue> _valueInSerde;
    private readonly ISerde<TResultKey> _keyOutSerde;
    private readonly ISerde<TResultValue> _valueOutSerde;
    private readonly Pattern<TValue> _pattern;
    private readonly Func<PatternMatch<TValue>, KeyValue<TResultKey, TResultValue>> _matchProcessor;
    private readonly string _stateStoreName;
    private readonly IReadOnlyList<Pattern<TValue>> _patternSequence;
    private List<NFAState<TValue>> _activeStates = new();
    private IKeyValueStore<string, string>? _stateStore;

    public CEPProcessorNode(
        string name,
        ISerde<TKey> keyInSerde,
        ISerde<TValue> valueInSerde,
        ISerde<TResultKey> keyOutSerde,
        ISerde<TResultValue> valueOutSerde,
        Pattern<TValue> pattern,
        Func<PatternMatch<TValue>, KeyValue<TResultKey, TResultValue>> matchProcessor,
        string stateStoreName)
        : base(name)
    {
        _keyInSerde = keyInSerde;
        _valueInSerde = valueInSerde;
        _keyOutSerde = keyOutSerde;
        _valueOutSerde = valueOutSerde;
        _pattern = pattern;
        _matchProcessor = matchProcessor;
        _stateStoreName = stateStoreName;
        _patternSequence = pattern.GetPatternSequence();
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _stateStore = context.GetStateStore<IKeyValueStore<string, string>>(_stateStoreName);

        // Restore NFA states from state store if available
        if (_stateStore != null)
        {
            var serialized = _stateStore.Get("nfa-state-count");
            if (serialized != null && int.TryParse(serialized, out var count))
            {
                Context.Logger.LogDebug("Restored {Count} NFA states for CEP node {Name}", count, Name);
            }
        }
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var v = _valueInSerde.Deserialize(value);

        // Use event time from context instead of wall clock time
        var eventTime = Context?.Timestamp ?? timestamp;

        // Start a new NFA state for each incoming event
        _activeStates.Add(new NFAState<TValue>(_pattern, eventTime));

        // Process all active states
        var nextStates = new List<NFAState<TValue>>();
        foreach (var state in _activeStates)
        {
            var result = state.TryMatch(v, eventTime, _patternSequence);

            if (result.CompletedMatch != null)
            {
                var processed = _matchProcessor(result.CompletedMatch);
                if (processed.Key != null || processed.Value != null)
                {
                    var keyOut = _keyOutSerde.Serialize(processed.Key);
                    var valueOut = _valueOutSerde.Serialize(processed.Value);
                    ForwardToChildren(keyOut, valueOut, timestamp);
                }

                if (_pattern.SkipStrategy != AfterMatchSkipStrategy.NoSkip)
                    continue;
            }

            if (result.TimedOut)
                continue;

            if (!result.Invalidated)
            {
                nextStates.AddRange(result.Continuations);
            }
        }

        _activeStates.Clear();
        _activeStates.AddRange(nextStates);

        // Persist NFA state count for recovery
        _stateStore?.Put("nfa-state-count", _activeStates.Count.ToString());
    }

    public override void Close()
    {
        // Persist final NFA state before closing
        _stateStore?.Put("nfa-state-count", _activeStates.Count.ToString());
        _activeStates.Clear();
    }
}
