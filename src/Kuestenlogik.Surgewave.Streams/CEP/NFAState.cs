namespace Kuestenlogik.Surgewave.Streams.CEP;

/// <summary>
/// Represents the state of a Non-deterministic Finite Automaton (NFA) for pattern matching.
/// </summary>
/// <typeparam name="T">The event type</typeparam>
internal sealed class NFAState<T>
{
    private readonly Pattern<T> _pattern;
    private readonly int _currentPatternIndex;
    private readonly int _matchCount;
    private readonly PatternMatch<T> _partialMatch;
    private readonly long _startTimestamp;

    public NFAState(Pattern<T> pattern, long timestamp)
    {
        var sequence = pattern.GetPatternSequence();
        _pattern = sequence[0];
        _currentPatternIndex = 0;
        _matchCount = 0;
        _partialMatch = new PatternMatch<T>(timestamp);
        _startTimestamp = timestamp;
    }

    private NFAState(Pattern<T> pattern, int currentIndex, int matchCount, PatternMatch<T> partialMatch, long startTimestamp)
    {
        _pattern = pattern;
        _currentPatternIndex = currentIndex;
        _matchCount = matchCount;
        _partialMatch = partialMatch;
        _startTimestamp = startTimestamp;
    }

    public Pattern<T> CurrentPattern => _pattern;
    public int CurrentPatternIndex => _currentPatternIndex;
    public int MatchCount => _matchCount;
    public PatternMatch<T> PartialMatch => _partialMatch;
    public long StartTimestamp => _startTimestamp;

    /// <summary>
    /// Attempts to match an event against the current pattern state.
    /// </summary>
    public NFATransitionResult<T> TryMatch(T @event, long timestamp, IReadOnlyList<Pattern<T>> patternSequence)
    {
        var result = new NFATransitionResult<T>();
        var currentPattern = patternSequence[_currentPatternIndex];
        var quantifier = currentPattern.PatternQuantifier;

        // Check time constraint
        if (currentPattern.WithinTime.HasValue)
        {
            var elapsed = timestamp - _startTimestamp;
            if (elapsed > currentPattern.WithinTime.Value.TotalMilliseconds)
            {
                result.TimedOut = true;
                return result;
            }
        }

        // Check if event matches the condition
        bool matches = currentPattern.Condition?.Invoke(@event) ?? true;

        // Handle NOT patterns
        if (currentPattern.Type == PatternType.NotStrict || currentPattern.Type == PatternType.NotRelaxed)
        {
            if (matches)
            {
                // Event matched a NOT pattern - invalidate this path
                result.Invalidated = true;
                return result;
            }
            // Continue without this event
            result.AddContinuation(this);
            return result;
        }

        if (!matches)
        {
            // Handle non-strict contiguity - we can skip this event
            if (currentPattern.Type == PatternType.Relaxed ||
                currentPattern.Type == PatternType.NonDeterministicRelaxed)
            {
                result.AddContinuation(this);
            }
            return result;
        }

        // Event matches
        var newMatchCount = _matchCount + 1;

        // Create updated partial match
        var newPartialMatch = ClonePartialMatch();
        newPartialMatch.Add(currentPattern.Name, @event, timestamp);

        // Check quantifier
        bool canContinueSamePattern = newMatchCount < quantifier.Max;
        bool canAdvanceToNext = newMatchCount >= quantifier.Min;

        // For non-deterministic relaxed, keep the current state too
        if (currentPattern.Type == PatternType.NonDeterministicRelaxed && canContinueSamePattern)
        {
            result.AddContinuation(this);
        }

        // Continue matching same pattern if quantifier allows
        if (canContinueSamePattern && !quantifier.IsConsecutive)
        {
            var continueState = new NFAState<T>(
                _pattern,
                _currentPatternIndex,
                newMatchCount,
                newPartialMatch,
                _startTimestamp);
            result.AddContinuation(continueState);
        }

        // Advance to next pattern
        if (canAdvanceToNext)
        {
            int nextIndex = _currentPatternIndex + 1;
            if (nextIndex >= patternSequence.Count)
            {
                // Pattern complete!
                result.CompletedMatch = newPartialMatch;
            }
            else
            {
                var advanceState = new NFAState<T>(
                    patternSequence[nextIndex],
                    nextIndex,
                    0,
                    newPartialMatch,
                    _startTimestamp);
                result.AddContinuation(advanceState);
            }
        }

        return result;
    }

    private PatternMatch<T> ClonePartialMatch()
    {
        var clone = new PatternMatch<T>(_startTimestamp);
        foreach (var kv in _partialMatch)
        {
            foreach (var e in kv.Value)
            {
                clone.Add(kv.Key, e, 0);
            }
        }
        return clone;
    }
}

/// <summary>
/// Result of an NFA state transition.
/// </summary>
internal sealed class NFATransitionResult<T>
{
    private List<NFAState<T>>? _continuations;

    public PatternMatch<T>? CompletedMatch { get; set; }
    public bool TimedOut { get; set; }
    public bool Invalidated { get; set; }

    public IReadOnlyList<NFAState<T>> Continuations => _continuations ?? (IReadOnlyList<NFAState<T>>)Array.Empty<NFAState<T>>();

    public void AddContinuation(NFAState<T> state)
    {
        _continuations ??= new List<NFAState<T>>();
        _continuations.Add(state);
    }
}

/// <summary>
/// Serializable snapshot of NFA state for state store backing.
/// Allows CEP pattern matching state to survive restarts.
/// </summary>
public sealed class NFAStateSnapshot
{
    public required int CurrentPatternIndex { get; init; }
    public required int MatchCount { get; init; }
    public required long StartTimestamp { get; init; }
    public required Dictionary<string, int> PatternMatchCounts { get; init; }
}
