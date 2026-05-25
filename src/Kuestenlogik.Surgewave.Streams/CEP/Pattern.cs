namespace Kuestenlogik.Surgewave.Streams.CEP;

/// <summary>
/// A pattern that defines a sequence of events to match.
/// Provides a Flink CEP-style fluent API for defining complex event patterns.
/// </summary>
/// <typeparam name="T">The event type</typeparam>
public sealed class Pattern<T>
{
    private readonly string _name;
    private readonly Pattern<T>? _previous;
    private readonly PatternType _patternType;
    private Func<T, bool>? _condition;
    private Func<T, T, bool>? _iterativeCondition;
    private Quantifier _quantifier = Quantifier.One;
    private TimeSpan? _within;
    private AfterMatchSkipStrategy _skipStrategy = AfterMatchSkipStrategy.NoSkip;

    private Pattern(string name, Pattern<T>? previous, PatternType patternType)
    {
        _name = name;
        _previous = previous;
        _patternType = patternType;
    }

    /// <summary>
    /// Creates a new pattern starting with the given name.
    /// </summary>
    public static Pattern<T> Begin(string name)
    {
        return new Pattern<T>(name, null, PatternType.Start);
    }

    /// <summary>
    /// Adds a condition that events must satisfy.
    /// </summary>
    public Pattern<T> Where(Func<T, bool> condition)
    {
        if (_condition == null)
            _condition = condition;
        else
        {
            var prevCondition = _condition;
            _condition = e => prevCondition(e) && condition(e);
        }
        return this;
    }

    /// <summary>
    /// Adds an OR condition.
    /// </summary>
    public Pattern<T> Or(Func<T, bool> condition)
    {
        if (_condition == null)
            _condition = condition;
        else
        {
            var prevCondition = _condition;
            _condition = e => prevCondition(e) || condition(e);
        }
        return this;
    }

    /// <summary>
    /// Adds a condition that can reference previously matched events.
    /// </summary>
    public Pattern<T> Where(Func<T, T, bool> iterativeCondition)
    {
        _iterativeCondition = iterativeCondition;
        return this;
    }

    /// <summary>
    /// Adds a NOT condition (event must not match).
    /// </summary>
    public Pattern<T> Subtype<TSubtype>() where TSubtype : T
    {
        var prevCondition = _condition;
        _condition = e => e is TSubtype && (prevCondition?.Invoke(e) ?? true);
        return this;
    }

    /// <summary>
    /// Specifies that the pattern expects exactly one matching event (default).
    /// </summary>
    public Pattern<T> Times(int times)
    {
        _quantifier = new Quantifier(times, times);
        return this;
    }

    /// <summary>
    /// Specifies that the pattern expects between min and max matching events.
    /// </summary>
    public Pattern<T> Times(int min, int max)
    {
        _quantifier = new Quantifier(min, max);
        return this;
    }

    /// <summary>
    /// Specifies that the pattern expects one or more matching events.
    /// </summary>
    public Pattern<T> OneOrMore()
    {
        _quantifier = Quantifier.OneOrMore;
        return this;
    }

    /// <summary>
    /// Specifies that the pattern expects zero or more matching events.
    /// </summary>
    public Pattern<T> ZeroOrMore()
    {
        _quantifier = Quantifier.ZeroOrMore;
        return this;
    }

    /// <summary>
    /// Specifies that the pattern is optional.
    /// </summary>
    public Pattern<T> Optional()
    {
        _quantifier = Quantifier.Optional;
        return this;
    }

    /// <summary>
    /// Makes the quantifier greedy (match as many as possible).
    /// </summary>
    public Pattern<T> Greedy()
    {
        _quantifier = _quantifier.AsGreedy();
        return this;
    }

    /// <summary>
    /// Specifies that matching events must be consecutive (no non-matching events between).
    /// </summary>
    public Pattern<T> Consecutive()
    {
        _quantifier = _quantifier.AsConsecutive();
        return this;
    }

    /// <summary>
    /// Adds a next pattern that must follow immediately (strict contiguity).
    /// </summary>
    public Pattern<T> Next(string name)
    {
        return new Pattern<T>(name, this, PatternType.Strict);
    }

    /// <summary>
    /// Adds a next pattern that must follow but with relaxed contiguity.
    /// Other events may appear in between.
    /// </summary>
    public Pattern<T> FollowedBy(string name)
    {
        return new Pattern<T>(name, this, PatternType.Relaxed);
    }

    /// <summary>
    /// Adds a next pattern with non-deterministic relaxed contiguity.
    /// </summary>
    public Pattern<T> FollowedByAny(string name)
    {
        return new Pattern<T>(name, this, PatternType.NonDeterministicRelaxed);
    }

    /// <summary>
    /// Adds a NOT pattern (the named event must not occur).
    /// </summary>
    public Pattern<T> NotNext(string name)
    {
        return new Pattern<T>(name, this, PatternType.NotStrict);
    }

    /// <summary>
    /// Adds a NOT pattern with relaxed contiguity.
    /// </summary>
    public Pattern<T> NotFollowedBy(string name)
    {
        return new Pattern<T>(name, this, PatternType.NotRelaxed);
    }

    /// <summary>
    /// Sets a time constraint for the entire pattern.
    /// </summary>
    public Pattern<T> Within(TimeSpan time)
    {
        _within = time;
        return this;
    }

    /// <summary>
    /// Sets the after-match skip strategy.
    /// </summary>
    public Pattern<T> AfterMatchSkip(AfterMatchSkipStrategy strategy)
    {
        _skipStrategy = strategy;
        return this;
    }

    // Internal accessors
    internal string Name => _name;
    internal Pattern<T>? Previous => _previous;
    internal PatternType Type => _patternType;
    internal Func<T, bool>? Condition => _condition;
    internal Func<T, T, bool>? IterativeCondition => _iterativeCondition;
    internal Quantifier PatternQuantifier => _quantifier;
    internal TimeSpan? WithinTime => _within;
    internal AfterMatchSkipStrategy SkipStrategy => _skipStrategy;

    /// <summary>
    /// Gets the complete pattern chain from start to end.
    /// </summary>
    internal IReadOnlyList<Pattern<T>> GetPatternSequence()
    {
        var patterns = new List<Pattern<T>>();
        var current = this;
        while (current != null)
        {
            patterns.Insert(0, current);
            current = current._previous;
        }
        return patterns;
    }
}

/// <summary>
/// Type of pattern transition.
/// </summary>
public enum PatternType
{
    Start,
    Strict,
    Relaxed,
    NonDeterministicRelaxed,
    NotStrict,
    NotRelaxed
}

/// <summary>
/// Quantifier for pattern matching.
/// </summary>
public readonly struct Quantifier
{
    public static readonly Quantifier One = new(1, 1);
    public static readonly Quantifier Optional = new(0, 1);
    public static readonly Quantifier OneOrMore = new(1, int.MaxValue);
    public static readonly Quantifier ZeroOrMore = new(0, int.MaxValue);

    public int Min { get; }
    public int Max { get; }
    public bool IsGreedy { get; }
    public bool IsConsecutive { get; }

    public Quantifier(int min, int max, bool greedy = false, bool consecutive = false)
    {
        Min = min;
        Max = max;
        IsGreedy = greedy;
        IsConsecutive = consecutive;
    }

    public Quantifier AsGreedy() => new(Min, Max, true, IsConsecutive);
    public Quantifier AsConsecutive() => new(Min, Max, IsGreedy, true);
}

/// <summary>
/// Strategy for skipping events after a match.
/// </summary>
public enum AfterMatchSkipStrategy
{
    NoSkip,
    SkipToNext,
    SkipPastLastEvent,
    SkipToFirst,
    SkipToLast
}
