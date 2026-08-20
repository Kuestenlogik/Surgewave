using System.Linq.Expressions;
using Kuestenlogik.Surgewave.Pipelines.Expressions;

namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// A typed pipeline flow over payloads of <typeparamref name="T"/> — predicates and field
/// selectors are C# lambdas, translated at build time into the broker's condition syntax and
/// JSON paths. The payload type never has to be deployed anywhere; it only shapes the
/// generated configuration.
/// </summary>
public sealed class PipelineFlow<T>
{
    private readonly PipelineFlow _flow;

    internal PipelineFlow(PipelineFlow flow)
    {
        _flow = flow;
    }

    /// <summary>The untyped flow, for stages that take raw paths or condition strings.</summary>
    public PipelineFlow Untyped => _flow;

    /// <summary>The underlying graph builder.</summary>
    public PipelineBuilder Builder => _flow.Builder;

    /// <summary>Sets the pipeline name.</summary>
    public PipelineFlow<T> Named(string name)
    {
        _flow.Named(name);
        return this;
    }

    /// <summary>Sets the pipeline description.</summary>
    public PipelineFlow<T> DescribedAs(string description)
    {
        _flow.DescribedAs(description);
        return this;
    }

    /// <summary>Adds a user-defined parameter (see <see cref="PipelineBuilder.WithParameter"/>).</summary>
    public PipelineFlow<T> WithParameter(string key, string value)
    {
        _flow.WithParameter(key, value);
        return this;
    }

    /// <summary>Schedules the pipeline (see <see cref="PipelineBuilder.WithSchedule"/>).</summary>
    public PipelineFlow<T> WithSchedule(string cronExpression, string timezone = "UTC", int? maxRunDurationMinutes = null)
    {
        _flow.WithSchedule(cronExpression, timezone, maxRunDurationMinutes);
        return this;
    }

    /// <summary>
    /// Keeps only records matching <paramref name="predicate"/>. Supported: comparisons of a
    /// payload property against a constant, string Contains/StartsWith/EndsWith, bare boolean
    /// properties, <c>!</c>, and <c>&amp;&amp;</c> (which becomes chained filter nodes).
    /// </summary>
    public PipelineFlow<T> Filter(Expression<Func<T, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        foreach (var condition in ConditionTranslator.Translate(predicate, Builder.NamingPolicy))
        {
            _flow.Filter(condition.Condition, condition.Negate);
        }

        return this;
    }

    /// <summary>Keeps only records matching a raw broker condition string.</summary>
    public PipelineFlow<T> Filter(string condition, bool negate = false)
    {
        _flow.Filter(condition, negate);
        return this;
    }

    /// <summary>Builds a new record value from typed field mappings, keeping the payload type.</summary>
    public PipelineFlow<T> Map(Action<MapBuilder<T>> configure)
    {
        return Map<T>(configure);
    }

    /// <summary>
    /// Builds a new record value from typed field mappings; downstream stages see the payload
    /// as <typeparamref name="TOut"/>.
    /// </summary>
    public PipelineFlow<TOut> Map<TOut>(Action<MapBuilder<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var mapBuilder = new MapBuilder<T>(Builder.NamingPolicy);
        configure(mapBuilder);
        _flow.AddStage(ConnectNodeTypes.Map, mapBuilder.BuildConfig());
        return new PipelineFlow<TOut>(_flow);
    }

    /// <summary>
    /// Replaces the record value with the selected field; downstream stages see the payload
    /// as <typeparamref name="TOut"/>.
    /// </summary>
    public PipelineFlow<TOut> ExtractField<TOut>(Expression<Func<T, TOut>> field)
    {
        ArgumentNullException.ThrowIfNull(field);
        _flow.ExtractField(JsonMemberPath.Build(field.Body, Builder.NamingPolicy));
        return new PipelineFlow<TOut>(_flow);
    }

    /// <summary>Copies the selected value fields into the record key.</summary>
    public PipelineFlow<T> ValueToKey(params Expression<Func<T, object?>>[] fields)
    {
        if (fields is null || fields.Length == 0)
        {
            throw new PipelineBuildException("ValueToKey needs at least one field selector.");
        }

        // ValueToKeyNode addresses top-level fields by name, not by JSON path.
        var names = fields
            .Select(f => JsonMemberPath.Build(f.Body, Builder.NamingPolicy).TrimStart('$', '.'))
            .ToArray();
        _flow.ValueToKey(names);
        return this;
    }

    /// <summary>Drops duplicates sharing the selected key within <paramref name="window"/>.</summary>
    public PipelineFlow<T> Deduplicate(
        Expression<Func<T, object?>> key,
        TimeSpan? window = null,
        DeduplicationStrategy strategy = DeduplicationStrategy.First)
    {
        ArgumentNullException.ThrowIfNull(key);
        _flow.Deduplicate(JsonMemberPath.Build(key.Body, Builder.NamingPolicy), window, strategy);
        return this;
    }

    /// <summary>Flattens nested objects into delimited top-level fields.</summary>
    public PipelineFlow<T> Flatten(string delimiter = ".")
    {
        _flow.Flatten(delimiter);
        return this;
    }

    /// <summary>Masks the given fields, replacing their values.</summary>
    public PipelineFlow<T> MaskFields(string replacement = "***", params string[] fields)
    {
        _flow.MaskFields(replacement, fields);
        return this;
    }

    /// <summary>Splits the array at <paramref name="arrayPath"/> into individual records.</summary>
    public PipelineFlow<T> Split(string arrayPath = "$", bool keepParent = true)
    {
        _flow.Split(arrayPath, keepParent);
        return this;
    }

    /// <summary>Limits throughput to <paramref name="limit"/> records per <paramref name="interval"/>.</summary>
    public PipelineFlow<T> RateLimit(int limit, TimeSpan? interval = null)
    {
        _flow.RateLimit(limit, interval);
        return this;
    }

    /// <summary>Adds any pipeline node by connector type name (escape hatch, keeps the payload type).</summary>
    public PipelineFlow<T> Through(string connectorType, Action<NodeConfigBuilder>? configure = null, string? label = null)
    {
        _flow.Through(connectorType, configure, label);
        return this;
    }

    /// <summary>Re-types the flow after a stage that reshaped the payload.</summary>
    public PipelineFlow<TOut> As<TOut>()
    {
        return new PipelineFlow<TOut>(_flow);
    }

    /// <summary>Attaches a retry policy to the most recently added node.</summary>
    public PipelineFlow<T> WithRetry(
        int maxRetries = 3,
        TimeSpan? backoff = null,
        double backoffMultiplier = 2.0,
        TimeSpan? maxBackoff = null)
    {
        _flow.WithRetry(maxRetries, backoff, backoffMultiplier, maxBackoff);
        return this;
    }

    /// <summary>Sets the editor display label of the most recently added node.</summary>
    public PipelineFlow<T> WithLabel(string label)
    {
        _flow.WithLabel(label);
        return this;
    }

    /// <summary>
    /// Routes records the most recently added node explicitly reports as failed to a
    /// dead-letter topic (see <see cref="PipelineFlow.OnError"/> for which nodes report).
    /// </summary>
    public PipelineFlow<T> OnError(string deadLetterTopic, bool includeStackTrace = false)
    {
        _flow.OnError(deadLetterTopic, includeStackTrace);
        return this;
    }

    /// <summary>
    /// Ends the chain by routing records to <paramref name="trueTopic"/> when
    /// <paramref name="predicate"/> holds, else to <paramref name="falseTopic"/>.
    /// The predicate must fit a single comparison (no <c>&amp;&amp;</c>).
    /// </summary>
    public PipelineBuilder RouteIf(Expression<Func<T, bool>> predicate, string trueTopic, string falseTopic)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        var condition = ConditionTranslator.TranslateSingle(predicate, Builder.NamingPolicy);

        // The If node has no negate switch — a negated predicate swaps the branch topics.
        return condition.Negate
            ? _flow.RouteIf(condition.Condition, falseTopic, trueTopic)
            : _flow.RouteIf(condition.Condition, trueTopic, falseTopic);
    }

    /// <summary>
    /// Ends the chain by routing records to per-value topics based on the selected
    /// discriminator field.
    /// </summary>
    public PipelineBuilder RouteBy(
        Expression<Func<T, object?>> discriminator,
        IReadOnlyDictionary<string, string> caseTopics,
        string? defaultTopic = null)
    {
        ArgumentNullException.ThrowIfNull(discriminator);
        return _flow.RouteBy(JsonMemberPath.Build(discriminator.Body, Builder.NamingPolicy), caseTopics, defaultTopic);
    }

    /// <summary>Ends the chain by routing its output to <paramref name="topic"/>.</summary>
    public PipelineBuilder To(string topic)
    {
        return _flow.To(topic);
    }

    /// <summary>
    /// Ends the chain by routing its output to <paramref name="topic"/>; the type parameter
    /// documents the payload shape written to the target topic.
    /// </summary>
    public PipelineBuilder To<TOut>(string topic)
    {
        return _flow.To(topic);
    }

    /// <summary>Builds the pipeline (shortcut for <c>Builder.Build()</c>).</summary>
    public BuiltPipeline Build()
    {
        return _flow.Build();
    }
}
