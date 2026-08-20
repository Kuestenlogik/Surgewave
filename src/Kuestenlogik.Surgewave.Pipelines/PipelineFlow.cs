using System.Globalization;
using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// A linear chain of pipeline stages under construction. Each stage appends a processor node
/// and wires it to the previous one; <see cref="To"/> routes the chain's output to a real
/// topic and ends it. The first processor consumes the source topics directly — no
/// intermediate hop is inserted at the entry.
/// </summary>
public class PipelineFlow
{
    private string[]? _pendingSourceTopics;
    private string? _lastNodeId;
    private bool _terminated;

    internal PipelineFlow(PipelineBuilder builder)
    {
        Builder = builder;
    }

    /// <summary>The underlying graph builder, for mixing fluent stages with explicit graph edits.</summary>
    public PipelineBuilder Builder { get; }

    internal static PipelineFlow FromTopics(PipelineBuilder builder, string[] topics)
    {
        if (topics is null || topics.Length == 0 || topics.Any(string.IsNullOrWhiteSpace))
        {
            throw new PipelineBuildException("FromTopic needs at least one non-empty topic name.");
        }

        return new PipelineFlow(builder) { _pendingSourceTopics = topics };
    }

    internal static PipelineFlow FromSourceNode(
        PipelineBuilder builder,
        string connectorType,
        IReadOnlyDictionary<string, string> config,
        string outputTopicKey)
    {
        var flow = new PipelineFlow(builder);
        var draft = builder.AddDraft(connectorType, config, outputTopicKey: outputTopicKey);
        flow._lastNodeId = draft.Id;
        return flow;
    }

    /// <summary>Sets the pipeline name.</summary>
    public PipelineFlow Named(string name)
    {
        Builder.Named(name);
        return this;
    }

    /// <summary>Sets the pipeline description.</summary>
    public PipelineFlow DescribedAs(string description)
    {
        Builder.DescribedAs(description);
        return this;
    }

    /// <summary>Adds a user-defined parameter (see <see cref="PipelineBuilder.WithParameter"/>).</summary>
    public PipelineFlow WithParameter(string key, string value)
    {
        Builder.WithParameter(key, value);
        return this;
    }

    /// <summary>Schedules the pipeline (see <see cref="PipelineBuilder.WithSchedule"/>).</summary>
    public PipelineFlow WithSchedule(string cronExpression, string timezone = "UTC", int? maxRunDurationMinutes = null)
    {
        Builder.WithSchedule(cronExpression, timezone, maxRunDurationMinutes);
        return this;
    }

    /// <summary>
    /// Keeps only records matching <paramref name="condition"/> — a raw broker condition string
    /// in <c>$.path OP value</c> syntax (for example <c>$.status == 'active'</c>).
    /// </summary>
    public PipelineFlow Filter(string condition, bool negate = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(condition);
        var config = new Dictionary<string, string> { ["condition"] = condition };
        if (negate)
        {
            config["negate"] = "true";
        }

        AddStage(ConnectNodeTypes.Filter, config);
        return this;
    }

    /// <summary>Builds a new record value from field mappings.</summary>
    public PipelineFlow Map(Action<MapBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var mapBuilder = new MapBuilder();
        configure(mapBuilder);
        AddStage(ConnectNodeTypes.Map, mapBuilder.BuildConfig());
        return this;
    }

    /// <summary>Replaces the record value with the field at <paramref name="fieldPath"/>.</summary>
    public PipelineFlow ExtractField(string fieldPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);
        AddStage(ConnectNodeTypes.ExtractField, new Dictionary<string, string> { ["extract.field"] = fieldPath });
        return this;
    }

    /// <summary>Flattens nested objects into delimited top-level fields.</summary>
    public PipelineFlow Flatten(string delimiter = ".")
    {
        AddStage(ConnectNodeTypes.Flatten, new Dictionary<string, string> { ["flatten.delimiter"] = delimiter });
        return this;
    }

    /// <summary>
    /// Casts fields to declared types using the Connect cast spec syntax,
    /// for example <c>age:int32,price:float64,active:boolean</c>.
    /// </summary>
    public PipelineFlow Cast(string castSpec)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(castSpec);
        AddStage(ConnectNodeTypes.Cast, new Dictionary<string, string> { ["cast.spec"] = castSpec });
        return this;
    }

    /// <summary>Masks the given fields, replacing their values.</summary>
    public PipelineFlow MaskFields(string replacement = "***", params string[] fields)
    {
        if (fields is null || fields.Length == 0)
        {
            throw new PipelineBuildException("MaskFields needs at least one field name.");
        }

        AddStage(ConnectNodeTypes.MaskField, new Dictionary<string, string>
        {
            ["mask.fields"] = string.Join(',', fields),
            ["mask.replacement"] = replacement,
        });
        return this;
    }

    /// <summary>Splits the array at <paramref name="arrayPath"/> into individual records.</summary>
    public PipelineFlow Split(string arrayPath = "$", bool keepParent = true)
    {
        AddStage(ConnectNodeTypes.Split, new Dictionary<string, string>
        {
            ["array.path"] = arrayPath,
            ["keep.parent"] = keepParent ? "true" : "false",
        });
        return this;
    }

    /// <summary>Copies the given value fields into the record key.</summary>
    public PipelineFlow ValueToKey(params string[] fields)
    {
        if (fields is null || fields.Length == 0)
        {
            throw new PipelineBuildException("ValueToKey needs at least one field name.");
        }

        AddStage(ConnectNodeTypes.ValueToKey, new Dictionary<string, string> { ["fields"] = string.Join(',', fields) });
        return this;
    }

    /// <summary>Drops duplicates sharing the value at <paramref name="keyPath"/> within <paramref name="window"/>.</summary>
    public PipelineFlow Deduplicate(
        string keyPath,
        TimeSpan? window = null,
        DeduplicationStrategy strategy = DeduplicationStrategy.First)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyPath);
        AddStage(ConnectNodeTypes.Deduplicate, new Dictionary<string, string>
        {
            ["dedup.key"] = keyPath,
            ["dedup.window.ms"] = ((long)(window ?? TimeSpan.FromMinutes(5)).TotalMilliseconds).ToString(CultureInfo.InvariantCulture),
            ["dedup.strategy"] = strategy == DeduplicationStrategy.Last ? "last" : "first",
        });
        return this;
    }

    /// <summary>Limits throughput to <paramref name="limit"/> records per <paramref name="interval"/>.</summary>
    public PipelineFlow RateLimit(int limit, TimeSpan? interval = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        AddStage(ConnectNodeTypes.RateLimiter, new Dictionary<string, string>
        {
            ["rate.limit"] = limit.ToString(CultureInfo.InvariantCulture),
            ["rate.interval.ms"] = ((long)(interval ?? TimeSpan.FromSeconds(1)).TotalMilliseconds).ToString(CultureInfo.InvariantCulture),
        });
        return this;
    }

    /// <summary>
    /// Adds any pipeline node by its connector type name (its .NET <c>Type.FullName</c>) —
    /// the escape hatch for connectors without a dedicated DSL stage, including plugin nodes.
    /// </summary>
    public PipelineFlow Through(string connectorType, Action<NodeConfigBuilder>? configure = null, string? label = null)
    {
        var configBuilder = new NodeConfigBuilder();
        configure?.Invoke(configBuilder);
        AddStage(connectorType, configBuilder.Build(), label);
        return this;
    }

    /// <summary>Attaches a retry policy to the most recently added node.</summary>
    public PipelineFlow WithRetry(
        int maxRetries = 3,
        TimeSpan? backoff = null,
        double backoffMultiplier = 2.0,
        TimeSpan? maxBackoff = null)
    {
        LastDraft("WithRetry").RetryPolicy = new RetryPolicy(
            maxRetries,
            (long)(backoff ?? TimeSpan.FromSeconds(1)).TotalMilliseconds,
            backoffMultiplier,
            (long)(maxBackoff ?? TimeSpan.FromSeconds(30)).TotalMilliseconds);
        return this;
    }

    /// <summary>Sets the editor display label of the most recently added node.</summary>
    public PipelineFlow WithLabel(string label)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(label);
        LastDraft("WithLabel").Label = label;
        return this;
    }

    /// <summary>
    /// Routes records the most recently added node explicitly reports as failed (via its
    /// error output) to <paramref name="deadLetterTopic"/> through a dead-letter sink node.
    /// Note that most built-in transform nodes drop unparseable records silently rather than
    /// reporting them; nodes with explicit error semantics (Retry, State, WaitForInput,
    /// RateLimiter with overflow=error) use this path.
    /// </summary>
    public PipelineFlow OnError(string deadLetterTopic, bool includeStackTrace = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(deadLetterTopic);
        var source = LastDraft("OnError");
        var dlq = Builder.AddDraft(ConnectNodeTypes.DlqSink, new Dictionary<string, string>
        {
            ["output.topic"] = deadLetterTopic,
            ["include.stack.trace"] = includeStackTrace ? "true" : "false",
        });
        Builder.Connect(source.Id, dlq.Id, error: true);
        return this;
    }

    /// <summary>
    /// Ends the chain by routing records to <paramref name="trueTopic"/> when
    /// <paramref name="condition"/> (broker condition syntax) holds, else to <paramref name="falseTopic"/>.
    /// </summary>
    public PipelineBuilder RouteIf(string condition, string trueTopic, string falseTopic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(condition);
        ArgumentException.ThrowIfNullOrWhiteSpace(trueTopic);
        ArgumentException.ThrowIfNullOrWhiteSpace(falseTopic);

        AddStage(ConnectNodeTypes.If, new Dictionary<string, string>
        {
            ["condition"] = condition,
            ["output.true.topic"] = trueTopic,
            ["output.false.topic"] = falseTopic,
        });
        _terminated = true;
        return Builder;
    }

    /// <summary>
    /// Ends the chain by routing records to per-value topics based on the discriminator field at
    /// <paramref name="discriminatorPath"/> (for example <c>$.type</c>).
    /// </summary>
    public PipelineBuilder RouteBy(
        string discriminatorPath,
        IReadOnlyDictionary<string, string> caseTopics,
        string? defaultTopic = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(discriminatorPath);
        ArgumentNullException.ThrowIfNull(caseTopics);
        if (caseTopics.Count == 0)
        {
            throw new PipelineBuildException("RouteBy needs at least one case.");
        }

        var config = new Dictionary<string, string> { ["discriminator"] = discriminatorPath };
        foreach (var (value, topic) in caseTopics)
        {
            config["case." + value] = topic;
        }

        if (defaultTopic is not null)
        {
            config["default.topic"] = defaultTopic;
        }

        AddStage(ConnectNodeTypes.Switch, config);
        _terminated = true;
        return Builder;
    }

    /// <summary>Ends the chain by routing its output to <paramref name="topic"/>.</summary>
    public PipelineBuilder To(string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ThrowIfTerminated();

        NodeDraft draft;
        if (_lastNodeId is null)
        {
            // Bare From(...).To(...): a single pass-through node bridges the topics.
            draft = AddStage(ConnectNodeTypes.TopicTrigger, null);
        }
        else
        {
            draft = LastDraft("To");
        }

        if (draft.Config.ContainsKey(draft.OutputTopicKey))
        {
            throw new PipelineBuildException(
                $"Node '{draft.Id}' already routes its output ('{draft.OutputTopicKey}' is set).");
        }

        draft.Config[draft.OutputTopicKey] = topic;
        _terminated = true;
        return Builder;
    }

    /// <summary>Builds the pipeline (shortcut for <c>Builder.Build()</c>).</summary>
    public BuiltPipeline Build()
    {
        return Builder.Build();
    }

    internal NodeDraft AddStage(string connectorType, IReadOnlyDictionary<string, string>? config, string? label = null)
    {
        ThrowIfTerminated();

        var draft = Builder.AddDraft(connectorType, config, label);

        if (_pendingSourceTopics is not null)
        {
            draft.Config["topics"] = string.Join(',', _pendingSourceTopics);
            _pendingSourceTopics = null;
        }
        else if (_lastNodeId is not null)
        {
            Builder.Connect(_lastNodeId, draft.Id);
        }

        _lastNodeId = draft.Id;
        return draft;
    }

    private NodeDraft LastDraft(string operation)
    {
        if (_lastNodeId is null)
        {
            throw new PipelineBuildException($"{operation} needs a preceding stage in the flow.");
        }

        return Builder.GetDraft(_lastNodeId);
    }

    private void ThrowIfTerminated()
    {
        if (_terminated)
        {
            throw new PipelineBuildException("This flow already ended in To/RouteIf/RouteBy — no further stages can be added.");
        }
    }
}
