using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// Assembles a pipeline definition as a graph of connector nodes. The fluent flow API
/// (<see cref="FromTopic(string[])"/> and friends) covers linear chains; nodes with custom
/// topology can be added directly via <see cref="AddNode"/> and <see cref="Connect"/>.
/// Building requires no running broker — the result is a plain, serializable definition.
/// </summary>
public sealed class PipelineBuilder
{
    private readonly List<NodeDraft> _nodes = [];
    private readonly List<(string SourceId, string TargetId, bool Error)> _connections = [];
    private readonly Dictionary<string, int> _idCounters = new(StringComparer.Ordinal);

    private string? _name;
    private string? _description;
    private Dictionary<string, string>? _parameters;
    private ScheduleConfig? _schedule;

    /// <summary>Creates an unnamed builder; a name must be set before exporting.</summary>
    public PipelineBuilder()
    {
    }

    /// <summary>Creates a builder for a pipeline with the given name.</summary>
    public PipelineBuilder(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        _name = name;
    }

    /// <summary>
    /// Naming policy applied when translating C# property chains to JSON paths.
    /// Defaults to camelCase, matching <c>JsonSerializerDefaults.Web</c>.
    /// </summary>
    internal JsonNamingPolicy? NamingPolicy { get; private set; } = JsonNamingPolicy.CamelCase;

    /// <summary>Sets the pipeline name (used as the identity for replace-by-name deployments).</summary>
    public PipelineBuilder Named(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        _name = name;
        return this;
    }

    /// <summary>Sets the pipeline description.</summary>
    public PipelineBuilder DescribedAs(string description)
    {
        ArgumentNullException.ThrowIfNull(description);
        _description = description;
        return this;
    }

    /// <summary>
    /// Adds a user-defined parameter, referencable from node configs as <c>${param.key}</c>
    /// (or <c>${key}</c>) and overridable per start request.
    /// </summary>
    public PipelineBuilder WithParameter(string key, string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);
        (_parameters ??= [])[key] = value;
        return this;
    }

    /// <summary>Schedules the pipeline to run periodically (5-field cron expression).</summary>
    public PipelineBuilder WithSchedule(string cronExpression, string timezone = "UTC", int? maxRunDurationMinutes = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cronExpression);
        _schedule = new ScheduleConfig
        {
            CronExpression = cronExpression,
            Timezone = timezone,
            Enabled = true,
            MaxRunDurationMinutes = maxRunDurationMinutes,
        };
        return this;
    }

    /// <summary>
    /// Sets the naming policy used to translate C# property names into JSON paths for
    /// filter conditions and field mappings. Pass null when payload JSON uses the C#
    /// property names verbatim (PascalCase).
    /// </summary>
    public PipelineBuilder WithNamingPolicy(JsonNamingPolicy? namingPolicy)
    {
        NamingPolicy = namingPolicy;
        return this;
    }

    /// <summary>Starts a flow reading from one or more existing topics.</summary>
    public PipelineFlow FromTopic(params string[] topics)
    {
        return PipelineFlow.FromTopics(this, topics);
    }

    /// <summary>Starts a typed flow reading payloads of <typeparamref name="T"/> from one or more topics.</summary>
    public PipelineFlow<T> FromTopic<T>(params string[] topics)
    {
        return new PipelineFlow<T>(PipelineFlow.FromTopics(this, topics));
    }

    /// <summary>Starts a flow triggered by a cron schedule (5-field expression).</summary>
    public PipelineFlow FromSchedule(string cronExpression, string? payload = null, string timezone = "UTC")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cronExpression);
        var config = new Dictionary<string, string> { ["cron"] = cronExpression, ["timezone"] = timezone };
        if (payload is not null)
        {
            config["payload"] = payload;
        }

        return PipelineFlow.FromSourceNode(this, ConnectNodeTypes.ScheduleTrigger, config, outputTopicKey: "topic");
    }

    /// <summary>Starts a flow fed by an HTTP webhook endpoint.</summary>
    public PipelineFlow FromWebhook(int port = 8888, string path = "/webhook", string? requiredAuthHeader = null)
    {
        var config = new Dictionary<string, string>
        {
            ["port"] = port.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["path"] = path,
        };
        if (requiredAuthHeader is not null)
        {
            config["require.auth.header"] = requiredAuthHeader;
        }

        return PipelineFlow.FromSourceNode(this, ConnectNodeTypes.WebhookTrigger, config, outputTopicKey: "topic");
    }

    /// <summary>
    /// Adds a node with an explicit connector type (its .NET <c>Type.FullName</c>) and config.
    /// Returns the generated node id for use with <see cref="Connect"/>.
    /// </summary>
    public string AddNode(
        string connectorType,
        IReadOnlyDictionary<string, string>? config = null,
        string? label = null)
    {
        return AddDraft(connectorType, config, label).Id;
    }

    /// <summary>Connects two nodes; error connections route a node's failures instead of its output.</summary>
    public PipelineBuilder Connect(string sourceNodeId, string targetNodeId, bool error = false)
    {
        if (_nodes.All(n => n.Id != sourceNodeId))
        {
            throw new PipelineBuildException($"Cannot connect: node '{sourceNodeId}' does not exist.");
        }

        if (_nodes.All(n => n.Id != targetNodeId))
        {
            throw new PipelineBuildException($"Cannot connect: node '{targetNodeId}' does not exist.");
        }

        if (sourceNodeId == targetNodeId)
        {
            throw new PipelineBuildException($"Cannot connect node '{sourceNodeId}' to itself.");
        }

        if (_connections.Contains((sourceNodeId, targetNodeId, error)))
        {
            throw new PipelineBuildException($"Nodes '{sourceNodeId}' and '{targetNodeId}' are already connected.");
        }

        _connections.Add((sourceNodeId, targetNodeId, error));
        return this;
    }

    /// <summary>
    /// Freezes the graph into an immutable, serializable pipeline. Validates connections and
    /// rejects cycles; assigns editor layout positions to nodes without explicit ones.
    /// </summary>
    public BuiltPipeline Build()
    {
        if (_nodes.Count == 0)
        {
            throw new PipelineBuildException("The pipeline has no nodes. Start a flow with FromTopic/FromSchedule/FromWebhook or add nodes explicitly.");
        }

        ThrowOnCycle();

        var positions = PipelineLayout.Assign(_nodes, _connections);

        var nodes = _nodes.Select(draft => new PipelineNode
        {
            Id = draft.Id,
            ConnectorType = draft.ConnectorType,
            Config = new Dictionary<string, string>(draft.Config),
            X = (draft.Position ?? positions[draft.Id]).X,
            Y = (draft.Position ?? positions[draft.Id]).Y,
            Label = draft.Label,
            RetryPolicy = draft.RetryPolicy,
        }).ToList();

        var connections = _connections.Select((c, index) => new PipelineConnection
        {
            Id = $"c{index + 1}",
            SourceNodeId = c.SourceId,
            TargetNodeId = c.TargetId,
            Type = c.Error ? PipelineConnectionType.Error : PipelineConnectionType.Normal,
        }).ToList();

        return new BuiltPipeline
        {
            Name = _name,
            Description = _description,
            Nodes = nodes,
            Connections = connections,
            Parameters = _parameters is null ? null : new Dictionary<string, string>(_parameters),
            Schedule = _schedule,
        };
    }

    internal NodeDraft AddDraft(
        string connectorType,
        IReadOnlyDictionary<string, string>? config = null,
        string? label = null,
        string outputTopicKey = "output.topic")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectorType);

        var draft = new NodeDraft
        {
            Id = NextId(connectorType),
            ConnectorType = connectorType,
            Label = label,
            OutputTopicKey = outputTopicKey,
        };

        if (config is not null)
        {
            foreach (var (key, value) in config)
            {
                draft.Config[key] = value;
            }
        }

        _nodes.Add(draft);
        return draft;
    }

    internal NodeDraft GetDraft(string nodeId)
    {
        return _nodes.First(n => n.Id == nodeId);
    }

    private string NextId(string connectorType)
    {
        var slug = IdSlug(connectorType);
        var next = _idCounters.GetValueOrDefault(slug) + 1;
        _idCounters[slug] = next;
        return $"{slug}-{next}";
    }

    private static string IdSlug(string connectorType)
    {
        var simpleName = connectorType[(connectorType.LastIndexOf('.') + 1)..];
        if (simpleName.EndsWith("Node", StringComparison.Ordinal))
        {
            simpleName = simpleName[..^4];
        }
        else if (simpleName.EndsWith("Connector", StringComparison.Ordinal))
        {
            simpleName = simpleName[..^9];
        }

        var slug = new System.Text.StringBuilder(simpleName.Length + 4);
        foreach (var c in simpleName)
        {
            if (char.IsUpper(c))
            {
                if (slug.Length > 0)
                {
                    slug.Append('-');
                }

                slug.Append(char.ToLowerInvariant(c));
            }
            else
            {
                slug.Append(c);
            }
        }

        return slug.Length > 0 ? slug.ToString() : "node";
    }

    private void ThrowOnCycle()
    {
        var outgoing = _connections
            .ToLookup(c => c.SourceId, c => c.TargetId);

        var states = new Dictionary<string, bool>(); // false = in progress, true = done

        foreach (var node in _nodes)
        {
            Visit(node.Id);
        }

        void Visit(string id)
        {
            if (states.TryGetValue(id, out var done))
            {
                if (!done)
                {
                    throw new PipelineBuildException(
                        $"The pipeline contains a cycle involving node '{id}'. " +
                        "Loops need explicit topics (see the LoopNode) rather than direct connections.");
                }

                return;
            }

            states[id] = false;
            foreach (var target in outgoing[id])
            {
                Visit(target);
            }

            states[id] = true;
        }
    }
}
