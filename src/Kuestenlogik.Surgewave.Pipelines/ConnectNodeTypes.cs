namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// Fully qualified connector type names of the native Connect pipeline nodes.
/// A node's identity on the wire is its .NET <c>Type.FullName</c>; these constants let the DSL
/// (and user code using the generic <c>Through</c> escape hatch) reference native nodes without
/// referencing the Connect assembly itself.
/// </summary>
public static class ConnectNodeTypes
{
    /// <summary>Reads records from Surgewave topics as a pipeline entry point.</summary>
    public const string TopicTrigger = "Kuestenlogik.Surgewave.Connect.Nodes.Trigger.TopicTrigger";

    /// <summary>Emits records on a cron schedule.</summary>
    public const string ScheduleTrigger = "Kuestenlogik.Surgewave.Connect.Nodes.Trigger.ScheduleTrigger";

    /// <summary>Accepts HTTP POST/PUT requests as pipeline input.</summary>
    public const string WebhookTrigger = "Kuestenlogik.Surgewave.Connect.Nodes.Trigger.WebhookTrigger";

    /// <summary>Passes records matching a condition, drops the rest.</summary>
    public const string Filter = "Kuestenlogik.Surgewave.Connect.Nodes.Transform.FilterNode";

    /// <summary>Builds a new record value from field mappings.</summary>
    public const string Map = "Kuestenlogik.Surgewave.Connect.Nodes.Transform.MapNode";

    /// <summary>Replaces the record value with a single extracted field.</summary>
    public const string ExtractField = "Kuestenlogik.Surgewave.Connect.Nodes.Transform.ExtractFieldNode";

    /// <summary>Flattens nested objects into delimited top-level fields.</summary>
    public const string Flatten = "Kuestenlogik.Surgewave.Connect.Nodes.Transform.FlattenNode";

    /// <summary>Casts fields to declared types.</summary>
    public const string Cast = "Kuestenlogik.Surgewave.Connect.Nodes.Transform.CastNode";

    /// <summary>Masks sensitive fields.</summary>
    public const string MaskField = "Kuestenlogik.Surgewave.Connect.Nodes.Transform.MaskFieldNode";

    /// <summary>Includes, excludes, and renames fields.</summary>
    public const string ReplaceField = "Kuestenlogik.Surgewave.Connect.Nodes.Transform.ReplaceFieldNode";

    /// <summary>Splits an array field into individual records.</summary>
    public const string Split = "Kuestenlogik.Surgewave.Connect.Nodes.Transform.SplitNode";

    /// <summary>Copies value fields into the record key.</summary>
    public const string ValueToKey = "Kuestenlogik.Surgewave.Connect.Nodes.Transform.ValueToKeyNode";

    /// <summary>Routes records to a true/false topic based on a condition.</summary>
    public const string If = "Kuestenlogik.Surgewave.Connect.Nodes.Logic.IfNode";

    /// <summary>Routes records to per-value topics based on a discriminator field.</summary>
    public const string Switch = "Kuestenlogik.Surgewave.Connect.Nodes.Logic.SwitchNode";

    /// <summary>Drops duplicate records within a time window.</summary>
    public const string Deduplicate = "Kuestenlogik.Surgewave.Connect.Nodes.Logic.DeduplicateNode";

    /// <summary>Limits record throughput.</summary>
    public const string RateLimiter = "Kuestenlogik.Surgewave.Connect.Nodes.Logic.RateLimiterNode";

    /// <summary>Merges multiple input topics into one stream.</summary>
    public const string Merge = "Kuestenlogik.Surgewave.Connect.Nodes.Logic.MergeNode";

    /// <summary>Collects failed records into a dead-letter topic.</summary>
    public const string DlqSink = "Kuestenlogik.Surgewave.Connect.Nodes.Logic.DlqSinkNode";
}
