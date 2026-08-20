using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// A pipeline node under construction. Mutable while the builder assembles the graph;
/// frozen into a <see cref="PipelineNode"/> by <see cref="PipelineBuilder.Build"/>.
/// </summary>
internal sealed class NodeDraft
{
    public required string Id { get; init; }

    public required string ConnectorType { get; init; }

    public Dictionary<string, string> Config { get; } = [];

    public string? Label { get; set; }

    public RetryPolicy? RetryPolicy { get; set; }

    /// <summary>
    /// The config key this node emits its output through — <c>output.topic</c> for processor
    /// nodes, <c>topic</c> for source connectors like the schedule/webhook triggers.
    /// </summary>
    public string OutputTopicKey { get; init; } = "output.topic";

    /// <summary>
    /// Explicit editor position; when unset, <see cref="PipelineLayout"/> assigns one.
    /// </summary>
    public (double X, double Y)? Position { get; set; }
}
