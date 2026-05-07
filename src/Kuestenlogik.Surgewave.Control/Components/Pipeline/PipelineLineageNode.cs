using Blazor.Diagrams.Core.Geometry;
using Blazor.Diagrams.Core.Models;
using Kuestenlogik.Surgewave.Control.Models.Pipeline;

namespace Kuestenlogik.Surgewave.Control.Components.Pipeline;

/// <summary>
/// Diagram node representing a pipeline in the cross-pipeline lineage view.
/// </summary>
public sealed class PipelineLineageNode : NodeModel
{
    public PipelineLineageNode(Point position) : base(position) { }

    public required string PipelineId { get; init; }
    public required string Name { get; set; }
    public PipelineStatus Status { get; set; }
    public int NodeCount { get; set; }
    public List<string> SourceTopics { get; set; } = [];
    public List<string> SinkTopics { get; set; } = [];
}
