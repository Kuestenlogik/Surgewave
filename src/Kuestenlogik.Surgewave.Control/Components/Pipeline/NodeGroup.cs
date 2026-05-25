using Blazor.Diagrams.Core.Geometry;
using Blazor.Diagrams.Core.Models;

namespace Kuestenlogik.Surgewave.Control.Components.Pipeline;

/// <summary>
/// Custom group model for grouping connector nodes in the pipeline diagram.
/// </summary>
public sealed class NodeGroup : GroupModel
{
    public NodeGroup(IEnumerable<NodeModel>? children = null, byte padding = 30)
        : base(children ?? [], padding)
    {
        GroupId = Guid.NewGuid().ToString("N");
    }

    /// <summary>
    /// Unique identifier for the group.
    /// </summary>
    public string GroupId { get; init; }

    /// <summary>
    /// Display name for the group.
    /// </summary>
    public string GroupName { get; set; } = "Group";

    /// <summary>
    /// Whether the group is collapsed (showing only a summary).
    /// </summary>
    public bool IsCollapsed { get; set; }

    /// <summary>
    /// Color theme for the group border.
    /// </summary>
    public string Color { get; set; } = "primary";
}
