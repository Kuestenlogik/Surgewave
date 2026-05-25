namespace Kuestenlogik.Surgewave.Control.Components.Pipeline;

/// <summary>
/// Represents an alignment guide line for visual feedback during node movement.
/// </summary>
public readonly struct AlignmentGuide
{
    /// <summary>
    /// X coordinate for vertical guides.
    /// </summary>
    public double X { get; init; }

    /// <summary>
    /// Y coordinate for horizontal guides.
    /// </summary>
    public double Y { get; init; }

    /// <summary>
    /// The node ID that this guide aligns with.
    /// </summary>
    public string? AlignedNodeId { get; init; }

    /// <summary>
    /// Creates a horizontal alignment guide at the specified Y position.
    /// </summary>
    public static AlignmentGuide Horizontal(double y, string? alignedNodeId = null)
        => new() { Y = y, AlignedNodeId = alignedNodeId };

    /// <summary>
    /// Creates a vertical alignment guide at the specified X position.
    /// </summary>
    public static AlignmentGuide Vertical(double x, string? alignedNodeId = null)
        => new() { X = x, AlignedNodeId = alignedNodeId };
}
