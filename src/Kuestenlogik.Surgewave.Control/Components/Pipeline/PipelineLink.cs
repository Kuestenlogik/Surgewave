using Blazor.Diagrams.Core.Anchors;
using Blazor.Diagrams.Core.Models;
using Blazor.Diagrams.Core.Models.Base;

namespace Kuestenlogik.Surgewave.Control.Components.Pipeline;

/// <summary>
/// Custom link model for connections between pipeline nodes.
/// </summary>
public sealed class PipelineLink : LinkModel
{
    public PipelineLink(PortModel sourcePort, PortModel? targetPort = null)
        : base(sourcePort, targetPort!)
    {
    }

    public PipelineLink(Anchor source, Anchor target) : base(source, target)
    {
    }

    public required string ConnectionId { get; init; }

    /// <summary>
    /// The internal topic used for this connection.
    /// </summary>
    public string? InternalTopic { get; set; }

    /// <summary>
    /// Number of messages flowing through this connection (for monitoring).
    /// </summary>
    public long MessageCount { get; set; }

    /// <summary>
    /// Current throughput in messages per second.
    /// </summary>
    public double Throughput { get; set; }

    /// <summary>
    /// Whether this connection is currently active.
    /// </summary>
    public bool IsActive { get; set; }

    /// <summary>
    /// Queue depth for back-pressure visualization (Feature 2).
    /// </summary>
    public long QueueDepth { get; set; }

    /// <summary>
    /// Back-pressure level computed from queue depth (Feature 2).
    /// </summary>
    public Models.Pipeline.BackPressureLevel BackPressureLevel { get; set; }

    /// <summary>
    /// Whether this is an error-routing link (Feature 5).
    /// </summary>
    public bool IsErrorLink { get; set; }

    /// <summary>
    /// Whether this link is highlighted for lineage visualization.
    /// </summary>
    public bool IsLineageHighlighted { get; set; }
}
