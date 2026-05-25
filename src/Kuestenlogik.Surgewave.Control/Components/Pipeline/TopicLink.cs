using Blazor.Diagrams.Core.Anchors;
using Blazor.Diagrams.Core.Models;

namespace Kuestenlogik.Surgewave.Control.Components.Pipeline;

/// <summary>
/// Custom link model representing a topic-based connection between pipelines.
/// </summary>
public sealed class TopicLink : LinkModel
{
    public TopicLink(PortModel sourcePort, PortModel targetPort)
        : base(sourcePort, targetPort)
    {
    }

    public TopicLink(Anchor source, Anchor target) : base(source, target)
    {
    }

    public required string TopicName { get; init; }
    public bool IsActive { get; set; }
}
