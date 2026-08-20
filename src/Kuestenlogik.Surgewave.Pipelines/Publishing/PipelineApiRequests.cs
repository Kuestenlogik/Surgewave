using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Pipelines.Publishing;

/// <summary>Body of <c>POST /api/pipelines</c>, mirroring the broker's CreatePipelineRequest.</summary>
internal sealed record CreatePipelineRequestBody
{
    public required string Name { get; init; }

    public string? Description { get; init; }

    public required List<PipelineNode> Nodes { get; init; }

    public required List<PipelineConnection> Connections { get; init; }

    public Dictionary<string, string>? Parameters { get; init; }
}

/// <summary>Body of <c>PUT /api/pipelines/{id}</c>, mirroring the broker's UpdatePipelineRequest.</summary>
internal sealed record UpdatePipelineRequestBody
{
    public string? Name { get; init; }

    public string? Description { get; init; }

    public List<PipelineNode>? Nodes { get; init; }

    public List<PipelineConnection>? Connections { get; init; }

    public Dictionary<string, string>? Parameters { get; init; }
}
