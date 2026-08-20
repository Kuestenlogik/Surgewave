using Kuestenlogik.Surgewave.Connect.Pipelines;
using Kuestenlogik.Surgewave.Pipelines.Serialization;

namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// An immutable, broker-independent pipeline definition produced by
/// <see cref="PipelineBuilder.Build"/>. Export it as editor-compatible JSON
/// (<see cref="ToJson"/>, <see cref="Save"/>) or publish it directly with
/// <c>PipelinePublisher</c> / <c>surgewave pipelines deploy</c>.
/// </summary>
public sealed record BuiltPipeline
{
    /// <summary>Pipeline name; may be null until set, but required for export and publish.</summary>
    public string? Name { get; init; }

    /// <summary>Optional description.</summary>
    public string? Description { get; init; }

    /// <summary>The connector nodes, with editor layout positions assigned.</summary>
    public required IReadOnlyList<PipelineNode> Nodes { get; init; }

    /// <summary>The connections defining data flow between nodes.</summary>
    public required IReadOnlyList<PipelineConnection> Connections { get; init; }

    /// <summary>User-defined parameters referenced via <c>${param.key}</c> in node configs.</summary>
    public IReadOnlyDictionary<string, string>? Parameters { get; init; }

    /// <summary>Optional periodic execution schedule.</summary>
    public ScheduleConfig? Schedule { get; init; }

    /// <summary>
    /// Converts to the portable pipeline export format understood by
    /// <c>POST /api/pipelines/import</c> and the Control UI's import dialog.
    /// The export is deterministic (fixed timestamp) so saved files diff cleanly in git.
    /// </summary>
    public PipelineExportFormat ToExport()
    {
        var name = RequireName();

        return new PipelineExportFormat
        {
            Version = "1.0",
            ExportedAt = DateTimeOffset.UnixEpoch,
            SurgewaveVersion = typeof(BuiltPipeline).Assembly.GetName().Version?.ToString(),
            Pipeline = new PipelineExportData
            {
                Name = name,
                Description = Description,
                Nodes = Nodes.Select(n => new PipelineNodeExport
                {
                    NodeId = n.Id,
                    ConnectorType = n.ConnectorType,
                    Config = new Dictionary<string, string>(n.Config),
                    X = n.X,
                    Y = n.Y,
                    Label = n.Label,
                    RetryPolicy = n.RetryPolicy,
                }).ToList(),
                Connections = Connections.Select(c => new PipelineConnectionExport
                {
                    SourceNodeId = c.SourceNodeId,
                    TargetNodeId = c.TargetNodeId,
                    Type = c.Type,
                }).ToList(),
                Parameters = Parameters is null ? null : new Dictionary<string, string>(Parameters),
                Schedule = Schedule,
            },
        };
    }

    /// <summary>Serializes the pipeline as indented, camelCase export JSON.</summary>
    public string ToJson()
    {
        return PipelineJson.Write(ToExport());
    }

    /// <summary>Writes the export JSON to <paramref name="path"/>.</summary>
    public void Save(string path)
    {
        File.WriteAllText(path, ToJson());
    }

    /// <summary>Writes the export JSON to <paramref name="path"/>.</summary>
    public Task SaveAsync(string path, CancellationToken cancellationToken = default)
    {
        return File.WriteAllTextAsync(path, ToJson(), cancellationToken);
    }

    internal string RequireName()
    {
        return Name ?? throw new PipelineBuildException(
            "The pipeline has no name. Set one with .Named(\"...\") or Pipeline.Create(\"...\").");
    }
}
