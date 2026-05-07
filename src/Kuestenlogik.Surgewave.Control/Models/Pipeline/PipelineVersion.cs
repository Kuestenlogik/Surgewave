namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// Represents a version of a pipeline definition.
/// </summary>
public record PipelineVersion
{
    /// <summary>
    /// Unique identifier for this version.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// The pipeline this version belongs to.
    /// </summary>
    public required string PipelineId { get; init; }

    /// <summary>
    /// Sequential version number.
    /// </summary>
    public int VersionNumber { get; init; }

    /// <summary>
    /// The complete pipeline definition at this version.
    /// </summary>
    public required PipelineDefinition Definition { get; init; }

    /// <summary>
    /// Brief summary of changes in this version.
    /// </summary>
    public string? ChangeSummary { get; init; }

    /// <summary>
    /// User who created this version.
    /// </summary>
    public string? ChangedBy { get; init; }

    /// <summary>
    /// When this version was created.
    /// </summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    /// Whether this is the current active version.
    /// </summary>
    public bool IsCurrent { get; init; }
}

/// <summary>
/// Summary of a pipeline version for listing.
/// </summary>
public record PipelineVersionSummary
{
    public required string Id { get; init; }
    public required string PipelineId { get; init; }
    public int VersionNumber { get; init; }
    public string? ChangeSummary { get; init; }
    public string? ChangedBy { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public bool IsCurrent { get; init; }
    public int NodeCount { get; init; }
    public int ConnectionCount { get; init; }
}

/// <summary>
/// Response containing version list.
/// </summary>
public record PipelineVersionListResponse
{
    public List<PipelineVersionSummary> Versions { get; init; } = [];
    public int TotalCount { get; init; }
}

/// <summary>
/// Diff between two pipeline versions.
/// </summary>
public record PipelineVersionDiff
{
    public required string FromVersionId { get; init; }
    public required string ToVersionId { get; init; }
    public int FromVersionNumber { get; init; }
    public int ToVersionNumber { get; init; }
    public List<NodeDiff> NodeChanges { get; init; } = [];
    public List<ConnectionDiff> ConnectionChanges { get; init; } = [];
}

/// <summary>
/// Change to a node between versions.
/// </summary>
public record NodeDiff
{
    public required string NodeId { get; init; }
    public required DiffType Type { get; init; }
    public PipelineNode? OldNode { get; init; }
    public PipelineNode? NewNode { get; init; }
    public List<string> ChangedProperties { get; init; } = [];
}

/// <summary>
/// Change to a connection between versions.
/// </summary>
public record ConnectionDiff
{
    public required string ConnectionId { get; init; }
    public required DiffType Type { get; init; }
    public PipelineConnection? OldConnection { get; init; }
    public PipelineConnection? NewConnection { get; init; }
}

/// <summary>
/// Type of change in a diff.
/// </summary>
public enum DiffType
{
    Added,
    Removed,
    Modified
}
