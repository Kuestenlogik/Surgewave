namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Input data for a specific node in a dry run.
/// </summary>
public record DryRunInput
{
    public required string NodeId { get; init; }
    public required List<DryRunRecord> Records { get; init; }
}

/// <summary>
/// A simplified record for dry-run input/output.
/// </summary>
public record DryRunRecord
{
    public string? Key { get; init; }
    public string? Value { get; init; }
    public Dictionary<string, string>? Headers { get; init; }
}

/// <summary>
/// Result of a pipeline dry run.
/// </summary>
public record DryRunResult
{
    public bool Success { get; init; }
    public required Dictionary<string, DryRunNodeTrace> NodeTraces { get; init; }
    public string? Error { get; init; }
}

/// <summary>
/// Trace of a single node's execution during a dry run.
/// </summary>
public record DryRunNodeTrace
{
    public required string NodeId { get; init; }
    public required string ConnectorType { get; init; }
    public int InputCount { get; init; }
    public int OutputCount { get; init; }
    public required List<DryRunRecord> Outputs { get; init; }
    public required List<string> Errors { get; init; }
}

/// <summary>
/// Request body for dry-run endpoint.
/// </summary>
public record DryRunRequest
{
    public List<DryRunInput>? Inputs { get; init; }
}
