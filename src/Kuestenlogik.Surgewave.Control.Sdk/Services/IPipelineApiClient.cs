using Kuestenlogik.Surgewave.Control.Models.Pipeline;

namespace Kuestenlogik.Surgewave.Control.Services;

public interface IPipelineApiClient
{
    Task<IReadOnlyList<PipelineDefinition>> ListPipelinesAsync(CancellationToken cancellationToken = default);
    Task<PipelineDefinition?> GetPipelineAsync(string id, CancellationToken cancellationToken = default);
    Task<PipelineDefinition> CreatePipelineAsync(string name, string? description, List<PipelineNode> nodes, List<PipelineConnection> connections, CancellationToken cancellationToken = default);
    Task<PipelineDefinition> UpdatePipelineAsync(string id, string name, string? description, List<PipelineNode> nodes, List<PipelineConnection> connections, CancellationToken cancellationToken = default);
    Task DeletePipelineAsync(string id, CancellationToken cancellationToken = default);
    Task StartPipelineAsync(string id, CancellationToken cancellationToken = default);
    Task StopPipelineAsync(string id, CancellationToken cancellationToken = default);
    Task<PipelineRuntimeStatus?> GetPipelineStatusAsync(string id, CancellationToken cancellationToken = default);

    // Import/Export
    Task<PipelineExportFormat> ExportPipelineAsync(string id, CancellationToken cancellationToken = default);
    Task<PipelineDefinition> ImportPipelineAsync(PipelineExportFormat export, string? nameOverride = null, CancellationToken cancellationToken = default);

    // Templates
    Task<IReadOnlyList<PipelineTemplateSummary>> ListTemplatesAsync(CancellationToken cancellationToken = default);
    Task<PipelineTemplate?> GetTemplateAsync(string templateId, CancellationToken cancellationToken = default);
    Task<PipelineDefinition> CreateFromTemplateAsync(string templateId, string? name = null, CancellationToken cancellationToken = default);

    // Execution history
    Task<ExecutionListResponse> ListExecutionsAsync(string pipelineId, int limit = 50, int offset = 0, CancellationToken cancellationToken = default);
    Task<ExecutionRecord?> GetExecutionAsync(string pipelineId, string executionId, CancellationToken cancellationToken = default);

    // Metrics
    Task<PipelineMetricsResponse?> GetPipelineMetricsAsync(string pipelineId, CancellationToken cancellationToken = default);

    // Start with parameters
    Task StartPipelineWithParametersAsync(string id, Dictionary<string, string>? parameters = null, CancellationToken cancellationToken = default);

    // Debug
    Task SetBreakpointsAsync(string pipelineId, string[] nodeIds, CancellationToken cancellationToken = default);
    Task RemoveBreakpointAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default);
    Task<DebugState?> GetDebugStateAsync(string pipelineId, CancellationToken cancellationToken = default);
    Task StepNodeAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default);
    Task ResumeNodeAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default);
    Task ResumeAllAsync(string pipelineId, CancellationToken cancellationToken = default);

    // Schedule
    Task<ScheduleConfig?> GetScheduleAsync(string pipelineId, CancellationToken cancellationToken = default);
    Task<ScheduleConfig> UpdateScheduleAsync(string pipelineId, ScheduleConfig schedule, CancellationToken cancellationToken = default);

    // Hot-deploy
    Task<HotDeployAnalysis?> AnalyzeChangesAsync(string pipelineId, UpdatePipelineRequest proposed, CancellationToken cancellationToken = default);
    Task<bool> HotDeployAsync(string pipelineId, UpdatePipelineRequest proposed, CancellationToken cancellationToken = default);

    // Provenance
    Task EnableProvenanceAsync(string pipelineId, CancellationToken cancellationToken = default);
    Task DisableProvenanceAsync(string pipelineId, CancellationToken cancellationToken = default);

    // Sub-pipeline ports
    Task<SubPipelinePortInfo?> GetPipelinePortsAsync(string pipelineId, CancellationToken cancellationToken = default);

    // Versioning
    Task<PipelineVersionListResponse> ListVersionsAsync(string pipelineId, int limit = 50, int offset = 0, CancellationToken cancellationToken = default);
    Task<PipelineVersion?> GetVersionAsync(string pipelineId, string versionId, CancellationToken cancellationToken = default);
    Task<PipelineVersionDiff> CompareVersionsAsync(string pipelineId, string fromVersionId, string toVersionId, CancellationToken cancellationToken = default);
    Task<PipelineDefinition> RestoreVersionAsync(string pipelineId, string versionId, CancellationToken cancellationToken = default);
    Task<PipelineValidationResultDto?> ValidatePipelineAsync(string id, CancellationToken ct = default);
}

public record PipelineRuntimeStatus
{
    public required string PipelineId { get; init; }
    public PipelineStatus Status { get; init; }
    public required List<NodeStatus> Nodes { get; init; }
}

public record PipelineValidationResultDto
{
    public bool IsValid { get; init; }
    public List<PipelineValidationIssueDto> Issues { get; init; } = [];
}

public record PipelineValidationIssueDto
{
    public string NodeId { get; init; } = "";
    public string NodeLabel { get; init; } = "";
    public string Severity { get; init; } = "Info";
    public string Code { get; init; } = "";
    public string Message { get; init; } = "";
}

public record NodeStatus
{
    public required string NodeId { get; init; }
    public required string ConnectorName { get; init; }
    public required string State { get; init; }
    public int TaskCount { get; init; }
    public string? Error { get; init; }
}
