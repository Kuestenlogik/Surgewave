using System.Net.Http.Json;
using Kuestenlogik.Surgewave.Control.Models.Pipeline;

namespace Kuestenlogik.Surgewave.Control.Services;

public sealed class PipelineApiClient : IPipelineApiClient
{
    private readonly HttpClient _httpClient;

    public PipelineApiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task<IReadOnlyList<PipelineDefinition>> ListPipelinesAsync(CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetFromJsonAsync<List<PipelineDefinition>>("/api/pipelines", cancellationToken);
        return response ?? [];
    }

    public async Task<PipelineDefinition?> GetPipelineAsync(string id, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<PipelineDefinition>($"/api/pipelines/{id}", cancellationToken);
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<PipelineDefinition> CreatePipelineAsync(
        string name,
        string? description,
        List<PipelineNode> nodes,
        List<PipelineConnection> connections,
        CancellationToken cancellationToken = default)
    {
        var request = new CreatePipelineRequest
        {
            Name = name,
            Description = description,
            Nodes = nodes,
            Connections = connections
        };

        var response = await _httpClient.PostAsJsonAsync("/api/pipelines", request, cancellationToken);
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadFromJsonAsync<PipelineDefinition>(cancellationToken)
            ?? throw new InvalidOperationException("Failed to deserialize pipeline response");
    }

    public async Task<PipelineDefinition> UpdatePipelineAsync(
        string id,
        string name,
        string? description,
        List<PipelineNode> nodes,
        List<PipelineConnection> connections,
        CancellationToken cancellationToken = default)
    {
        var request = new UpdatePipelineRequest
        {
            Name = name,
            Description = description,
            Nodes = nodes,
            Connections = connections
        };

        var response = await _httpClient.PutAsJsonAsync($"/api/pipelines/{id}", request, cancellationToken);
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadFromJsonAsync<PipelineDefinition>(cancellationToken)
            ?? throw new InvalidOperationException("Failed to deserialize pipeline response");
    }

    public async Task DeletePipelineAsync(string id, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.DeleteAsync($"/api/pipelines/{id}", cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task StartPipelineAsync(string id, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsync($"/api/pipelines/{id}/start", null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task StopPipelineAsync(string id, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsync($"/api/pipelines/{id}/stop", null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task<PipelineRuntimeStatus?> GetPipelineStatusAsync(string id, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<PipelineRuntimeStatus>($"/api/pipelines/{id}/status", cancellationToken);
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<PipelineExportFormat> ExportPipelineAsync(string id, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetFromJsonAsync<PipelineExportFormat>($"/api/pipelines/{id}/export", cancellationToken);
        return response ?? throw new InvalidOperationException("Failed to export pipeline");
    }

    public async Task<PipelineDefinition> ImportPipelineAsync(PipelineExportFormat export, string? nameOverride = null, CancellationToken cancellationToken = default)
    {
        var request = new ImportPipelineRequest
        {
            Export = export,
            NameOverride = nameOverride
        };

        var response = await _httpClient.PostAsJsonAsync("/api/pipelines/import", request, cancellationToken);
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadFromJsonAsync<PipelineDefinition>(cancellationToken)
            ?? throw new InvalidOperationException("Failed to deserialize pipeline response");
    }

    public async Task<IReadOnlyList<PipelineTemplateSummary>> ListTemplatesAsync(CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetFromJsonAsync<List<PipelineTemplateSummary>>("/api/pipelines/templates", cancellationToken);
        return response ?? [];
    }

    public async Task<PipelineTemplate?> GetTemplateAsync(string templateId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<PipelineTemplate>($"/api/pipelines/templates/{templateId}", cancellationToken);
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<PipelineDefinition> CreateFromTemplateAsync(string templateId, string? name = null, CancellationToken cancellationToken = default)
    {
        var request = name != null ? new { Name = name } : null;
        var response = await _httpClient.PostAsJsonAsync($"/api/pipelines/templates/{templateId}/create", request, cancellationToken);
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadFromJsonAsync<PipelineDefinition>(cancellationToken)
            ?? throw new InvalidOperationException("Failed to deserialize pipeline response");
    }

    public async Task<ExecutionListResponse> ListExecutionsAsync(string pipelineId, int limit = 50, int offset = 0, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetFromJsonAsync<ExecutionListResponse>(
            $"/api/pipelines/{pipelineId}/executions?limit={limit}&offset={offset}", cancellationToken);
        return response ?? new ExecutionListResponse();
    }

    public async Task<ExecutionRecord?> GetExecutionAsync(string pipelineId, string executionId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<ExecutionRecord>(
                $"/api/pipelines/{pipelineId}/executions/{executionId}", cancellationToken);
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<PipelineMetricsResponse?> GetPipelineMetricsAsync(string pipelineId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<PipelineMetricsResponse>($"/api/pipelines/{pipelineId}/metrics", cancellationToken);
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task StartPipelineWithParametersAsync(string id, Dictionary<string, string>? parameters = null, CancellationToken cancellationToken = default)
    {
        var response = parameters != null && parameters.Count > 0
            ? await _httpClient.PostAsJsonAsync($"/api/pipelines/{id}/start", new { Parameters = parameters }, cancellationToken)
            : await _httpClient.PostAsync($"/api/pipelines/{id}/start", null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task SetBreakpointsAsync(string pipelineId, string[] nodeIds, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync($"/api/pipelines/{pipelineId}/debug/breakpoints", new { NodeIds = nodeIds }, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task RemoveBreakpointAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.DeleteAsync($"/api/pipelines/{pipelineId}/debug/breakpoints/{nodeId}", cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task<DebugState?> GetDebugStateAsync(string pipelineId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<DebugState>($"/api/pipelines/{pipelineId}/debug/state", cancellationToken);
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task StepNodeAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsync($"/api/pipelines/{pipelineId}/debug/step/{nodeId}", null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task ResumeNodeAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsync($"/api/pipelines/{pipelineId}/debug/resume/{nodeId}", null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task ResumeAllAsync(string pipelineId, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsync($"/api/pipelines/{pipelineId}/debug/resume", null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task<ScheduleConfig?> GetScheduleAsync(string pipelineId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<ScheduleConfig>($"/api/pipelines/{pipelineId}/schedule", cancellationToken);
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<ScheduleConfig> UpdateScheduleAsync(string pipelineId, ScheduleConfig schedule, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PutAsJsonAsync($"/api/pipelines/{pipelineId}/schedule", schedule, cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<ScheduleConfig>(cancellationToken)
            ?? throw new InvalidOperationException("Failed to update schedule");
    }

    public async Task<HotDeployAnalysis?> AnalyzeChangesAsync(string pipelineId, UpdatePipelineRequest proposed, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync($"/api/pipelines/{pipelineId}/analyze-changes", proposed, cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<HotDeployAnalysis>(cancellationToken);
    }

    public async Task<bool> HotDeployAsync(string pipelineId, UpdatePipelineRequest proposed, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync($"/api/pipelines/{pipelineId}/hot-deploy", proposed, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task EnableProvenanceAsync(string pipelineId, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsync($"/api/pipelines/{pipelineId}/provenance/enable", null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task DisableProvenanceAsync(string pipelineId, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsync($"/api/pipelines/{pipelineId}/provenance/disable", null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task<SubPipelinePortInfo?> GetPipelinePortsAsync(string pipelineId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<SubPipelinePortInfo>($"/api/pipelines/{pipelineId}/ports", cancellationToken);
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<PipelineVersionListResponse> ListVersionsAsync(string pipelineId, int limit = 50, int offset = 0, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetFromJsonAsync<PipelineVersionListResponse>(
            $"/api/pipelines/{pipelineId}/versions?limit={limit}&offset={offset}", cancellationToken);
        return response ?? new PipelineVersionListResponse();
    }

    public async Task<PipelineVersion?> GetVersionAsync(string pipelineId, string versionId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<PipelineVersion>(
                $"/api/pipelines/{pipelineId}/versions/{versionId}", cancellationToken);
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<PipelineVersionDiff> CompareVersionsAsync(string pipelineId, string fromVersionId, string toVersionId, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetFromJsonAsync<PipelineVersionDiff>(
            $"/api/pipelines/{pipelineId}/versions/compare?from={fromVersionId}&to={toVersionId}", cancellationToken);
        return response ?? throw new InvalidOperationException("Failed to compare versions");
    }

    public async Task<PipelineDefinition> RestoreVersionAsync(string pipelineId, string versionId, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsync($"/api/pipelines/{pipelineId}/versions/{versionId}/restore", null, cancellationToken);
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadFromJsonAsync<PipelineDefinition>(cancellationToken)
            ?? throw new InvalidOperationException("Failed to restore version");
    }

    public async Task<PipelineValidationResultDto?> ValidatePipelineAsync(string id, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.PostAsync($"/api/pipelines/{id}/validate", null, ct);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadFromJsonAsync<PipelineValidationResultDto>(ct);
        }
        catch
        {
            return null;
        }
    }

    private record CreatePipelineRequest
    {
        public string? Name { get; init; }
        public string? Description { get; init; }
        public List<PipelineNode>? Nodes { get; init; }
        public List<PipelineConnection>? Connections { get; init; }
    }

}
