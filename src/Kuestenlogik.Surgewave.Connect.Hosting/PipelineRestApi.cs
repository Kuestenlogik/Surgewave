using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Holder for the pipeline orchestrator instance.
/// Used when the orchestrator is created manually outside of DI.
/// </summary>
public static class PipelineOrchestratorHolder
{
    public static PipelineOrchestrator? Instance { get; set; }
}

/// <summary>
/// Holder for the execution store instance.
/// </summary>
public static class ExecutionStoreHolder
{
    public static ExecutionStore? Instance { get; set; }
}

/// <summary>
/// Holder for the execution logger instance.
/// </summary>
public static class ExecutionLoggerHolder
{
    public static ExecutionLogger? Instance { get; set; }
}

/// <summary>
/// Holder for the pipeline debugger instance.
/// </summary>
public static class PipelineDebuggerHolder
{
    public static PipelineDebugger? Instance { get; set; }
}

/// <summary>
/// REST API for managing pipelines.
/// </summary>
public static class PipelineRestApi
{
    /// <summary>
    /// Adds pipeline services to the service collection.
    /// </summary>
    public static IServiceCollection AddSurgewavePipelines(this IServiceCollection services)
    {
        services.AddSingleton<PipelineStore>();
        services.AddSingleton<PipelineTopicManager>();
        // PipelineOrchestrator is created manually and set via PipelineOrchestratorHolder
        return services;
    }

    /// <summary>
    /// Maps pipeline REST API endpoints.
    /// </summary>
    public static IEndpointRouteBuilder MapSurgewavePipelines(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/pipelines")
            .WithTags("Pipelines");

        // List all pipelines
        group.MapGet("", ListPipelines)
            .WithName("ListPipelines")
            .WithSummary("List all pipelines")
            .Produces<IReadOnlyList<PipelineDefinition>>();

        // Get a pipeline
        group.MapGet("/{id}", GetPipeline)
            .WithName("GetPipeline")
            .WithSummary("Get a pipeline by ID")
            .Produces<PipelineDefinition>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Create a pipeline
        group.MapPost("", CreatePipeline)
            .WithName("CreatePipeline")
            .WithSummary("Create a new pipeline")
            .Produces<PipelineDefinition>(StatusCodes.Status201Created)
            .ProducesValidationProblem();

        // Update a pipeline
        group.MapPut("/{id}", UpdatePipeline)
            .WithName("UpdatePipeline")
            .WithSummary("Update a pipeline")
            .Produces<PipelineDefinition>()
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status409Conflict);

        // Delete a pipeline
        group.MapDelete("/{id}", DeletePipeline)
            .WithName("DeletePipeline")
            .WithSummary("Delete a pipeline")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Start a pipeline
        group.MapPost("/{id}/start", StartPipeline)
            .WithName("StartPipeline")
            .WithSummary("Start a pipeline")
            .Produces(StatusCodes.Status202Accepted)
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Stop a pipeline
        group.MapPost("/{id}/stop", StopPipeline)
            .WithName("StopPipeline")
            .WithSummary("Stop a pipeline")
            .Produces(StatusCodes.Status202Accepted)
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Get pipeline status
        group.MapGet("/{id}/status", GetPipelineStatus)
            .WithName("GetPipelineStatus")
            .WithSummary("Get pipeline runtime status")
            .Produces<PipelineRuntimeStatus>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Pipeline versioning endpoints
        group.MapGet("/{id}/versions", GetPipelineVersions)
            .WithName("GetPipelineVersions")
            .WithSummary("List pipeline version history")
            .Produces<List<PipelineVersionEntry>>();

        group.MapGet("/{id}/versions/{version:int}", GetPipelineVersion)
            .WithName("GetPipelineVersion")
            .WithSummary("Get a specific pipeline version")
            .Produces<PipelineVersionEntry>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/{id}/versions/{from:int}/diff/{to:int}", GetPipelineVersionDiff)
            .WithName("GetPipelineVersionDiff")
            .WithSummary("Get diff between two pipeline versions")
            .Produces<PipelineVersionDiff>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/{id}/rollback/{version:int}", RollbackPipeline)
            .WithName("RollbackPipeline")
            .WithSummary("Rollback a pipeline to a previous version")
            .Produces<PipelineDefinition>()
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status409Conflict);

        // Validate pipeline placement and plugin availability
        group.MapPost("/{id}/validate", ValidatePipeline)
            .WithName("ValidatePipeline")
            .WithSummary("Validate pipeline placement, plugin availability, and worker capabilities")
            .Produces<PipelineValidationResult>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Dry-run a pipeline
        group.MapPost("/{id}/dry-run", DryRunPipeline)
            .WithName("DryRunPipeline")
            .WithSummary("Run a pipeline with sample data in preview mode")
            .Produces<DryRunResult>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Get pipeline metrics
        group.MapGet("/{id}/metrics", GetPipelineMetrics)
            .WithName("GetPipelineMetrics")
            .WithSummary("Get pipeline metrics and throughput")
            .Produces<PipelineMetrics>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Export a pipeline
        group.MapGet("/{id}/export", ExportPipeline)
            .WithName("ExportPipeline")
            .WithSummary("Export a pipeline as portable JSON")
            .Produces<PipelineExportFormat>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Import a pipeline
        group.MapPost("/import", ImportPipeline)
            .WithName("ImportPipeline")
            .WithSummary("Import a pipeline from export JSON")
            .Produces<PipelineDefinition>(StatusCodes.Status201Created)
            .ProducesValidationProblem();

        // Get pipeline templates
        group.MapGet("/templates", ListTemplates)
            .WithName("ListPipelineTemplates")
            .WithSummary("List available pipeline templates")
            .Produces<IReadOnlyList<PipelineTemplateSummary>>();

        // Get a specific template
        group.MapGet("/templates/{templateId}", GetTemplate)
            .WithName("GetPipelineTemplate")
            .WithSummary("Get a pipeline template by ID")
            .Produces<PipelineTemplate>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Create pipeline from template
        group.MapPost("/templates/{templateId}/create", CreateFromTemplate)
            .WithName("CreatePipelineFromTemplate")
            .WithSummary("Create a new pipeline from a template")
            .Produces<PipelineDefinition>(StatusCodes.Status201Created)
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Execution history endpoints
        group.MapGet("/{id}/executions", ListExecutions)
            .WithName("ListPipelineExecutions")
            .WithSummary("List execution history for a pipeline")
            .Produces<ExecutionListResponse>();

        group.MapGet("/{id}/executions/{executionId}", GetExecution)
            .WithName("GetPipelineExecution")
            .WithSummary("Get execution details")
            .Produces<ExecutionRecord>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/{id}/executions/stats", GetExecutionStats)
            .WithName("GetPipelineExecutionStats")
            .WithSummary("Get real-time execution stats for running pipelines")
            .Produces<ExecutionStats>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Debug endpoints
        group.MapPost("/{id}/debug/breakpoints", SetBreakpoints)
            .WithName("SetBreakpoints")
            .WithSummary("Set breakpoints on pipeline nodes")
            .Produces(StatusCodes.Status200OK);

        group.MapDelete("/{id}/debug/breakpoints/{nodeId}", RemoveBreakpoint)
            .WithName("RemoveBreakpoint")
            .WithSummary("Remove a breakpoint from a pipeline node")
            .Produces(StatusCodes.Status200OK);

        group.MapGet("/{id}/debug/state", GetDebugState)
            .WithName("GetDebugState")
            .WithSummary("Get debug state for a pipeline")
            .Produces<DebugState>();

        group.MapPost("/{id}/debug/step/{nodeId}", DebugStep)
            .WithName("DebugStep")
            .WithSummary("Step to next record on a paused node")
            .Produces(StatusCodes.Status200OK);

        group.MapPost("/{id}/debug/resume/{nodeId}", DebugResumeNode)
            .WithName("DebugResumeNode")
            .WithSummary("Resume a paused node")
            .Produces(StatusCodes.Status200OK);

        group.MapPost("/{id}/debug/resume", DebugResumeAll)
            .WithName("DebugResumeAll")
            .WithSummary("Resume all paused nodes in a pipeline")
            .Produces(StatusCodes.Status200OK);

        // Schedule endpoints
        group.MapGet("/{id}/schedule", GetSchedule)
            .WithName("GetPipelineSchedule")
            .WithSummary("Get pipeline schedule configuration")
            .Produces<ScheduleConfig>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPut("/{id}/schedule", UpdateSchedule)
            .WithName("UpdatePipelineSchedule")
            .WithSummary("Update pipeline schedule configuration")
            .Produces<ScheduleConfig>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Hot-deploy endpoints
        group.MapPost("/{id}/analyze-changes", AnalyzeChanges)
            .WithName("AnalyzePipelineChanges")
            .WithSummary("Analyze if changes can be hot-deployed")
            .Produces<HotDeployAnalysis>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/{id}/hot-deploy", HotDeploy)
            .WithName("HotDeployPipeline")
            .WithSummary("Hot-deploy config changes to a running pipeline")
            .Produces(StatusCodes.Status200OK)
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status409Conflict);

        // Provenance endpoints
        group.MapPost("/{id}/provenance/enable", EnableProvenance)
            .WithName("EnableProvenance")
            .WithSummary("Enable record provenance tracking")
            .Produces(StatusCodes.Status200OK);

        group.MapPost("/{id}/provenance/disable", DisableProvenance)
            .WithName("DisableProvenance")
            .WithSummary("Disable record provenance tracking")
            .Produces(StatusCodes.Status200OK);

        // Port info endpoint for sub-pipelines
        group.MapGet("/{id}/ports", GetPipelinePorts)
            .WithName("GetPipelinePorts")
            .WithSummary("Get source/sink nodes as port info for sub-pipeline mapping")
            .Produces<SubPipelinePortInfo>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Connector plugins endpoints
        var connectorGroup = app.MapGroup("/api/connectors")
            .WithTags("Connectors");

        connectorGroup.MapGet("", ListConnectorTypes)
            .WithName("ListConnectorTypes")
            .WithSummary("List available connector types")
            .Produces<IReadOnlyList<ConnectorTypeInfo>>();

        connectorGroup.MapGet("/{type}/config", GetConnectorConfigSchema)
            .WithName("GetConnectorConfigSchema")
            .WithSummary("Get connector configuration schema")
            .Produces<ConnectorConfigSchema>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Debug endpoint to show registered assemblies and type loading
        connectorGroup.MapGet("/_debug", (PluginDiscovery discovery) =>
        {
            var plugins = discovery.GetAllPlugins();
            var results = new List<object>();

            foreach (var plugin in plugins)
            {
                var loadedType = discovery.LoadPluginType(plugin.Class);
                var attrs = loadedType?.GetCustomAttributes(false);
                var attrNames = attrs?.Select(a => a.GetType().FullName).ToList();

                results.Add(new
                {
                    plugin.Class,
                    plugin.Type,
                    LoadSuccess = loadedType != null,
                    LoadedTypeName = loadedType?.FullName,
                    AttributeCount = attrs?.Length ?? 0,
                    AttributeTypes = attrNames
                });
            }

            return Results.Ok(new { PluginCount = plugins.Count, Types = results });
        });

        return app;
    }

    private static IResult ListPipelines()
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        return Results.Ok(orchestrator.GetAll());
    }

    private static IResult GetPipeline(string id)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        var pipeline = orchestrator.Get(id);
        if (pipeline == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Pipeline '{id}' not found" });
        }

        return Results.Ok(pipeline);
    }

    private static async Task<IResult> CreatePipeline(
        CreatePipelineRequest request,
        CancellationToken cancellationToken)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        if (string.IsNullOrEmpty(request.Name))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["name"] = ["Pipeline name is required"]
            });
        }

        var pipeline = await orchestrator.CreateAsync(
            request.Name,
            request.Description,
            request.Nodes ?? [],
            request.Connections ?? [],
            request.Parameters,
            cancellationToken);

        return Results.Created($"/api/pipelines/{pipeline.Id}", pipeline);
    }

    private static async Task<IResult> UpdatePipeline(
        string id,
        UpdatePipelineRequest request,
        CancellationToken cancellationToken)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        try
        {
            var pipeline = await orchestrator.UpdateAsync(
                id,
                request.Name ?? "",
                request.Description,
                request.Nodes ?? [],
                request.Connections ?? [],
                request.Parameters,
                cancellationToken);

            return Results.Ok(pipeline);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("not found"))
        {
            return Results.NotFound(new ProblemDetails { Detail = ex.Message });
        }
        catch (InvalidOperationException ex)
        {
            return Results.Conflict(new ProblemDetails { Detail = ex.Message });
        }
    }

    private static async Task<IResult> DeletePipeline(
        string id,
        CancellationToken cancellationToken)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        try
        {
            await orchestrator.DeleteAsync(id, cancellationToken);
            return Results.NoContent();
        }
        catch (InvalidOperationException ex)
        {
            return Results.NotFound(new ProblemDetails { Detail = ex.Message });
        }
    }

    private static async Task<IResult> StartPipeline(
        string id,
        StartPipelineRequest? request,
        CancellationToken cancellationToken)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        try
        {
            await orchestrator.StartAsync(id, parameterOverrides: request?.Parameters, cancellationToken: cancellationToken);
            return Results.Accepted();
        }
        catch (InvalidOperationException ex)
        {
            return Results.NotFound(new ProblemDetails { Detail = ex.Message });
        }
    }

    private static async Task<IResult> StopPipeline(
        string id,
        CancellationToken cancellationToken)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        try
        {
            await orchestrator.StopAsync(id, cancellationToken);
            return Results.Accepted();
        }
        catch (InvalidOperationException ex)
        {
            return Results.NotFound(new ProblemDetails { Detail = ex.Message });
        }
    }

    private static IResult GetPipelineStatus(string id)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        var status = orchestrator.GetStatus(id);
        if (status == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Pipeline '{id}' not found" });
        }

        return Results.Ok(status);
    }

    private static IResult GetPipelineVersions(string id)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        var pipeline = orchestrator.Get(id);
        if (pipeline == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Pipeline '{id}' not found" });
        }

        return Results.Ok(orchestrator.GetVersions(id));
    }

    private static IResult GetPipelineVersion(string id, int version)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        var entry = orchestrator.GetVersion(id, version);
        if (entry == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Version {version} not found for pipeline '{id}'" });
        }

        return Results.Ok(entry);
    }

    private static IResult GetPipelineVersionDiff(string id, int from, int to)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        var diff = orchestrator.GetVersionDiff(id, from, to);
        if (diff == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Version diff {from}..{to} not found for pipeline '{id}'" });
        }

        return Results.Ok(diff);
    }

    private static async Task<IResult> RollbackPipeline(
        string id,
        int version,
        CancellationToken cancellationToken)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        try
        {
            var pipeline = await orchestrator.RollbackAsync(id, version, cancellationToken);
            return Results.Ok(pipeline);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("not found"))
        {
            return Results.NotFound(new ProblemDetails { Detail = ex.Message });
        }
        catch (InvalidOperationException ex)
        {
            return Results.Conflict(new ProblemDetails { Detail = ex.Message });
        }
    }

    private static Task<IResult> ValidatePipeline(string id)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");

        var pipeline = orchestrator.Get(id);
        if (pipeline == null)
        {
            return Task.FromResult(Results.NotFound(
                new ProblemDetails { Detail = $"Pipeline '{id}' not found" }));
        }

        var validator = new PipelineValidator();
        var registry = orchestrator.GetAggregatedRegistry();
        var workers = orchestrator.GetWorkers();

        if (registry == null || workers == null)
        {
            // Not in distributed mode — basic validation only
            return Task.FromResult<IResult>(Results.Ok(new PipelineValidationResult
            {
                IsValid = true,
                Issues = []
            }));
        }

        var result = validator.Validate(pipeline, registry, workers);
        return Task.FromResult(Results.Ok(result));
    }

    private static async Task<IResult> DryRunPipeline(
        string id,
        DryRunRequest request,
        CancellationToken cancellationToken)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        try
        {
            var result = await orchestrator.DryRunAsync(id, request.Inputs ?? [], cancellationToken);
            return Results.Ok(result);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("not found"))
        {
            return Results.NotFound(new ProblemDetails { Detail = ex.Message });
        }
    }

    private static IResult GetPipelineMetrics(string id)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        var pipeline = orchestrator.Get(id);
        if (pipeline == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Pipeline '{id}' not found" });
        }

        var metrics = orchestrator.GetMetrics(id);
        if (metrics == null)
        {
            return Results.Ok(new PipelineMetrics
            {
                PipelineId = id,
                StartedAt = pipeline.CreatedAt,
                TotalRecordsProcessed = 0,
                TotalErrors = 0,
                RecordsPerSecond = 0,
                Nodes = new Dictionary<string, NodeMetrics>()
            });
        }

        return Results.Ok(metrics);
    }

    private static IResult ExportPipeline(string id)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        var pipeline = orchestrator.Get(id);
        if (pipeline == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Pipeline '{id}' not found" });
        }

        var export = new PipelineExportFormat
        {
            Version = "1.0",
            ExportedAt = DateTimeOffset.UtcNow,
            SurgewaveVersion = typeof(PipelineRestApi).Assembly.GetName().Version?.ToString() ?? "1.0.0",
            Pipeline = new PipelineExportData
            {
                Name = pipeline.Name,
                Description = pipeline.Description,
                Nodes = pipeline.Nodes.Select(n => new PipelineNodeExport
                {
                    NodeId = n.Id,
                    ConnectorType = n.ConnectorType,
                    Config = new Dictionary<string, string>(n.Config),
                    X = n.X,
                    Y = n.Y,
                    Label = n.Label
                }).ToList(),
                Connections = pipeline.Connections.Select(c => new PipelineConnectionExport
                {
                    SourceNodeId = c.SourceNodeId,
                    TargetNodeId = c.TargetNodeId
                }).ToList()
            }
        };

        return Results.Ok(export);
    }

    private static async Task<IResult> ImportPipeline(
        ImportPipelineRequest request,
        CancellationToken cancellationToken)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");

        var exportData = request.Export.Pipeline;
        var name = request.NameOverride ?? exportData.Name;

        if (string.IsNullOrEmpty(name))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["name"] = ["Pipeline name is required"]
            });
        }

        // Convert export format to internal format
        var nodes = exportData.Nodes.Select(n => new PipelineNode
        {
            Id = Guid.NewGuid().ToString("N"), // Generate new IDs
            ConnectorType = n.ConnectorType,
            Config = new Dictionary<string, string>(n.Config),
            X = n.X,
            Y = n.Y,
            Label = n.Label
        }).ToList();

        // Build ID mapping (old -> new)
        var idMapping = new Dictionary<string, string>();
        for (var i = 0; i < exportData.Nodes.Count; i++)
        {
            idMapping[exportData.Nodes[i].NodeId] = nodes[i].Id;
        }

        // Remap connections
        var connections = exportData.Connections
            .Where(c => idMapping.ContainsKey(c.SourceNodeId) && idMapping.ContainsKey(c.TargetNodeId))
            .Select(c => new PipelineConnection
            {
                Id = Guid.NewGuid().ToString("N"),
                SourceNodeId = idMapping[c.SourceNodeId],
                TargetNodeId = idMapping[c.TargetNodeId]
            }).ToList();

        var pipeline = await orchestrator.CreateAsync(
            name,
            exportData.Description,
            nodes,
            connections,
            cancellationToken: cancellationToken);

        return Results.Created($"/api/pipelines/{pipeline.Id}", pipeline);
    }

    private static IResult ListExecutions(string id, int limit = 50, int offset = 0)
    {
        var store = ExecutionStoreHolder.Instance;
        if (store == null)
        {
            return Results.Ok(new ExecutionListResponse { Executions = [], TotalCount = 0 });
        }

        var executions = store.GetByPipeline(id, limit, offset);
        var totalCount = store.GetCount(id);

        return Results.Ok(new ExecutionListResponse
        {
            Executions = executions.ToList(),
            TotalCount = totalCount,
            HasMore = offset + limit < totalCount
        });
    }

    private static IResult GetExecution(string id, string executionId)
    {
        var store = ExecutionStoreHolder.Instance;
        if (store == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = "Execution store not initialized" });
        }

        var execution = store.Get(executionId);
        if (execution == null || execution.PipelineId != id)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Execution '{executionId}' not found" });
        }

        return Results.Ok(execution);
    }

    private static IResult GetExecutionStats(string id)
    {
        var logger = ExecutionLoggerHolder.Instance;
        if (logger == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = "Execution logger not initialized" });
        }

        // Find active execution for this pipeline
        // Note: In a full implementation, we'd track active execution IDs per pipeline
        return Results.NotFound(new ProblemDetails { Detail = "No active execution found" });
    }

    // --- Debug endpoint handlers ---

    private static IResult SetBreakpoints(string id, SetBreakpointsRequest request)
    {
        var debugger = PipelineDebuggerHolder.Instance;
        if (debugger == null)
            return Results.Problem(detail: "Debugger not initialized");

        foreach (var nodeId in request.NodeIds)
        {
            debugger.SetBreakpoint(id, nodeId);
        }

        return Results.Ok();
    }

    private static IResult RemoveBreakpoint(string id, string nodeId)
    {
        var debugger = PipelineDebuggerHolder.Instance;
        if (debugger == null)
            return Results.Problem(detail: "Debugger not initialized");

        debugger.RemoveBreakpoint(id, nodeId);
        return Results.Ok();
    }

    private static IResult GetDebugState(string id)
    {
        var debugger = PipelineDebuggerHolder.Instance;
        if (debugger == null)
            return Results.Ok(new DebugState());

        return Results.Ok(debugger.GetDebugState(id));
    }

    private static IResult DebugStep(string id, string nodeId)
    {
        var debugger = PipelineDebuggerHolder.Instance;
        if (debugger == null)
            return Results.Problem(detail: "Debugger not initialized");

        debugger.StepNext(id, nodeId);
        return Results.Ok();
    }

    private static IResult DebugResumeNode(string id, string nodeId)
    {
        var debugger = PipelineDebuggerHolder.Instance;
        if (debugger == null)
            return Results.Problem(detail: "Debugger not initialized");

        debugger.ResumeNode(id, nodeId);
        return Results.Ok();
    }

    private static IResult DebugResumeAll(string id)
    {
        var debugger = PipelineDebuggerHolder.Instance;
        if (debugger == null)
            return Results.Problem(detail: "Debugger not initialized");

        debugger.ResumeAll(id);
        return Results.Ok();
    }

    private static IResult ListTemplates()
    {
        var templates = PipelineTemplates.All
            .Select(t => new PipelineTemplateSummary
            {
                Id = t.Id,
                Name = t.Name,
                Description = t.Description,
                Category = t.Category,
                Icon = t.Icon
            })
            .ToList();

        return Results.Ok(templates);
    }

    private static IResult GetTemplate(string templateId)
    {
        var template = PipelineTemplates.GetById(templateId);
        if (template == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Template '{templateId}' not found" });
        }

        return Results.Ok(template);
    }

    private static async Task<IResult> CreateFromTemplate(
        string templateId,
        CreateFromTemplateRequest? request,
        CancellationToken cancellationToken)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");

        var template = PipelineTemplates.GetById(templateId);
        if (template == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Template '{templateId}' not found" });
        }

        var name = request?.Name ?? template.Pipeline.Name;

        // Convert template to pipeline
        var nodes = template.Pipeline.Nodes.Select(n => new PipelineNode
        {
            Id = Guid.NewGuid().ToString("N"),
            ConnectorType = n.ConnectorType,
            Config = new Dictionary<string, string>(n.Config),
            X = n.X,
            Y = n.Y,
            Label = n.Label
        }).ToList();

        var idMapping = new Dictionary<string, string>();
        for (var i = 0; i < template.Pipeline.Nodes.Count; i++)
        {
            idMapping[template.Pipeline.Nodes[i].NodeId] = nodes[i].Id;
        }

        var connections = template.Pipeline.Connections
            .Where(c => idMapping.ContainsKey(c.SourceNodeId) && idMapping.ContainsKey(c.TargetNodeId))
            .Select(c => new PipelineConnection
            {
                Id = Guid.NewGuid().ToString("N"),
                SourceNodeId = idMapping[c.SourceNodeId],
                TargetNodeId = idMapping[c.TargetNodeId]
            }).ToList();

        var pipeline = await orchestrator.CreateAsync(
            name,
            template.Pipeline.Description,
            nodes,
            connections,
            cancellationToken: cancellationToken);

        return Results.Created($"/api/pipelines/{pipeline.Id}", pipeline);
    }

    private static IResult GetSchedule(string id)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        var pipeline = orchestrator.Get(id);
        if (pipeline == null)
            return Results.NotFound(new ProblemDetails { Detail = $"Pipeline '{id}' not found" });
        return Results.Ok(pipeline.Schedule ?? new ScheduleConfig());
    }

    private static async Task<IResult> UpdateSchedule(string id, ScheduleConfig schedule, CancellationToken cancellationToken)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        try
        {
            await orchestrator.UpdateScheduleAsync(id, schedule, cancellationToken);
            return Results.Ok(schedule);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("not found"))
        {
            return Results.NotFound(new ProblemDetails { Detail = ex.Message });
        }
    }

    private static async Task<IResult> AnalyzeChanges(string id, UpdatePipelineRequest request)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        var current = orchestrator.Get(id);
        if (current == null)
            return Results.NotFound(new ProblemDetails { Detail = $"Pipeline '{id}' not found" });

        var proposed = new PipelineDefinition
        {
            Id = id,
            Name = request.Name ?? current.Name,
            Description = request.Description ?? current.Description,
            Nodes = request.Nodes ?? current.Nodes,
            Connections = request.Connections ?? current.Connections,
            Status = current.Status,
            CreatedAt = current.CreatedAt,
            Parameters = request.Parameters ?? current.Parameters
        };

        var analysis = HotDeployAnalyzer.Analyze(current, proposed);
        return Results.Ok(analysis);
    }

    private static async Task<IResult> HotDeploy(string id, UpdatePipelineRequest request, CancellationToken cancellationToken)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        try
        {
            var proposed = new PipelineDefinition
            {
                Id = id,
                Name = request.Name ?? "",
                Description = request.Description,
                Nodes = request.Nodes ?? [],
                Connections = request.Connections ?? [],
                Status = PipelineStatus.Running,
                CreatedAt = DateTimeOffset.UtcNow,
                Parameters = request.Parameters
            };

            var result = await orchestrator.HotDeployAsync(id, proposed, cancellationToken);
            return result
                ? Results.Ok(new { Success = true })
                : Results.Conflict(new ProblemDetails { Detail = "Changes require restart - cannot hot-deploy" });
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("not found"))
        {
            return Results.NotFound(new ProblemDetails { Detail = ex.Message });
        }
    }

    private static IResult EnableProvenance(string id)
    {
        ProvenanceTracker.Enabled = true;
        return Results.Ok();
    }

    private static IResult DisableProvenance(string id)
    {
        ProvenanceTracker.Enabled = false;
        return Results.Ok();
    }

    private static IResult GetPipelinePorts(string id)
    {
        var orchestrator = PipelineOrchestratorHolder.Instance
            ?? throw new InvalidOperationException("Pipeline orchestrator not initialized");
        var pipeline = orchestrator.Get(id);
        if (pipeline == null)
            return Results.NotFound(new ProblemDetails { Detail = $"Pipeline '{id}' not found" });

        var analysis = SubPipelineAnalyzer.AnalyzePorts(pipeline);
        return Results.Ok(analysis);
    }

    private static List<ConnectorTypeInfo> ListConnectorTypes(PluginDiscovery discovery)
    {
        return discovery.GetAllPlugins()
            .Select(p =>
            {
                var metadata = GetConnectorMetadata(p.Class, discovery);
                var displayName = metadata?.Name ?? GetConnectorDisplayName(p.Class);
                var version = metadata?.Version ?? p.Version;

                // Ensure version is not empty or "unknown"
                if (string.IsNullOrWhiteSpace(version) || version.Equals("unknown", StringComparison.OrdinalIgnoreCase))
                {
                    version = "1.0.0";
                }

                // Determine connector type from class name if unknown
                var connectorType = p.Type;
                if (connectorType.Equals("unknown", StringComparison.OrdinalIgnoreCase))
                {
                    connectorType = InferConnectorTypeFromClassName(p.Class);
                }

                return new ConnectorTypeInfo
                {
                    Type = p.Class,
                    Name = displayName,
                    Category = GetConnectorCategory(connectorType),
                    IsSource = connectorType.Equals("source", StringComparison.OrdinalIgnoreCase),
                    IsSink = connectorType.Equals("sink", StringComparison.OrdinalIgnoreCase),
                    Description = metadata?.Description ?? $"A {connectorType} connector",
                    Version = version,
                    Author = metadata?.Author,
                    DocumentationUrl = metadata?.DocumentationUrl,
                    LicenseUrl = metadata?.LicenseUrl,
                    Tags = metadata?.Tags?.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
                    Icon = metadata?.Icon
                };
            })
            .ToList();
    }

    private static string InferConnectorTypeFromClassName(string className)
    {
        // Extract simple class name
        var lastDot = className.LastIndexOf('.');
        var name = lastDot >= 0 ? className[(lastDot + 1)..] : className;

        if (name.Contains("Source", StringComparison.OrdinalIgnoreCase) ||
            name.Contains("Producer", StringComparison.OrdinalIgnoreCase) ||
            name.Contains("Reader", StringComparison.OrdinalIgnoreCase) ||
            name.Contains("Input", StringComparison.OrdinalIgnoreCase))
        {
            return "source";
        }

        if (name.Contains("Sink", StringComparison.OrdinalIgnoreCase) ||
            name.Contains("Consumer", StringComparison.OrdinalIgnoreCase) ||
            name.Contains("Writer", StringComparison.OrdinalIgnoreCase) ||
            name.Contains("Output", StringComparison.OrdinalIgnoreCase))
        {
            return "sink";
        }

        // Default to sink for "Connector" classes without clear indication
        return "sink";
    }

    private static ConnectorMetadataAttribute? GetConnectorMetadata(string className, PluginDiscovery discovery)
    {
        try
        {
            var connectorType = discovery.LoadPluginType(className);
            if (connectorType == null) return null;

            // Get the ConnectorMetadataAttribute using reflection to avoid type identity issues
            foreach (var attr in connectorType.GetCustomAttributes(false))
            {
                var attrType = attr.GetType();
                if (attrType.Name == nameof(ConnectorMetadataAttribute))
                {
                    // Read properties using reflection
                    return new ConnectorMetadataAttribute
                    {
                        Name = attrType.GetProperty("Name")?.GetValue(attr)?.ToString() ?? "",
                        Description = attrType.GetProperty("Description")?.GetValue(attr)?.ToString(),
                        Version = attrType.GetProperty("Version")?.GetValue(attr)?.ToString(),
                        Author = attrType.GetProperty("Author")?.GetValue(attr)?.ToString(),
                        DocumentationUrl = attrType.GetProperty("DocumentationUrl")?.GetValue(attr)?.ToString(),
                        LicenseUrl = attrType.GetProperty("LicenseUrl")?.GetValue(attr)?.ToString(),
                        Tags = attrType.GetProperty("Tags")?.GetValue(attr)?.ToString(),
                        Icon = attrType.GetProperty("Icon")?.GetValue(attr)?.ToString()
                    };
                }
            }
        }
        catch
        {
            // Ignore errors reading metadata
        }

        return null;
    }

    private static IResult GetConnectorConfigSchema(string type, PluginDiscovery discovery)
    {
        var plugin = discovery.GetAllPlugins().FirstOrDefault(p => p.Class == type);
        if (plugin == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector type '{type}' not found" });
        }

        // Load the connector type using PluginDiscovery (handles cross-assembly loading)
        var connectorType = discovery.LoadPluginType(type);
        if (connectorType == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Cannot load connector type '{type}'" });
        }

        try
        {
            // Create instance and get Config property using reflection
            // This avoids type identity issues when assemblies are loaded in different contexts
            var instance = Activator.CreateInstance(connectorType);
            if (instance == null)
            {
                return Results.Problem(detail: $"Failed to create connector instance for '{type}'");
            }

            try
            {
                // Get Config property using reflection
                var configProperty = connectorType.GetProperty("Config");
                if (configProperty == null)
                {
                    return Results.Problem(detail: $"Connector '{type}' does not have a Config property");
                }

                var configDefObj = configProperty.GetValue(instance);
                if (configDefObj == null)
                {
                    return Results.Problem(detail: $"Connector '{type}' returned null Config");
                }

                // Get Keys property from ConfigDef using reflection
                var keysProperty = configDefObj.GetType().GetProperty("Keys");
                if (keysProperty == null)
                {
                    return Results.Problem(detail: $"ConfigDef does not have a Keys property");
                }

                var keysObj = keysProperty.GetValue(configDefObj) as System.Collections.IEnumerable;
                if (keysObj == null)
                {
                    return Results.Ok(new ConnectorConfigSchema { Type = type, Keys = [] });
                }

                var keys = new List<ConfigKeyInfo>();
                foreach (var keyObj in keysObj)
                {
                    var keyType = keyObj.GetType();
                    var importance = keyType.GetProperty("Importance")?.GetValue(keyObj)?.ToString() ?? "Medium";
                    var defaultValue = keyType.GetProperty("DefaultValue")?.GetValue(keyObj)?.ToString();

                    var editor = keyType.GetProperty("Editor")?.GetValue(keyObj);
                    var editorStr = editor?.ToString();
                    if (editorStr == "Default") editorStr = null;

                    keys.Add(new ConfigKeyInfo
                    {
                        Name = keyType.GetProperty("Name")?.GetValue(keyObj)?.ToString() ?? "",
                        Type = keyType.GetProperty("Type")?.GetValue(keyObj)?.ToString() ?? "",
                        DefaultValue = defaultValue,
                        Documentation = keyType.GetProperty("Documentation")?.GetValue(keyObj)?.ToString(),
                        Importance = importance,
                        IsRequired = importance == "High" && defaultValue == null,
                        Editor = editorStr,
                        EditorLanguage = keyType.GetProperty("EditorLanguage")?.GetValue(keyObj)?.ToString(),
                        Options = keyType.GetProperty("Options")?.GetValue(keyObj) as string[]
                    });
                }

                return Results.Ok(new ConnectorConfigSchema { Type = type, Keys = keys });
            }
            finally
            {
                // Dispose if disposable
                (instance as IDisposable)?.Dispose();
            }
        }
        catch (Exception ex)
        {
            return Results.Problem(detail: $"Failed to load connector config: {ex.Message}");
        }
    }

    private static string GetConnectorDisplayName(string className)
    {
        // Extract simple name from full class name
        var lastDot = className.LastIndexOf('.');
        var name = lastDot >= 0 ? className[(lastDot + 1)..] : className;

        // Remove "Connector" suffix if present
        if (name.EndsWith("Connector", StringComparison.OrdinalIgnoreCase))
        {
            name = name[..^9];
        }

        // Add spaces before capitals (e.g., "FileSource" -> "File Source")
        return string.Concat(name.Select((c, i) =>
            i > 0 && char.IsUpper(c) ? " " + c : c.ToString()));
    }

    private static string GetConnectorCategory(string type)
    {
        return type.ToLowerInvariant() switch
        {
            "source" => "Sources",
            "sink" => "Sinks",
            _ => "Other"
        };
    }
}

/// <summary>
/// Request to create a new pipeline.
/// </summary>
public record CreatePipelineRequest
{
    public string? Name { get; init; }
    public string? Description { get; init; }
    public List<PipelineNode>? Nodes { get; init; }
    public List<PipelineConnection>? Connections { get; init; }
    public Dictionary<string, string>? Parameters { get; init; }
}

/// <summary>
/// Request to update a pipeline.
/// </summary>
public record UpdatePipelineRequest
{
    public string? Name { get; init; }
    public string? Description { get; init; }
    public List<PipelineNode>? Nodes { get; init; }
    public List<PipelineConnection>? Connections { get; init; }
    public Dictionary<string, string>? Parameters { get; init; }
}

/// <summary>
/// Information about a connector type.
/// </summary>
public record ConnectorTypeInfo
{
    public required string Type { get; init; }
    public required string Name { get; init; }
    public required string Category { get; init; }
    public bool IsSource { get; init; }
    public bool IsSink { get; init; }
    public string? Description { get; init; }
    public string? Version { get; init; }
    public string? Author { get; init; }
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1056:URI-like properties should not be strings")]
    public string? DocumentationUrl { get; init; }
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1056:URI-like properties should not be strings")]
    public string? LicenseUrl { get; init; }
    public string[]? Tags { get; init; }
    public string? Icon { get; init; }
}

/// <summary>
/// Configuration schema for a connector type.
/// </summary>
public record ConnectorConfigSchema
{
    public required string Type { get; init; }
    public required List<ConfigKeyInfo> Keys { get; init; }
}

/// <summary>
/// Information about a configuration key.
/// </summary>
public record ConfigKeyInfo
{
    public required string Name { get; init; }
    public required string Type { get; init; }
    public string? DefaultValue { get; init; }
    public string? Documentation { get; init; }
    public required string Importance { get; init; }
    public bool IsRequired { get; init; }
    public string? Editor { get; init; }
    public string? EditorLanguage { get; init; }
    public string[]? Options { get; init; }
}

/// <summary>
/// Summary of a pipeline template.
/// </summary>
public record PipelineTemplateSummary
{
    public required string Id { get; init; }
    public required string Name { get; init; }
    public required string Description { get; init; }
    public required string Category { get; init; }
    public string? Icon { get; init; }
}

/// <summary>
/// Request to start a pipeline with optional parameters.
/// </summary>
public record StartPipelineRequest
{
    public Dictionary<string, string>? Parameters { get; init; }
}

/// <summary>
/// Request to create a pipeline from a template.
/// </summary>
public record CreateFromTemplateRequest
{
    public string? Name { get; init; }
}

/// <summary>
/// Response containing a list of executions.
/// </summary>
public record ExecutionListResponse
{
    public required List<ExecutionRecord> Executions { get; init; }
    public int TotalCount { get; init; }
    public bool HasMore { get; init; }
}
