using Kuestenlogik.Surgewave.Connect.Distributed;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Validates a pipeline definition against the current cluster state,
/// checking connector availability, worker capabilities, and placement constraints.
/// </summary>
public sealed class PipelineValidator
{
    /// <summary>
    /// Validates a pipeline definition and returns detailed issues per node.
    /// </summary>
    public PipelineValidationResult Validate(
        PipelineDefinition pipeline,
        AggregatedConnectorRegistry registry,
        IReadOnlyDictionary<string, WorkerInfo> workers)
    {
        var issues = new List<PipelineValidationIssue>();

        foreach (var node in pipeline.Nodes)
        {
            ValidateNode(node, pipeline, registry, workers, issues);
        }

        return new PipelineValidationResult
        {
            IsValid = !issues.Any(i => i.Severity == ValidationSeverity.Error),
            Issues = issues
        };
    }

    private static void ValidateNode(
        PipelineNode node,
        PipelineDefinition pipeline,
        AggregatedConnectorRegistry registry,
        IReadOnlyDictionary<string, WorkerInfo> workers,
        List<PipelineValidationIssue> issues)
    {
        var label = node.Label ?? node.Id;
        var placement = node.Placement ?? new PlacementConfig();

        // Check connector type availability across all workers
        var allWorkers = registry.GetWorkersForType(node.ConnectorType);
        var allTypes = registry.GetAllTypes();
        var typeExists = allTypes.Any(t =>
            string.Equals(t.ClassName, node.ConnectorType, StringComparison.OrdinalIgnoreCase));

        if (!typeExists && allWorkers.Count == 0)
        {
            issues.Add(new PipelineValidationIssue
            {
                NodeId = node.Id,
                NodeLabel = label,
                Severity = ValidationSeverity.Error,
                Code = "PLUGIN_NOT_AVAILABLE",
                Message = $"Connector type '{node.ConnectorType}' is not available on any worker or locally"
            });
            return;
        }

        switch (placement.Strategy)
        {
            case PlacementStrategy.Manual:
                ValidateManualPlacement(node, label, placement, registry, workers, issues);
                break;

            case PlacementStrategy.TagBased:
                ValidateTagBasedPlacement(node, label, placement, registry, workers, issues);
                break;

            case PlacementStrategy.Auto:
                ValidateAutoPlacement(node, label, placement, registry, workers, issues);
                break;
        }

        // Check connections
        var hasInput = pipeline.Connections.Any(c =>
            string.Equals(c.TargetNodeId, node.Id, StringComparison.OrdinalIgnoreCase));
        var hasOutput = pipeline.Connections.Any(c =>
            string.Equals(c.SourceNodeId, node.Id, StringComparison.OrdinalIgnoreCase));

        if (!hasInput && !hasOutput)
        {
            issues.Add(new PipelineValidationIssue
            {
                NodeId = node.Id,
                NodeLabel = label,
                Severity = ValidationSeverity.Warning,
                Code = "NO_CONNECTIONS",
                Message = "Node has no input or output connections"
            });
        }
    }

    private static void ValidateManualPlacement(
        PipelineNode node, string label, PlacementConfig placement,
        AggregatedConnectorRegistry registry,
        IReadOnlyDictionary<string, WorkerInfo> workers,
        List<PipelineValidationIssue> issues)
    {
        if (string.IsNullOrEmpty(placement.WorkerId))
        {
            issues.Add(new PipelineValidationIssue
            {
                NodeId = node.Id, NodeLabel = label,
                Severity = ValidationSeverity.Error,
                Code = "WORKER_NOT_SPECIFIED",
                Message = "Manual placement requires a WorkerId"
            });
            return;
        }

        if (!workers.ContainsKey(placement.WorkerId))
        {
            issues.Add(new PipelineValidationIssue
            {
                NodeId = node.Id, NodeLabel = label,
                Severity = ValidationSeverity.Error,
                Code = "WORKER_NOT_FOUND",
                Message = $"Worker '{placement.WorkerId}' is not connected"
            });
            return;
        }

        var worker = workers[placement.WorkerId];
        var hasPlugin = worker.AvailableTypes.Any(t =>
            string.Equals(t.ClassName, node.ConnectorType, StringComparison.OrdinalIgnoreCase));

        if (!hasPlugin)
        {
            if (placement.AllowAutoInstall && worker.AllowAutoInstall)
            {
                issues.Add(new PipelineValidationIssue
                {
                    NodeId = node.Id, NodeLabel = label,
                    Severity = ValidationSeverity.Warning,
                    Code = "WORKER_MISSING_PLUGIN",
                    Message = $"Worker '{placement.WorkerId}' does not have '{node.ConnectorType}' — will be auto-installed"
                });
            }
            else
            {
                issues.Add(new PipelineValidationIssue
                {
                    NodeId = node.Id, NodeLabel = label,
                    Severity = ValidationSeverity.Error,
                    Code = "AUTO_INSTALL_DISABLED",
                    Message = $"Worker '{placement.WorkerId}' does not have '{node.ConnectorType}' and auto-install is disabled"
                });
            }
        }
    }

    private static void ValidateTagBasedPlacement(
        PipelineNode node, string label, PlacementConfig placement,
        AggregatedConnectorRegistry registry,
        IReadOnlyDictionary<string, WorkerInfo> workers,
        List<PipelineValidationIssue> issues)
    {
        if (placement.RequiredTags.Length == 0)
        {
            issues.Add(new PipelineValidationIssue
            {
                NodeId = node.Id, NodeLabel = label,
                Severity = ValidationSeverity.Error,
                Code = "NO_TAGS_SPECIFIED",
                Message = "Tag-based placement requires at least one tag"
            });
            return;
        }

        var matchingWorkers = registry.GetWorkersWithTags(placement.RequiredTags);
        if (matchingWorkers.Count == 0)
        {
            issues.Add(new PipelineValidationIssue
            {
                NodeId = node.Id, NodeLabel = label,
                Severity = ValidationSeverity.Error,
                Code = "NO_WORKER_WITH_TAG",
                Message = $"No connected worker has tags [{string.Join(", ", placement.RequiredTags)}]"
            });
            return;
        }

        var workersWithPlugin = registry.GetWorkersForTypeWithTags(node.ConnectorType, placement.RequiredTags);
        if (workersWithPlugin.Count == 0)
        {
            var anyAutoInstall = matchingWorkers.Any(wId =>
            {
                var meta = registry.GetWorkerMetadata(wId);
                return meta?.AllowAutoInstall == true && placement.AllowAutoInstall;
            });

            if (anyAutoInstall)
            {
                issues.Add(new PipelineValidationIssue
                {
                    NodeId = node.Id, NodeLabel = label,
                    Severity = ValidationSeverity.Warning,
                    Code = "WORKER_MISSING_PLUGIN",
                    Message = $"No worker with tags [{string.Join(", ", placement.RequiredTags)}] has '{node.ConnectorType}' — will be auto-installed"
                });
            }
            else
            {
                issues.Add(new PipelineValidationIssue
                {
                    NodeId = node.Id, NodeLabel = label,
                    Severity = ValidationSeverity.Error,
                    Code = "AUTO_INSTALL_DISABLED",
                    Message = $"No worker with tags [{string.Join(", ", placement.RequiredTags)}] has '{node.ConnectorType}' and auto-install is disabled"
                });
            }
        }
        else
        {
            issues.Add(new PipelineValidationIssue
            {
                NodeId = node.Id, NodeLabel = label,
                Severity = ValidationSeverity.Info,
                Code = "REMOTE_EXECUTION",
                Message = $"Will execute on worker with tags [{string.Join(", ", placement.RequiredTags)}] ({workersWithPlugin.Count} candidates)"
            });
        }
    }

    private static void ValidateAutoPlacement(
        PipelineNode node, string label, PlacementConfig placement,
        AggregatedConnectorRegistry registry,
        IReadOnlyDictionary<string, WorkerInfo> workers,
        List<PipelineValidationIssue> issues)
    {
        var workersForType = registry.GetWorkersForType(node.ConnectorType);
        if (workersForType.Count > 0)
        {
            if (workersForType.Count > 1)
            {
                issues.Add(new PipelineValidationIssue
                {
                    NodeId = node.Id, NodeLabel = label,
                    Severity = ValidationSeverity.Info,
                    Code = "REMOTE_EXECUTION",
                    Message = $"{workersForType.Count} workers available for '{node.ConnectorType}'"
                });
            }
            return;
        }

        // No worker has the plugin — check if auto-install is possible
        var anyAutoInstall = workers.Values.Any(w => w.AllowAutoInstall) && placement.AllowAutoInstall;
        if (anyAutoInstall)
        {
            issues.Add(new PipelineValidationIssue
            {
                NodeId = node.Id, NodeLabel = label,
                Severity = ValidationSeverity.Warning,
                Code = "WORKER_MISSING_PLUGIN",
                Message = $"No worker has '{node.ConnectorType}' locally — will be auto-installed on a suitable worker"
            });
        }
    }
}

/// <summary>
/// Result of pipeline validation with per-node issues.
/// </summary>
public sealed record PipelineValidationResult
{
    public bool IsValid { get; init; }
    public IReadOnlyList<PipelineValidationIssue> Issues { get; init; } = [];

    public IReadOnlyList<PipelineValidationIssue> Errors =>
        Issues.Where(i => i.Severity == ValidationSeverity.Error).ToList();

    public IReadOnlyList<PipelineValidationIssue> Warnings =>
        Issues.Where(i => i.Severity == ValidationSeverity.Warning).ToList();
}

/// <summary>
/// A single validation issue for a pipeline node.
/// </summary>
public sealed record PipelineValidationIssue
{
    public required string NodeId { get; init; }
    public required string NodeLabel { get; init; }
    public required ValidationSeverity Severity { get; init; }
    public required string Code { get; init; }
    public required string Message { get; init; }
}

/// <summary>
/// Severity level for pipeline validation issues.
/// </summary>
public enum ValidationSeverity
{
    /// <summary>Informational — no action needed.</summary>
    Info,
    /// <summary>Warning — pipeline can start but with caveats (e.g., auto-install).</summary>
    Warning,
    /// <summary>Error — pipeline cannot start until resolved.</summary>
    Error
}
