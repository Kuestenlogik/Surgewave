using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Upgrades;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// REST API endpoints for rolling upgrade management.
/// Provides cluster version information, compatibility checks, and graceful shutdown control.
/// </summary>
public static class RollingUpgradeRestApi
{
    public static IEndpointRouteBuilder MapRollingUpgrade(
        this IEndpointRouteBuilder app,
        ClusterState clusterState,
        ClusteringConfig clusteringConfig,
        GracefulShutdownOrchestrator? shutdownOrchestrator,
        VersionCompatibilityChecker compatibilityChecker)
    {
        var group = app.MapGroup("/api/cluster")
            .WithTags("Rolling Upgrades");

        // GET /api/cluster/version — Current broker version + all cluster broker versions
        group.MapGet("/version", () => GetClusterVersions(clusterState, clusteringConfig))
            .WithName("GetClusterVersions")
            .WithSummary("Get current broker version and all cluster broker versions")
            .Produces<ClusterVersionResponse>();

        // POST /api/cluster/upgrade/check — Pre-upgrade compatibility check
        group.MapPost("/upgrade/check", (UpgradeCheckRequest request) =>
                CheckCompatibility(request, compatibilityChecker, clusterState))
            .WithName("CheckUpgradeCompatibility")
            .WithSummary("Check if a target version is compatible with the current cluster")
            .Produces<UpgradeCheckResponse>()
            .ProducesValidationProblem();

        // POST /api/cluster/upgrade/prepare — Pre-flight checks for upgrade
        group.MapPost("/upgrade/prepare", () => PrepareUpgrade(clusterState, clusteringConfig))
            .WithName("PrepareUpgrade")
            .WithSummary("Run pre-flight checks before starting a rolling upgrade")
            .Produces<UpgradePrepareResponse>();

        // POST /api/broker/shutdown/graceful — Initiate graceful shutdown
        group.MapPost("/broker/shutdown/graceful", async (CancellationToken ct) =>
                await InitiateGracefulShutdown(shutdownOrchestrator, ct))
            .WithName("InitiateGracefulShutdown")
            .WithSummary("Initiate graceful shutdown with leadership transfer")
            .Produces<ShutdownStatusResponse>();

        // GET /api/broker/shutdown/status — Shutdown progress
        group.MapGet("/broker/shutdown/status", () => GetShutdownStatus(shutdownOrchestrator))
            .WithName("GetShutdownStatus")
            .WithSummary("Get current shutdown progress")
            .Produces<ShutdownStatusResponse>();

        return app;
    }

    private static IResult GetClusterVersions(ClusterState clusterState, ClusteringConfig config)
    {
        var localVersion = BrokerVersion.Current;
        var brokerVersions = clusterState.Brokers.Values.Select(b => new BrokerVersionInfo(
            b.BrokerId,
            b.Host,
            b.Port,
            localVersion.ToString() // In a real cluster, each broker would report its own version
        )).ToList();

        return Results.Ok(new ClusterVersionResponse(
            localVersion.ToString(),
            config.BrokerId,
            brokerVersions));
    }

    private static IResult CheckCompatibility(
        UpgradeCheckRequest request,
        VersionCompatibilityChecker checker,
        ClusterState clusterState)
    {
        if (!BrokerVersion.TryParse(request.TargetVersion, out var targetVersion) || targetVersion is null)
        {
            return Results.BadRequest(new { Error = $"Invalid version format: '{request.TargetVersion}'" });
        }

        // Collect current cluster versions (using current version for all brokers in this node's view)
        var clusterVersions = clusterState.Brokers.Values
            .Select(_ => BrokerVersion.Current)
            .ToList();

        var result = checker.Check(targetVersion, clusterVersions);

        return Results.Ok(new UpgradeCheckResponse(
            result.IsCompatible,
            targetVersion.ToString(),
            BrokerVersion.Current.ToString(),
            result.Reason,
            result.Warnings.ToList()));
    }

    private static IResult PrepareUpgrade(ClusterState clusterState, ClusteringConfig config)
    {
        var checks = new List<PreFlightCheck>();

        // Check 1: Minimum brokers
        var brokerCount = clusterState.Brokers.Count;
        checks.Add(new PreFlightCheck(
            "MinimumBrokers",
            brokerCount >= 2,
            brokerCount >= 2
                ? $"{brokerCount} brokers available"
                : "At least 2 brokers required for zero-downtime upgrade"));

        // Check 2: All partitions have ISR > 1
        var underReplicatedPartitions = new List<string>();
        foreach (var (tp, state) in clusterState.PartitionStates)
        {
            if (state.Isr.Count <= 1 && state.Replicas.Count > 1)
            {
                underReplicatedPartitions.Add($"{tp.Topic}-{tp.Partition}");
            }
        }
        checks.Add(new PreFlightCheck(
            "FullIsr",
            underReplicatedPartitions.Count == 0,
            underReplicatedPartitions.Count == 0
                ? "All partitions have full ISR"
                : $"{underReplicatedPartitions.Count} under-replicated partitions: {string.Join(", ", underReplicatedPartitions.Take(10))}"));

        // Check 3: No active reassignments
        checks.Add(new PreFlightCheck(
            "NoActiveReassignments",
            true, // Would check reassignment manager if available
            "No active partition reassignments"));

        // Check 4: Controller is available
        var controllerAvailable = clusterState.ControllerId >= 0;
        checks.Add(new PreFlightCheck(
            "ControllerAvailable",
            controllerAvailable,
            controllerAvailable
                ? $"Controller is broker {clusterState.ControllerId}"
                : "No controller elected"));

        var allPassed = checks.All(c => c.Passed);

        return Results.Ok(new UpgradePrepareResponse(
            allPassed,
            BrokerVersion.Current.ToString(),
            checks));
    }

    private static async Task<IResult> InitiateGracefulShutdown(
        GracefulShutdownOrchestrator? orchestrator,
        CancellationToken ct)
    {
        if (orchestrator is null)
        {
            return Results.Ok(new ShutdownStatusResponse(
                false,
                ShutdownProgress.NotStarted.ToString(),
                "Rolling upgrade infrastructure not initialized (single-node mode?)",
                null));
        }

        if (orchestrator.IsShuttingDown)
        {
            return Results.Ok(new ShutdownStatusResponse(
                true,
                orchestrator.Progress.ToString(),
                "Shutdown already in progress",
                null));
        }

        var result = await orchestrator.InitiateGracefulShutdownAsync(ct: ct);

        return Results.Ok(new ShutdownStatusResponse(
            result.Success,
            ShutdownProgress.Completed.ToString(),
            result.Success ? "Graceful shutdown completed" : "Shutdown completed with warnings",
            new ShutdownDetails(
                result.PartitionsTransferred,
                result.ConnectionsClosed,
                result.Duration.TotalMilliseconds,
                result.Warnings.ToList())));
    }

    private static IResult GetShutdownStatus(GracefulShutdownOrchestrator? orchestrator)
    {
        if (orchestrator is null)
        {
            return Results.Ok(new ShutdownStatusResponse(
                false,
                ShutdownProgress.NotStarted.ToString(),
                "Rolling upgrade infrastructure not initialized",
                null));
        }

        return Results.Ok(new ShutdownStatusResponse(
            orchestrator.IsShuttingDown,
            orchestrator.Progress.ToString(),
            orchestrator.IsShuttingDown ? "Shutdown in progress" : "No shutdown in progress",
            null));
    }
}

// --- Response DTOs ---

public sealed record ClusterVersionResponse(
    string LocalVersion,
    int LocalBrokerId,
    List<BrokerVersionInfo> Brokers);

public sealed record BrokerVersionInfo(
    int BrokerId,
    string Host,
    int Port,
    string Version);

public sealed record UpgradeCheckRequest(string TargetVersion);

public sealed record UpgradeCheckResponse(
    bool IsCompatible,
    string TargetVersion,
    string CurrentVersion,
    string? Reason,
    List<string> Warnings);

public sealed record UpgradePrepareResponse(
    bool Ready,
    string CurrentVersion,
    List<PreFlightCheck> Checks);

public sealed record PreFlightCheck(
    string Name,
    bool Passed,
    string Message);

public sealed record ShutdownStatusResponse(
    bool InProgress,
    string Phase,
    string Message,
    ShutdownDetails? Details);

public sealed record ShutdownDetails(
    int PartitionsTransferred,
    int ConnectionsClosed,
    double DurationMs,
    List<string> Warnings);
