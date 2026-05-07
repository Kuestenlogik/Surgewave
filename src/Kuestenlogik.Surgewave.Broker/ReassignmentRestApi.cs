using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Reassignment;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// REST API endpoints for online partition reassignment.
/// Provides plan management, progress tracking, auto-balancing, and broker decommission.
/// </summary>
public static class ReassignmentRestApi
{
    public static IEndpointRouteBuilder MapSurgewaveReassignment(
        this IEndpointRouteBuilder app,
        ReassignmentExecutor executor,
        ReassignmentPlanner planner,
        ReassignmentConfig reassignmentConfig)
    {
        var group = app.MapGroup("/api/partitions")
            .WithTags("Partition Reassignment");

        // POST /api/partitions/reassign — Submit a reassignment plan
        group.MapPost("/reassign", async (SubmitReassignmentRequest request, CancellationToken ct) =>
                await SubmitPlan(executor, planner, request, ct))
            .WithName("SubmitReassignmentPlan")
            .WithSummary("Submit a reassignment plan for execution")
            .Produces<ReassignmentPlanResponse>()
            .ProducesValidationProblem();

        // GET /api/partitions/reassign/{planId} — Get plan status with progress
        group.MapGet("/reassign/{planId}", (string planId) => GetPlanStatus(executor, planId))
            .WithName("GetReassignmentPlanStatus")
            .WithSummary("Get status and progress of a reassignment plan")
            .Produces<ReassignmentPlanResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // DELETE /api/partitions/reassign/{planId} — Cancel a plan
        group.MapDelete("/reassign/{planId}", async (string planId, CancellationToken ct) =>
                await CancelPlan(executor, planId, ct))
            .WithName("CancelReassignmentPlan")
            .WithSummary("Cancel a running reassignment plan")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /api/partitions/reassign — List all plans
        group.MapGet("/reassign", () => ListPlans(executor))
            .WithName("ListReassignmentPlans")
            .WithSummary("List all reassignment plans (active and completed)")
            .Produces<IReadOnlyList<ReassignmentPlanSummaryResponse>>();

        // POST /api/partitions/balance — Auto-generate and execute a balance plan
        group.MapPost("/balance", async (CancellationToken ct) =>
                await AutoBalance(executor, planner, reassignmentConfig, ct))
            .WithName("AutoBalancePartitions")
            .WithSummary("Automatically generate and execute a balance plan across all brokers")
            .Produces<ReassignmentPlanResponse>();

        // POST /api/partitions/decommission/{brokerId} — Generate decommission plan
        group.MapPost("/decommission/{brokerId:int}", async (int brokerId, CancellationToken ct) =>
                await DecommissionBroker(executor, planner, brokerId, reassignmentConfig, ct))
            .WithName("DecommissionBroker")
            .WithSummary("Generate and execute a plan to move all partitions off a broker")
            .Produces<ReassignmentPlanResponse>()
            .ProducesProblem(StatusCodes.Status400BadRequest);

        // GET /api/partitions/assignment — Current partition assignment across all brokers
        group.MapGet("/assignment", () => GetAssignment(executor))
            .WithName("GetPartitionAssignment")
            .WithSummary("Get current partition assignment across all brokers")
            .Produces<IReadOnlyList<TopicPartitionAssignmentResponse>>();

        // POST /api/partitions/reassign/validate — Validate a proposed plan without executing
        group.MapPost("/reassign/validate", (SubmitReassignmentRequest request) =>
                ValidatePlan(executor, planner, request))
            .WithName("ValidateReassignmentPlan")
            .WithSummary("Validate a proposed reassignment plan without executing it")
            .Produces<ReassignmentValidationResponse>();

        return app;
    }

    private static async Task<IResult> SubmitPlan(
        ReassignmentExecutor executor,
        ReassignmentPlanner planner,
        SubmitReassignmentRequest request,
        CancellationToken ct)
    {
        var plan = new OnlineReassignmentPlan
        {
            ThrottleRateBytesPerSec = request.ThrottleRateBytesPerSec ?? 50_000_000,
            Description = request.Description
        };

        foreach (var a in request.Assignments)
        {
            plan.Assignments.Add(new OnlinePartitionReassignment
            {
                Topic = a.Topic,
                Partition = a.Partition,
                CurrentReplicas = a.CurrentReplicas ?? [],
                TargetReplicas = a.TargetReplicas
            });
        }

        var result = await executor.ExecuteAsync(plan, ct);
        return Results.Ok(MapPlanResponse(plan, result));
    }

    private static IResult GetPlanStatus(ReassignmentExecutor executor, string planId)
    {
        var plan = executor.GetStatus(planId);
        if (plan == null)
            return Results.NotFound(new { message = $"Plan '{planId}' not found" });

        return Results.Ok(MapPlanResponse(plan));
    }

    private static async Task<IResult> CancelPlan(
        ReassignmentExecutor executor, string planId, CancellationToken ct)
    {
        var plan = executor.GetStatus(planId);
        if (plan == null)
            return Results.NotFound(new { message = $"Plan '{planId}' not found" });

        await executor.CancelAsync(planId, ct);
        return Results.NoContent();
    }

    private static IResult ListPlans(ReassignmentExecutor executor)
    {
        var plans = executor.ListReassignments();
        var summaries = plans.Select(p => new ReassignmentPlanSummaryResponse(
            p.Id,
            p.Status.ToString(),
            p.Assignments.Count,
            p.Assignments.Count(a => a.Status == ReassignmentStatus.Completed),
            p.Assignments.Count(a => a.Status == ReassignmentStatus.Failed),
            p.CreatedAt,
            p.CompletedAt,
            p.Description)).ToList();

        return Results.Ok(summaries);
    }

    private static async Task<IResult> AutoBalance(
        ReassignmentExecutor executor,
        ReassignmentPlanner planner,
        ReassignmentConfig reassignmentConfig,
        CancellationToken ct)
    {
        var currentAssignments = executor.GetCurrentAssignments();
        var brokerIds = currentAssignments
            .SelectMany(a => a.Replicas)
            .Distinct()
            .OrderBy(id => id)
            .ToList();

        var plan = planner.GenerateBalancePlan(currentAssignments, brokerIds);
        plan.ThrottleRateBytesPerSec = reassignmentConfig.DefaultThrottleRateBytesPerSec;

        if (plan.Assignments.Count == 0)
        {
            return Results.Ok(new ReassignmentPlanResponse(
                plan.Id, "Completed", [], 0, 0, 0, null, "No rebalancing needed"));
        }

        var result = await executor.ExecuteAsync(plan, ct);
        return Results.Ok(MapPlanResponse(plan, result));
    }

    private static async Task<IResult> DecommissionBroker(
        ReassignmentExecutor executor,
        ReassignmentPlanner planner,
        int brokerId,
        ReassignmentConfig reassignmentConfig,
        CancellationToken ct)
    {
        var currentAssignments = executor.GetCurrentAssignments();
        var allBrokerIds = currentAssignments
            .SelectMany(a => a.Replicas)
            .Distinct()
            .OrderBy(id => id)
            .ToList();

        if (!allBrokerIds.Contains(brokerId))
        {
            return Results.BadRequest(new { message = $"Broker {brokerId} has no partition assignments" });
        }

        var remainingBrokers = allBrokerIds.Where(id => id != brokerId).ToList();

        if (remainingBrokers.Count == 0)
        {
            return Results.BadRequest(new { message = "Cannot decommission the only broker" });
        }

        var plan = planner.GenerateDecommissionPlan(brokerId, currentAssignments, remainingBrokers);
        plan.ThrottleRateBytesPerSec = reassignmentConfig.DefaultThrottleRateBytesPerSec;

        if (plan.Assignments.Count == 0)
        {
            return Results.Ok(new ReassignmentPlanResponse(
                plan.Id, "Completed", [], 0, 0, 0, null,
                $"Broker {brokerId} has no partitions to move"));
        }

        var result = await executor.ExecuteAsync(plan, ct);
        return Results.Ok(MapPlanResponse(plan, result));
    }

    private static IResult GetAssignment(ReassignmentExecutor executor)
    {
        var assignments = executor.GetCurrentAssignments();
        var response = assignments.Select(a => new TopicPartitionAssignmentResponse(
            a.Topic,
            a.Partition,
            a.Leader,
            a.Replicas.ToList(),
            a.Isr.ToList(),
            a.SizeBytes)).ToList();

        return Results.Ok(response);
    }

    private static IResult ValidatePlan(
        ReassignmentExecutor executor,
        ReassignmentPlanner planner,
        SubmitReassignmentRequest request)
    {
        var plan = new OnlineReassignmentPlan();
        foreach (var a in request.Assignments)
        {
            plan.Assignments.Add(new OnlinePartitionReassignment
            {
                Topic = a.Topic,
                Partition = a.Partition,
                CurrentReplicas = a.CurrentReplicas ?? [],
                TargetReplicas = a.TargetReplicas
            });
        }

        var currentAssignments = executor.GetCurrentAssignments();
        var brokerIds = currentAssignments
            .SelectMany(a => a.Replicas)
            .Distinct()
            .ToList();

        var validation = planner.ValidatePlan(plan, brokerIds);

        return Results.Ok(new ReassignmentValidationResponse(
            validation.IsValid,
            validation.Errors.ToList(),
            validation.Warnings.ToList()));
    }

    private static ReassignmentPlanResponse MapPlanResponse(
        OnlineReassignmentPlan plan,
        ReassignmentResult? result = null)
    {
        return new ReassignmentPlanResponse(
            plan.Id,
            plan.Status.ToString(),
            plan.Assignments.Select(a => new PartitionReassignmentResponse(
                a.Topic,
                a.Partition,
                a.CurrentReplicas.ToList(),
                a.TargetReplicas.ToList(),
                a.Status.ToString(),
                a.Progress,
                a.BytesCopied,
                a.TotalBytes,
                a.Error)).ToList(),
            result?.Completed ?? plan.Assignments.Count(a => a.Status == ReassignmentStatus.Completed),
            result?.Failed ?? plan.Assignments.Count(a => a.Status == ReassignmentStatus.Failed),
            plan.ThrottleRateBytesPerSec,
            plan.CompletedAt,
            plan.Description);
    }
}

// --- Request / Response DTOs ---

/// <summary>
/// Request to submit a reassignment plan.
/// </summary>
public sealed record SubmitReassignmentRequest(
    List<ReassignmentAssignmentRequest> Assignments,
    int? ThrottleRateBytesPerSec = null,
    string? Description = null);

/// <summary>
/// A single partition assignment in a submission request.
/// </summary>
public sealed record ReassignmentAssignmentRequest(
    string Topic,
    int Partition,
    List<int> TargetReplicas,
    List<int>? CurrentReplicas = null);

/// <summary>
/// Response representing a reassignment plan with progress details.
/// </summary>
public sealed record ReassignmentPlanResponse(
    string PlanId,
    string Status,
    List<PartitionReassignmentResponse> Assignments,
    int Completed,
    int Failed,
    int ThrottleRateBytesPerSec,
    DateTimeOffset? CompletedAt,
    string? Description);

/// <summary>
/// Progress of a single partition reassignment.
/// </summary>
public sealed record PartitionReassignmentResponse(
    string Topic,
    int Partition,
    List<int> CurrentReplicas,
    List<int> TargetReplicas,
    string Status,
    double Progress,
    long BytesCopied,
    long TotalBytes,
    string? Error);

/// <summary>
/// Summary of a reassignment plan (for list view).
/// </summary>
public sealed record ReassignmentPlanSummaryResponse(
    string PlanId,
    string Status,
    int TotalPartitions,
    int Completed,
    int Failed,
    DateTimeOffset CreatedAt,
    DateTimeOffset? CompletedAt,
    string? Description);

/// <summary>
/// Current partition assignment information.
/// </summary>
public sealed record TopicPartitionAssignmentResponse(
    string Topic,
    int Partition,
    int Leader,
    List<int> Replicas,
    List<int> Isr,
    long SizeBytes);

/// <summary>
/// Result of validating a reassignment plan.
/// </summary>
public sealed record ReassignmentValidationResponse(
    bool IsValid,
    List<string> Errors,
    List<string> Warnings);
