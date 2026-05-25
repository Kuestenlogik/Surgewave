using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// REST API endpoints for cross-topic transactions.
/// </summary>
public static class CrossTopicTransactionRestApi
{
    /// <summary>
    /// Maps cross-topic transaction REST endpoints.
    /// </summary>
    public static IEndpointRouteBuilder MapCrossTopicTransactions(
        this IEndpointRouteBuilder app,
        CrossTopicTransactionManager manager)
    {
        var group = app.MapGroup("/api/transactions")
            .WithTags("Cross-Topic Transactions");

        // POST /api/transactions — Begin a new transaction
        group.MapPost("/", (BeginTransactionRequest? request) =>
        {
            TimeSpan? timeout = request?.TimeoutSeconds > 0
                ? TimeSpan.FromSeconds(request.TimeoutSeconds)
                : null;

            var txn = manager.Begin(request?.ProducerId, timeout);
            return Results.Ok(new TransactionResponse(
                txn.TransactionId,
                txn.State.ToString(),
                txn.StartedAt,
                txn.Timeout.TotalSeconds,
                txn.PendingWrites.Count,
                txn.ProducerId));
        })
        .WithName("BeginCrossTopicTransaction")
        .WithSummary("Begin a new cross-topic transaction")
        .Produces<TransactionResponse>();

        // POST /api/transactions/{id}/write — Add a write to the transaction
        group.MapPost("/{id}/write", (string id, AddWriteRequest request) =>
        {
            try
            {
                var value = Convert.FromBase64String(request.ValueBase64);
                byte[]? key = request.KeyBase64 != null ? Convert.FromBase64String(request.KeyBase64) : null;
                manager.AddWrite(id, request.Topic, request.Partition, key, value, request.Headers);
                return Results.Ok(new { TransactionId = id, PendingWrites = manager.GetTransaction(id)?.PendingWrites.Count ?? 0 });
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { Error = ex.Message });
            }
        })
        .WithName("AddWriteToTransaction")
        .WithSummary("Add a write to a cross-topic transaction")
        .Produces(StatusCodes.Status200OK)
        .ProducesProblem(StatusCodes.Status400BadRequest);

        // POST /api/transactions/{id}/commit — Commit the transaction
        group.MapPost("/{id}/commit", async (string id, CancellationToken ct) =>
        {
            var result = await manager.CommitAsync(id, ct);
            return result.Success
                ? Results.Ok(result)
                : Results.BadRequest(result);
        })
        .WithName("CommitCrossTopicTransaction")
        .WithSummary("Commit a cross-topic transaction atomically")
        .Produces<TransactionCommitResult>()
        .ProducesProblem(StatusCodes.Status400BadRequest);

        // POST /api/transactions/{id}/abort — Abort the transaction
        group.MapPost("/{id}/abort", async (string id, CancellationToken ct) =>
        {
            await manager.AbortAsync(id, ct);
            return Results.Ok(new { TransactionId = id, State = "Aborted" });
        })
        .WithName("AbortCrossTopicTransaction")
        .WithSummary("Abort a cross-topic transaction");

        // GET /api/transactions — List active transactions
        group.MapGet("/", () =>
        {
            var active = manager.ListActive();
            return Results.Ok(active.Select(t => new TransactionResponse(
                t.TransactionId,
                t.State.ToString(),
                t.StartedAt,
                t.Timeout.TotalSeconds,
                t.PendingWrites.Count,
                t.ProducerId)));
        })
        .WithName("ListCrossTopicTransactions")
        .WithSummary("List active cross-topic transactions");

        // GET /api/transactions/{id} — Get transaction status
        group.MapGet("/{id}", (string id) =>
        {
            var txn = manager.GetTransaction(id);
            if (txn == null)
                return Results.NotFound(new { Error = $"Transaction {id} not found" });

            return Results.Ok(new TransactionResponse(
                txn.TransactionId,
                txn.State.ToString(),
                txn.StartedAt,
                txn.Timeout.TotalSeconds,
                txn.PendingWrites.Count,
                txn.ProducerId));
        })
        .WithName("GetCrossTopicTransaction")
        .WithSummary("Get cross-topic transaction status")
        .Produces<TransactionResponse>()
        .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }
}

/// <summary>
/// Request to begin a new cross-topic transaction.
/// </summary>
public sealed record BeginTransactionRequest(string? ProducerId = null, int TimeoutSeconds = 60);

/// <summary>
/// Request to add a write to a cross-topic transaction.
/// </summary>
public sealed record AddWriteRequest(
    string Topic,
    int Partition,
    string? KeyBase64,
    string ValueBase64,
    Dictionary<string, string>? Headers = null);

/// <summary>
/// Cross-topic transaction response.
/// </summary>
public sealed record TransactionResponse(
    string TransactionId,
    string State,
    DateTimeOffset StartedAt,
    double TimeoutSeconds,
    int PendingWrites,
    string? ProducerId);
