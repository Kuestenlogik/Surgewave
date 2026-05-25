using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Connect.Pipelines.Chat;

/// <summary>
/// Holder for the pipeline chat manager instance.
/// </summary>
public static class PipelineChatManagerHolder
{
    public static PipelineChatManager? Instance { get; set; }
}

/// <summary>
/// REST API for pipeline chat functionality.
/// Provides endpoints for sending messages to agent-based pipelines
/// and managing chat sessions.
/// </summary>
public static class PipelineChatApi
{
    /// <summary>
    /// Maps chat API endpoints under /api/pipelines/{id}/chat.
    /// </summary>
    public static IEndpointRouteBuilder MapSurgewavePipelineChat(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/pipelines/{pipelineId}/chat")
            .WithTags("Pipeline Chat");

        // Send a message and wait for response
        group.MapPost("", SendMessage)
            .WithName("SendChatMessage")
            .WithSummary("Send a chat message to an agent pipeline and wait for a response")
            .Produces<ChatResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status503ServiceUnavailable);

        // Send a message and stream the response via Server-Sent Events
        group.MapPost("/stream", StreamMessage)
            .WithName("StreamChatMessage")
            .WithSummary("Send a chat message and stream the response as Server-Sent Events")
            .Produces(StatusCodes.Status200OK, contentType: "text/event-stream")
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status503ServiceUnavailable);

        // Send a message without waiting (fire-and-forget)
        group.MapPost("/async", SendMessageAsync)
            .WithName("SendChatMessageAsync")
            .WithSummary("Send a chat message without waiting for a response")
            .Produces<AsyncChatResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Get chat history for a session
        group.MapGet("/sessions/{sessionId}/history", GetHistory)
            .WithName("GetChatHistory")
            .WithSummary("Get conversation history for a chat session")
            .Produces<ChatHistoryResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // List sessions for a pipeline
        group.MapGet("/sessions", ListSessions)
            .WithName("ListChatSessions")
            .WithSummary("List active chat sessions for a pipeline")
            .Produces<ChatSessionListResponse>();

        // Delete a session
        group.MapDelete("/sessions/{sessionId}", DeleteSession)
            .WithName("DeleteChatSession")
            .WithSummary("Delete a chat session")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Get topic info (for pipeline editor wiring)
        group.MapGet("/topics", GetChatTopics)
            .WithName("GetChatTopics")
            .WithSummary("Get the signal and response topic names for wiring in pipelines")
            .Produces<ChatTopicInfo>();

        return app;
    }

    private static async Task<IResult> SendMessage(string pipelineId, ChatRequest request, CancellationToken cancellationToken)
    {
        var manager = GetManager();
        if (manager is null)
            return Results.Problem("Chat service not available", statusCode: StatusCodes.Status503ServiceUnavailable);

        var orchestrator = PipelineOrchestratorHolder.Instance;
        if (orchestrator?.Get(pipelineId) is null)
            return Results.NotFound(new ProblemDetails { Detail = $"Pipeline '{pipelineId}' not found" });

        var sessionId = request.SessionId ?? Guid.NewGuid().ToString("N");

        try
        {
            var response = await manager.SendMessageAsync(pipelineId, sessionId, request.Message, cancellationToken);
            return Results.Ok(new ChatResponse
            {
                SessionId = sessionId,
                MessageId = response.Id,
                Content = response.Content,
                Role = response.Role,
                Timestamp = response.Timestamp,
                Metadata = response.Metadata
            });
        }
        catch (Exception ex)
        {
            return Results.Problem(ex.Message, statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static async Task<IResult> SendMessageAsync(string pipelineId, ChatRequest request, CancellationToken cancellationToken)
    {
        var manager = GetManager();
        if (manager is null)
            return Results.Problem("Chat service not available", statusCode: StatusCodes.Status503ServiceUnavailable);

        var orchestrator = PipelineOrchestratorHolder.Instance;
        if (orchestrator?.Get(pipelineId) is null)
            return Results.NotFound(new ProblemDetails { Detail = $"Pipeline '{pipelineId}' not found" });

        var sessionId = request.SessionId ?? Guid.NewGuid().ToString("N");

        var messageId = await manager.SendMessageFireAndForgetAsync(pipelineId, sessionId, request.Message, cancellationToken);
        return Results.Ok(new AsyncChatResponse
        {
            SessionId = sessionId,
            MessageId = messageId
        });
    }

    private static readonly JsonSerializerOptions s_sseJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    private static async Task StreamMessage(HttpContext httpContext, string pipelineId, ChatRequest request, CancellationToken cancellationToken)
    {
        var manager = GetManager();
        if (manager is null)
        {
            httpContext.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            await httpContext.Response.WriteAsJsonAsync(
                new ProblemDetails { Detail = "Chat service not available" }, cancellationToken);
            return;
        }

        var orchestrator = PipelineOrchestratorHolder.Instance;
        if (orchestrator?.Get(pipelineId) is null)
        {
            httpContext.Response.StatusCode = StatusCodes.Status404NotFound;
            await httpContext.Response.WriteAsJsonAsync(
                new ProblemDetails { Detail = $"Pipeline '{pipelineId}' not found" }, cancellationToken);
            return;
        }

        var sessionId = request.SessionId ?? Guid.NewGuid().ToString("N");

        httpContext.Response.ContentType = "text/event-stream";
        httpContext.Response.Headers.CacheControl = "no-cache";
        httpContext.Response.Headers.Connection = "keep-alive";

        try
        {
            await foreach (var evt in manager.StreamChatAsync(pipelineId, sessionId, request.Message, cancellationToken))
            {
                var json = JsonSerializer.Serialize(evt, s_sseJsonOptions);
                await httpContext.Response.WriteAsync($"event: {evt.Type}\ndata: {json}\n\n", cancellationToken);
                await httpContext.Response.Body.FlushAsync(cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Client disconnected, normal for SSE
        }
        catch (Exception ex)
        {
            var errorEvt = new ChatStreamEvent { Type = "error", Error = ex.Message };
            var json = JsonSerializer.Serialize(errorEvt, s_sseJsonOptions);
            try
            {
                await httpContext.Response.WriteAsync($"event: error\ndata: {json}\n\n", cancellationToken);
                await httpContext.Response.Body.FlushAsync(cancellationToken);
            }
            catch
            {
                // Response may already be completed
            }
        }
    }

    private static IResult GetHistory(string pipelineId, string sessionId)
    {
        var manager = GetManager();
        if (manager is null)
            return Results.Problem("Chat service not available", statusCode: StatusCodes.Status503ServiceUnavailable);

        var session = manager.GetSession(pipelineId, sessionId);
        if (session is null)
            return Results.NotFound(new ProblemDetails { Detail = $"Session '{sessionId}' not found" });

        return Results.Ok(new ChatHistoryResponse
        {
            SessionId = session.SessionId,
            PipelineId = session.PipelineId,
            Messages = session.History,
            CreatedAt = session.CreatedAt,
            LastActivityAt = session.LastActivityAt
        });
    }

    private static IResult ListSessions(string pipelineId)
    {
        var manager = GetManager();
        if (manager is null)
            return Results.Problem("Chat service not available", statusCode: StatusCodes.Status503ServiceUnavailable);

        var sessions = manager.ListSessions(pipelineId);
        return Results.Ok(new ChatSessionListResponse
        {
            PipelineId = pipelineId,
            Sessions = sessions.Select(s => new ChatSessionSummary
            {
                SessionId = s.SessionId,
                MessageCount = s.MessageCount,
                PendingCount = s.PendingCount,
                CreatedAt = s.CreatedAt,
                LastActivityAt = s.LastActivityAt
            }).ToList()
        });
    }

    private static IResult DeleteSession(string pipelineId, string sessionId)
    {
        var manager = GetManager();
        if (manager is null)
            return Results.Problem("Chat service not available", statusCode: StatusCodes.Status503ServiceUnavailable);

        return manager.RemoveSession(pipelineId, sessionId)
            ? Results.NoContent()
            : Results.NotFound(new ProblemDetails { Detail = $"Session '{sessionId}' not found" });
    }

    private static IResult GetChatTopics(string pipelineId)
    {
        return Results.Ok(new ChatTopicInfo
        {
            PipelineId = pipelineId,
            SignalTopic = PipelineChatManager.GetSignalTopicName(pipelineId),
            ResponseTopic = PipelineChatManager.GetResponseTopicName(pipelineId)
        });
    }

    private static PipelineChatManager? GetManager() => PipelineChatManagerHolder.Instance;
}

// --- Request/Response Models ---

public record ChatRequest
{
    public required string Message { get; init; }
    public string? SessionId { get; init; }
}

public record ChatResponse
{
    public required string SessionId { get; init; }
    public required string MessageId { get; init; }
    public required string Content { get; init; }
    public required string Role { get; init; }
    public DateTimeOffset Timestamp { get; init; }
    public Dictionary<string, string>? Metadata { get; init; }
}

public record AsyncChatResponse
{
    public required string SessionId { get; init; }
    public required string MessageId { get; init; }
}

public record ChatHistoryResponse
{
    public required string SessionId { get; init; }
    public required string PipelineId { get; init; }
    public required IReadOnlyList<ChatMessage> Messages { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset LastActivityAt { get; init; }
}

public record ChatSessionListResponse
{
    public required string PipelineId { get; init; }
    public required IReadOnlyList<ChatSessionSummary> Sessions { get; init; }
}

public record ChatSessionSummary
{
    public required string SessionId { get; init; }
    public int MessageCount { get; init; }
    public int PendingCount { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset LastActivityAt { get; init; }
}

public record ChatTopicInfo
{
    public required string PipelineId { get; init; }
    public required string SignalTopic { get; init; }
    public required string ResponseTopic { get; init; }
}

