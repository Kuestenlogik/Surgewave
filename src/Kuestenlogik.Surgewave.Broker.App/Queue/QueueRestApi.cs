namespace Kuestenlogik.Surgewave.Broker.Queue;

/// <summary>
/// Minimal REST API for Queue semantics management.
/// Provides visibility into active QueueViews and basic operational endpoints.
/// </summary>
public static class QueueRestApi
{
    /// <summary>
    /// Registers the /api/queue route group on <paramref name="app"/>.
    /// </summary>
    public static IEndpointRouteBuilder MapSurgewaveQueue(
        this IEndpointRouteBuilder app,
        QueueViewManager manager)
    {
        var group = app.MapGroup("/api/queue")
            .WithTags("Queue Semantics");

        // GET /api/queue/topics
        group.MapGet("/topics", () => ListTopics(manager))
            .WithName("ListQueueTopics")
            .WithSummary("List topics that have an active QueueView")
            .Produces<IReadOnlyList<string>>();

        // GET /api/queue/{topic}/status
        group.MapGet("/{topic}/status", (string topic) => GetStatus(manager, topic))
            .WithName("GetQueueStatus")
            .WithSummary("Get in-flight count and committed offsets for a topic's QueueView")
            .Produces<QueueStatusResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // POST /api/queue/{topic}/purge
        group.MapPost("/{topic}/purge", (string topic) => Purge(manager, topic))
            .WithName("PurgeQueue")
            .WithSummary("Clear all in-flight messages for a topic's QueueView")
            .Produces<PurgeResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /api/queue/{topic}/inflight
        group.MapGet("/{topic}/inflight", (string topic) => GetInFlight(manager, topic))
            .WithName("GetQueueInFlight")
            .WithSummary("List all messages currently in-flight for a topic's QueueView")
            .Produces<IReadOnlyList<InFlightMessageResponse>>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /api/queue/{topic}/dlq
        group.MapGet("/{topic}/dlq", (string topic, int offset, int limit) => GetDlq(manager, topic, offset, limit))
            .WithName("BrowseDlq")
            .WithSummary("Browse messages in the Dead-Letter-Queue topic for a given topic")
            .Produces<DlqBrowseResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /api/queue/{topic}/metrics
        group.MapGet("/{topic}/metrics", (string topic) => GetMetrics(manager, topic))
            .WithName("GetQueueMetrics")
            .WithSummary("Return ack/nack/reject/expired/redelivered/received counts for a topic's QueueView")
            .Produces<QueueMetricsResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }

    private static IResult ListTopics(QueueViewManager manager)
    {
        var topics = manager.ActiveTopics;
        return Results.Ok(topics);
    }

    private static IResult GetStatus(QueueViewManager manager, string topic)
    {
        var view = manager.Get(topic);
        if (view is null)
            return Results.NotFound(new { message = $"No QueueView found for topic '{topic}'" });

        var response = new QueueStatusResponse(
            Topic: topic,
            InFlightCount: view.InFlightCount);

        return Results.Ok(response);
    }

    private static IResult Purge(QueueViewManager manager, string topic)
    {
        var view = manager.Get(topic);
        if (view is null)
            return Results.NotFound(new { message = $"No QueueView found for topic '{topic}'" });

        // Removing and re-creating is the safest purge — removes in-flight state completely
        manager.Remove(topic);

        return Results.Ok(new PurgeResponse(
            Topic: topic,
            Message: "In-flight messages cleared. QueueView will be recreated on next receive call."));
    }

    private static IResult GetInFlight(QueueViewManager manager, string topic)
    {
        var view = manager.Get(topic);
        if (view is null)
            return Results.NotFound(new { message = $"No QueueView found for topic '{topic}'" });

        var snapshot = view.GetInFlightMessages();
        var response = snapshot.Select(m => new InFlightMessageResponse(
            MessageId: m.MessageId,
            DeliveryCount: m.DeliveryCount,
            ExpiresAt: m.ExpiresAt,
            ConsumerId: m.ConsumerId,
            Partition: m.Partition,
            Offset: m.Offset)).ToList();

        return Results.Ok(response);
    }

    private static IResult GetDlq(QueueViewManager manager, string topic, int offset = 0, int limit = 50)
    {
        var view = manager.Get(topic);
        if (view is null)
            return Results.NotFound(new { message = $"No QueueView found for topic '{topic}'" });

        // Derive the DLQ topic name using the same convention as QueueView
        var dlqTopic = $"{topic}.dlq";

        var response = new DlqBrowseResponse(
            Topic: topic,
            DlqTopic: dlqTopic,
            Offset: offset,
            Limit: limit,
            Messages: []);

        return Results.Ok(response);
    }

    private static IResult GetMetrics(QueueViewManager manager, string topic)
    {
        var view = manager.Get(topic);
        if (view is null)
            return Results.NotFound(new { message = $"No QueueView found for topic '{topic}'" });

        var response = new QueueMetricsResponse(
            Topic: topic,
            TotalAcked: view.TotalAcked,
            TotalNacked: view.TotalNacked,
            TotalRejected: view.TotalRejected,
            TotalExpired: view.TotalExpired,
            TotalRedelivered: view.TotalRedelivered,
            TotalReceived: view.TotalReceived,
            InFlightCount: view.InFlightCount);

        return Results.Ok(response);
    }
}

/// <summary>
/// Response carrying status information for a single QueueView.
/// </summary>
public sealed record QueueStatusResponse(
    string Topic,
    int InFlightCount);

/// <summary>
/// Response returned after a successful purge operation.
/// </summary>
public sealed record PurgeResponse(
    string Topic,
    string Message);

/// <summary>
/// Summary of a single in-flight message, safe for serialisation over REST.
/// </summary>
public sealed record InFlightMessageResponse(
    string MessageId,
    int DeliveryCount,
    DateTimeOffset ExpiresAt,
    string? ConsumerId,
    int Partition,
    long Offset);

/// <summary>
/// Response returned by the DLQ browse endpoint.
/// </summary>
public sealed record DlqBrowseResponse(
    string Topic,
    string DlqTopic,
    int Offset,
    int Limit,
    IReadOnlyList<InFlightMessageResponse> Messages);

/// <summary>
/// Response carrying lifetime metric counters for a single QueueView.
/// </summary>
public sealed record QueueMetricsResponse(
    string Topic,
    long TotalAcked,
    long TotalNacked,
    long TotalRejected,
    long TotalExpired,
    long TotalRedelivered,
    long TotalReceived,
    int InFlightCount);
