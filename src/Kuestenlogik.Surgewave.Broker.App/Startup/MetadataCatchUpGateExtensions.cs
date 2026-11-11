using Kuestenlogik.Surgewave.Clustering.Replication;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// Refuses API requests over HTTP while the broker has not caught up with its metadata log (#250).
/// </summary>
/// <remarks>
/// The HTTP port is up from the start, because the liveness probe and the metrics live on it and a broker waiting for
/// a quorum is alive. Everything else on it — gRPC and the REST transcoded from it — reads the cluster's metadata, and
/// before the log is applied that is an empty state: a topic created before a restart would be reported as not there.
/// Such a request gets 503, which a gRPC client reads as UNAVAILABLE and retries; the Kafka and native listener is not
/// even bound until then.
/// </remarks>
public static class MetadataCatchUpGateExtensions
{
    /// <summary>
    /// Inserts the gate. Ahead of the endpoint mappings, so that it sees every request to them.
    /// </summary>
    public static void UseSurgewaveMetadataCatchUpGate(this WebApplication app, ClusterController controller) =>
        app.Use((context, next) => AnswerOrRefuseAsync(context, controller, next));

    /// <summary>
    /// Passes the request on when the broker has caught up or the request needs no metadata, and refuses it with 503
    /// otherwise.
    /// </summary>
    internal static async Task AnswerOrRefuseAsync(HttpContext context, ClusterController controller, RequestDelegate next)
    {
        if (controller.IsMetadataCaughtUp || IsAnsweredBeforeCatchUp(context.Request.Path))
        {
            await next(context);
            return;
        }

        context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
        context.Response.Headers.RetryAfter = "1";
        await context.Response.WriteAsJsonAsync(new
        {
            error = "The broker has not caught up with its metadata log yet and answers no API request until it has.",
        });
    }

    /// <summary>
    /// The paths that say how the broker itself is doing and need no metadata for it: the health probes and the
    /// metrics.
    /// </summary>
    internal static bool IsAnsweredBeforeCatchUp(PathString path) =>
        path.StartsWithSegments("/health") || path.StartsWithSegments("/metrics");
}
