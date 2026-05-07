using Kuestenlogik.Surgewave.Protocol.Kafka;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Interface for Kafka request handlers.
/// Handlers process specific types of Kafka API requests and return responses.
/// </summary>
public interface IKafkaRequestHandler
{
    /// <summary>
    /// Gets the API keys that this handler can process.
    /// </summary>
    IEnumerable<ApiKey> SupportedApiKeys { get; }

    /// <summary>
    /// Process a Kafka request and return a response.
    /// </summary>
    /// <param name="request">The Kafka request to process</param>
    /// <param name="context">The request context with connection state and dependencies</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The response to send to the client</returns>
    Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken);
}

/// <summary>
/// Context for request processing, providing access to connection state and shared dependencies.
/// </summary>
public sealed class RequestContext
{
    public required Security.ConnectionState ConnectionState { get; init; }
    public required string ClientId { get; init; }
}
