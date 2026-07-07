using System.Collections.Frozen;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Fast O(1) request dispatcher using frozen dictionary lookup.
/// No reflection - handlers are registered at startup and lookup is a simple dictionary access.
/// </summary>
public sealed class RequestDispatcher
{
    private readonly FrozenDictionary<ApiKey, IKafkaRequestHandler> _handlers;
    private readonly ILogger<RequestDispatcher>? _logger;

    public RequestDispatcher(IEnumerable<IKafkaRequestHandler> handlers, ILogger<RequestDispatcher>? logger = null)
    {
        _logger = logger;
        var handlerMap = new Dictionary<ApiKey, IKafkaRequestHandler>();

        foreach (var handler in handlers)
        {
            foreach (var apiKey in handler.SupportedApiKeys)
            {
                handlerMap[apiKey] = handler;
            }
        }

        // Freeze for optimal read performance - no further modifications possible
        _handlers = handlerMap.ToFrozenDictionary();
    }

    /// <summary>
    /// Dispatch a request to the appropriate handler.
    /// O(1) lookup, no reflection, no allocation.
    /// </summary>
    public Task<KafkaResponse> DispatchAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        _logger?.LogDebug("Dispatching request: ApiKey={ApiKey}, CorrelationId={CorrelationId}",
            request.ApiKey, request.CorrelationId);

        if (_handlers.TryGetValue(request.ApiKey, out var handler))
        {
            return handler.HandleAsync(request, context, cancellationToken);
        }

        // Return a minimal error response for unsupported API keys rather than throwing.
        // Confluent.Kafka 2.14+ sends GetTelemetrySubscriptions (KIP-714) and other new
        // API keys that Surgewave does not implement yet. Throwing here would kill the
        // connection and surface as "Broker transport failure" on the client — which
        // masks the real problem and breaks unrelated subsequent requests on the same
        // socket. A clean error response lets the client handle the unsupported-API
        // gracefully (log + skip) without tearing down the connection.
        _logger?.LogDebug("No handler for API key {ApiKey} — returning UnsupportedVersion error response", request.ApiKey);
        return Task.FromResult<KafkaResponse>(new UnsupportedApiKeyResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
        });
    }

    /// <summary>
    /// Check if a handler is registered for the given API key.
    /// </summary>
    public bool HasHandler(ApiKey apiKey) => _handlers.ContainsKey(apiKey);

    /// <summary>
    /// Return a NEW dispatcher containing this dispatcher's handlers plus the
    /// given one (its ApiKeys win on overlap). Used at startup to add the
    /// inter-broker handler once the cluster components exist, since the frozen
    /// map cannot be mutated in place (#69).
    /// </summary>
    public RequestDispatcher WithAdditionalHandler(IKafkaRequestHandler handler)
    {
        var handlers = _handlers.Values.Distinct().Append(handler);
        return new RequestDispatcher(handlers, _logger);
    }
}
