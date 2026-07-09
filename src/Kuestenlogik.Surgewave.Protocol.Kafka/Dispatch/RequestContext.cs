namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Context for request processing, providing access to connection state and shared dependencies.
/// </summary>
public sealed class RequestContext
{
    public required ConnectionState ConnectionState { get; init; }
    public required string ClientId { get; init; }
}
