namespace Kuestenlogik.Surgewave.Protocol;

/// <summary>
/// Base interface for all protocol requests
/// </summary>
public interface IProtocolRequest
{
    /// <summary>
    /// Correlation ID to match requests with responses
    /// </summary>
    int CorrelationId { get; }

    /// <summary>
    /// Client identifier
    /// </summary>
    string ClientId { get; }

    /// <summary>
    /// Serialize the request to binary format
    /// </summary>
    byte[] Serialize();
}
