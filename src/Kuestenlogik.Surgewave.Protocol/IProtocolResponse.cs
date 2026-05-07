namespace Kuestenlogik.Surgewave.Protocol;

/// <summary>
/// Base interface for all protocol responses
/// </summary>
public interface IProtocolResponse
{
    /// <summary>
    /// Correlation ID to match responses with requests
    /// </summary>
    int CorrelationId { get; }

    /// <summary>
    /// Serialize the response to binary format
    /// </summary>
    byte[] Serialize();
}
