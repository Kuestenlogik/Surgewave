namespace Kuestenlogik.Surgewave.Protocol;

/// <summary>
/// Context for protocol operations, providing access to broker resources
/// </summary>
public interface IProtocolContext
{
    /// <summary>
    /// Execute a request and get a response
    /// </summary>
    Task<IProtocolResponse> ExecuteAsync(IProtocolRequest request, CancellationToken cancellationToken = default);
}
