using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Transport layer abstraction for Surgewave client-broker communication.
/// Enables pluggable transports: TCP, SharedMemory, etc.
/// </summary>
public interface ISurgewaveTransport : IAsyncDisposable
{
    /// <summary>
    /// The type of transport.
    /// </summary>
    SurgewaveTransportType TransportType { get; }

    /// <summary>
    /// Whether the transport is currently connected.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// Whether the server supports compression.
    /// </summary>
    bool ServerSupportsCompression { get; }

    /// <summary>
    /// Connect to the broker.
    /// </summary>
    ValueTask ConnectAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Send a request and receive a response.
    /// </summary>
    /// <param name="opCode">The operation code.</param>
    /// <param name="payload">The request payload.</param>
    /// <param name="compress">Whether to compress the payload if supported.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The response header and payload.</returns>
    ValueTask<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> SendRequestAsync(
        SurgewaveOpCode opCode,
        ReadOnlyMemory<byte> payload,
        bool compress = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Register a handler for unsolicited server-push messages identified by op-code.
    /// Push messages arrive with RequestId == 0 and are dispatched to the handler
    /// instead of completing a pending request.
    /// </summary>
    void RegisterPushHandler(SurgewaveOpCode opCode, Func<SurgewaveResponseHeader, ReadOnlyMemory<byte>, Task> handler);

    /// <summary>
    /// Remove a previously registered push handler.
    /// </summary>
    void UnregisterPushHandler(SurgewaveOpCode opCode);
}
