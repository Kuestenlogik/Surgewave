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
    /// Send a request and receive a response whose payload may be borrowed from a pool (#80).
    ///
    /// <para><b>This is the allocation-free read path.</b> <see cref="SendRequestAsync"/> must give
    /// the caller memory it owns forever, so an uncompressed response costs one array the size of
    /// the payload — on a fetch, the client's largest source of garbage. Here the caller returns
    /// the buffer instead, by disposing the lease once it has decoded or copied what it needs.</para>
    ///
    /// <para>The default implementation delegates to <see cref="SendRequestAsync"/> and yields a
    /// lease that owns nothing, so every existing transport keeps working unchanged and callers can
    /// use this path unconditionally. Transports override it to serve from their own pooled reads.</para>
    /// </summary>
    async ValueTask<SurgewaveResponseLease> SendRequestLeasedAsync(
        SurgewaveOpCode opCode,
        ReadOnlyMemory<byte> payload,
        bool compress = true,
        CancellationToken cancellationToken = default)
    {
        var (header, responsePayload) = await SendRequestAsync(opCode, payload, compress, cancellationToken).ConfigureAwait(false);
        return new SurgewaveResponseLease(header, responsePayload);
    }

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
