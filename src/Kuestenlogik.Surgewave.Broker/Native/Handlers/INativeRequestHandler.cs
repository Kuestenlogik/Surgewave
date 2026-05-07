using Kuestenlogik.Surgewave.Broker.Native.Streaming;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Interface for native protocol request handlers.
/// Each handler is responsible for a specific set of related operations.
/// </summary>
public interface INativeRequestHandler
{
    /// <summary>
    /// The operation codes this handler can process.
    /// </summary>
    IEnumerable<SurgewaveOpCode> SupportedOpCodes { get; }

    /// <summary>
    /// Handle a native protocol request.
    /// </summary>
    /// <param name="context">The request context containing stream, header, and connection state.</param>
    /// <param name="payload">The request payload bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task HandleAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}

/// <summary>
/// Context passed to native request handlers containing all necessary state.
/// Write destination (PipeWriter/Stream) is captured in the callback closures.
/// </summary>
public sealed class NativeRequestContext
{
    public required SurgewaveRequestHeader Header { get; init; }
    public required BrokerConfig Config { get; init; }
    public required Func<uint, SurgewaveOpCode, SurgewaveErrorCode, ReadOnlyMemory<byte>, CancellationToken, Task> SendResponseAsync { get; init; }
    public required Func<uint, SurgewaveOpCode, SurgewaveErrorCode, string, CancellationToken, Task> SendErrorAsync { get; init; }
    public bool ClientSupportsCompression { get; init; }

    /// <summary>
    /// Per-connection subscription manager for push streaming.
    /// Null if streaming is not enabled or the connection hasn't been upgraded.
    /// </summary>
    public StreamSubscriptionManager? SubscriptionManager { get; init; }
}
