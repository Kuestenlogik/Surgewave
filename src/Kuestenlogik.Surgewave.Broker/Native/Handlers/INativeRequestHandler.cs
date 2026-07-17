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
    /// <param name="context">
    /// The request context. It is reused across requests on a connection — see
    /// <see cref="NativeRequestContext"/> before holding on to it.
    /// </param>
    /// <param name="payload">The request payload bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task HandleAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}
