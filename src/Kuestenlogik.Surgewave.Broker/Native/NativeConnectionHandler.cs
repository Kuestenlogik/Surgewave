using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native;

/// <summary>
/// Connection handler for the Surgewave native protocol. Registered at
/// <see cref="Order"/> 0 so its specific magic-byte match is checked before any
/// fallback (e.g. Kafka). The peeked magic bytes were already consumed off the
/// stream by the accept loop, so this handler ignores the passed-in copy and
/// hands the stream straight to the native pipeline (#59).
/// </summary>
internal sealed class NativeConnectionHandler : IConnectionHandler
{
    private readonly SurgewaveNativeHandler _nativeHandler;

    public NativeConnectionHandler(SurgewaveNativeHandler nativeHandler)
        => _nativeHandler = nativeHandler;

    public int Order => 0;

    public bool CanHandle(ReadOnlySpan<byte> magic) => magic.SequenceEqual(SurgewaveNativeProtocol.Magic);

    public Task HandleConnectionAsync(
        Stream stream,
        ReadOnlyMemory<byte> magic,
        ConnectionContext context,
        CancellationToken cancellationToken)
        => _nativeHandler.HandleConnectionAsync(stream, cancellationToken);
}
