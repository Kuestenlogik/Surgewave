using System.Buffers;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Writes native-protocol request frames. Payloads up to <see cref="MaxCoalescedPayloadBytes"/>
/// are coalesced with the 12-byte header into one pooled buffer so the frame leaves in a single
/// <see cref="Stream.WriteAsync(ReadOnlyMemory{byte}, CancellationToken)"/> — one syscall and,
/// with TCP_NODELAY, one segment instead of a 12-byte runt packet followed by the payload.
/// Larger payloads keep the classic two-write path: copying them would cost more than the saved
/// syscall.
/// Buffer ownership: the coalescing buffer is rented from <see cref="ArrayPool{T}.Shared"/> and
/// returned inside this method after the awaited write completes; it never escapes. The
/// caller-supplied scratch header buffer and the payload are only read. Callers must serialize
/// invocations per stream (TcpTransport/QuicTransport hold their send lock).
/// </summary>
public static class NativeRequestFrameWriter
{
    /// <summary>
    /// Cap chosen so header + payload stay within the 64 KiB ArrayPool bucket and match the
    /// transports' 64 KiB send-buffer default.
    /// </summary>
    public const int MaxCoalescedPayloadBytes = 64 * 1024 - SurgewaveNativeProtocol.HeaderSize;

    public static async ValueTask WriteAsync(
        Stream stream,
        SurgewaveRequestHeader header,
        ReadOnlyMemory<byte> payload,
        byte[] scratchHeaderBuffer,
        CancellationToken cancellationToken)
    {
        if (payload.Length > 0 && payload.Length <= MaxCoalescedPayloadBytes)
        {
            var totalLength = SurgewaveNativeProtocol.HeaderSize + payload.Length;
            var buffer = ArrayPool<byte>.Shared.Rent(totalLength);
            try
            {
                header.WriteTo(buffer);
                payload.Span.CopyTo(buffer.AsSpan(SurgewaveNativeProtocol.HeaderSize));
                await stream.WriteAsync(buffer.AsMemory(0, totalLength), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        else
        {
            header.WriteTo(scratchHeaderBuffer);
            await stream.WriteAsync(scratchHeaderBuffer.AsMemory(0, SurgewaveNativeProtocol.HeaderSize), cancellationToken).ConfigureAwait(false);
            if (payload.Length > 0)
            {
                await stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
