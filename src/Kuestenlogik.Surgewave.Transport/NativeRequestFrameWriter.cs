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

    /// <summary>
    /// Writes the frame, giving up after <paramref name="timeout"/> if it makes no progress (#117).
    ///
    /// <para><b>Why waiting has to be bounded separately from cancellation.</b> A socket send that
    /// has already started cannot be cancelled — the token is only observed before each write. When
    /// the peer stops draining its receive buffer, the write blocks and no token in the world ends
    /// that wait; the caller hangs on a connection that will never progress. This method stops
    /// waiting instead, so the transport can tear the socket down, which is the only thing that
    /// actually releases the blocked send.</para>
    ///
    /// <para><b>The caller's token deliberately does not abort the wait.</b> Once a frame has begun,
    /// abandoning it is what desynchronises the peer. A cancellation that arrives mid-frame is
    /// therefore honoured after the frame is out (or after the deadline) rather than by killing a
    /// connection that is merely slow. Cancelling before the first byte stays free — the caller
    /// checks the token itself before calling in.</para>
    ///
    /// <para>The write that is left behind on a deadline keeps running until the socket dies; its
    /// exception is observed here so it cannot surface as an unobserved task exception.</para>
    /// </summary>
    /// <exception cref="TimeoutException">The frame did not reach the peer within the deadline.</exception>
    public static ValueTask WriteWithDeadlineAsync(
        Stream stream,
        SurgewaveRequestHeader header,
        ReadOnlyMemory<byte> payload,
        byte[] scratchHeaderBuffer,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var write = WriteAsync(stream, header, payload, scratchHeaderBuffer, cancellationToken);

        // The common case: the kernel accepted the frame straight away. No Task, no timer, nothing
        // on the hot path.
        if (write.IsCompletedSuccessfully || timeout == Timeout.InfiniteTimeSpan)
        {
            return write;
        }

        return AwaitWithDeadlineAsync(write.AsTask(), timeout);
    }

    private static async ValueTask AwaitWithDeadlineAsync(Task write, TimeSpan timeout)
    {
        try
        {
            await write.WaitAsync(timeout, CancellationToken.None).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // We stop waiting, but the send is still in the kernel's hands and only ends when the
            // socket is torn down. Observe whatever it throws then.
            _ = write.ContinueWith(
                static abandoned => _ = abandoned.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            throw;
        }
    }

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
