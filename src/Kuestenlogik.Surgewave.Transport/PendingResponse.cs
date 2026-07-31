using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Correlates one pipelined request with its response without allocating a
/// <see cref="TaskCompletionSource{TResult}"/> per request (#80): the awaiter is a
/// <see cref="ValueTask{TResult}"/> backed by a pooled <see cref="IValueTaskSource{TResult}"/>.
///
/// <para><b>Ownership contract.</b> The instance is registered in the transport's pending-request
/// map before the frame is written. Exactly one party may complete it, and the winner is decided
/// by who removes it from that map:</para>
/// <list type="bullet">
/// <item>The reader loop removes it, then calls <see cref="TrySetResult"/> / <see cref="TrySetException"/>.</item>
/// <item>The waiter removes it (on cancellation), then calls <see cref="TrySetCanceled"/>.</item>
/// </list>
/// <para>The loser of that race must not complete it and must not recycle it. An instance may only
/// be returned to a pool after its result has actually been consumed — recycling one that a reader
/// might still complete would deliver a response to an unrelated caller.</para>
/// </summary>
public sealed class PendingResponse : IValueTaskSource<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)>
{
    private ManualResetValueTaskSourceCore<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> _core = new()
    {
        // Never run a caller's continuation on the reader loop: one slow continuation would
        // stall every other in-flight response on the connection.
        RunContinuationsAsynchronously = true
    };

    // 0 = pending, 1 = a completion has been committed. Guards ManualResetValueTaskSourceCore,
    // whose SetResult/SetException throw when invoked twice.
    private int _completed;

    /// <summary>Token identifying the current use; changes on every <see cref="Reset"/>.</summary>
    public short Version => _core.Version;

    /// <summary>The awaitable for the current use. Await it exactly once.</summary>
    public ValueTask<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> ValueTask
        => new(this, _core.Version);

    public bool TrySetResult((SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload) result)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0) return false;
        _core.SetResult(result);
        return true;
    }

    public bool TrySetException(Exception exception)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0) return false;
        _core.SetException(exception);
        return true;
    }

    public bool TrySetCanceled(CancellationToken cancellationToken)
        => TrySetException(new OperationCanceledException(cancellationToken));

    /// <summary>
    /// Prepares the instance for another request. Only call this once the previous result has
    /// been consumed and no reader can still reference the instance.
    /// </summary>
    public void Reset()
    {
        _core.Reset();
        Volatile.Write(ref _completed, 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public (SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload) GetResult(short token)
        => _core.GetResult(token);

    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(
        Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}
