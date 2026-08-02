using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Transport;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Semantics of the allocation-free request/response correlation (#80). These pin the two
/// failure modes that make a pooled <see cref="IValueTaskSource{TResult}"/> dangerous: a lost
/// completion (the awaiter hangs forever) and a double completion (a response delivered to an
/// unrelated caller after recycling).
/// </summary>
public class PendingResponseTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    private static SurgewaveResponseLease Response(uint requestId)
        => new(new SurgewaveResponseHeader
        {
            RequestId = requestId,
            OpCode = SurgewaveOpCode.Fetch,
            ErrorCode = SurgewaveErrorCode.None,
            PayloadLength = 1
        }, new byte[] { (byte)requestId });

    [Fact]
    public async Task Result_SetBeforeAwait_IsObserved()
    {
        var pending = new PendingResponse();
        Assert.True(pending.TrySetResult(Response(7)));

        var response = await pending.ValueTask.AsTask().WaitAsync(Timeout);

        Assert.Equal(7u, response.Header.RequestId);
        Assert.Equal(7, response.Payload.Span[0]);
    }

    [Fact]
    public async Task Result_SetAfterAwaitStarted_CompletesTheAwaiter()
    {
        var pending = new PendingResponse();
        var awaiter = pending.ValueTask.AsTask();

        Assert.False(awaiter.IsCompleted);
        Assert.True(pending.TrySetResult(Response(3)));

        var response = await awaiter.WaitAsync(Timeout);
        Assert.Equal(3u, response.Header.RequestId);
    }

    [Fact]
    public async Task Exception_IsPropagatedToTheAwaiter()
    {
        var pending = new PendingResponse();
        var awaiter = pending.ValueTask.AsTask();

        Assert.True(pending.TrySetException(new IOException("connection lost")));

        var ex = await Assert.ThrowsAsync<IOException>(() => awaiter.WaitAsync(Timeout));
        Assert.Equal("connection lost", ex.Message);
    }

    [Fact]
    public async Task Cancellation_IsPropagatedToTheAwaiter()
    {
        var pending = new PendingResponse();
        var awaiter = pending.ValueTask.AsTask();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        Assert.True(pending.TrySetCanceled(cts.Token));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => awaiter.WaitAsync(Timeout));
    }

    [Fact]
    public async Task SecondCompletion_IsRejected_AndDoesNotOverwriteTheFirst()
    {
        var pending = new PendingResponse();

        Assert.True(pending.TrySetResult(Response(1)));
        // Whoever lost the pending-map race must not be able to clobber the delivered result —
        // nor throw, which ManualResetValueTaskSourceCore would do on a second SetResult.
        Assert.False(pending.TrySetResult(Response(2)));
        Assert.False(pending.TrySetException(new IOException("late")));
        Assert.False(pending.TrySetCanceled(new CancellationToken(true)));

        var delivered = await pending.ValueTask.AsTask().WaitAsync(Timeout);
        Assert.Equal(1u, delivered.Header.RequestId);
    }

    [Fact]
    public async Task Reset_AllowsReuse_AndTheNewUseIsIndependent()
    {
        var pending = new PendingResponse();

        pending.TrySetResult(Response(1));
        var first = await pending.ValueTask.AsTask().WaitAsync(Timeout);
        Assert.Equal(1u, first.Header.RequestId);

        pending.Reset();

        // A recycled instance must accept a completion again (the guard has to be cleared)
        // and hand out the new result, not the stale one.
        Assert.True(pending.TrySetResult(Response(2)));
        var second = await pending.ValueTask.AsTask().WaitAsync(Timeout);
        Assert.Equal(2u, second.Header.RequestId);
        Assert.Equal(2, second.Payload.Span[0]);
    }

    [Fact]
    public void Reset_InvalidatesTheOldToken()
    {
        var pending = new PendingResponse();
        var staleToken = pending.Version;

        pending.TrySetResult(Response(1));
        pending.Reset();

        // Reading through a token from the previous use must fail loudly rather than return
        // another caller's response.
        Assert.Throws<InvalidOperationException>(() => pending.GetResult(staleToken));
    }

    [Fact]
    public async Task ConcurrentCompleters_ExactlyOneWins_AndTheResultIsConsistent()
    {
        for (int round = 0; round < 200; round++)
        {
            var pending = new PendingResponse();
            var start = new ManualResetEventSlim(false);
            var wins = 0;

            var contenders = Enumerable.Range(1, 4).Select(i => Task.Run(() =>
            {
                start.Wait();
                if (pending.TrySetResult(Response((uint)i)))
                    Interlocked.Increment(ref wins);
            })).ToArray();

            start.Set();
            await Task.WhenAll(contenders).WaitAsync(Timeout);

            Assert.Equal(1, wins);
            var response = await pending.ValueTask.AsTask().WaitAsync(Timeout);
            // The delivered header and payload must come from the same completer.
            Assert.Equal((byte)response.Header.RequestId, response.Payload.Span[0]);
        }
    }

    /// <summary>
    /// The point of the pooled source (#80): a recycled correlation costs no allocation, where the
    /// previous TaskCompletionSource-per-request path allocated on every single request. Measured
    /// on one thread with the completion consumed synchronously, so nothing but the correlation
    /// itself is on the account.
    /// </summary>
    [Fact]
    public void RecycledCorrelation_AllocatesNothing_UnlikeATaskCompletionSourcePerRequest()
    {
        const int iterations = 1000;
        var pending = new PendingResponse();
        var response = Response(1);

        // Warm up: first use JITs the paths and may allocate.
        pending.TrySetResult(response);
        _ = pending.GetResult(pending.Version);
        pending.Reset();

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++)
        {
            pending.TrySetResult(response);
            _ = pending.GetResult(pending.Version);
            pending.Reset();
        }
        var pooledBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++)
        {
            var tcs = new TaskCompletionSource<SurgewaveResponseLease>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            tcs.TrySetResult(response);
        }
        var tcsBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(0, pooledBytes);
        Assert.True(tcsBytes > 0, "baseline did not allocate — the comparison would be meaningless");
    }

    [Fact]
    public async Task ContinuationsRunAsynchronously_SoAReaderLoopIsNeverBlocked()
    {
        var pending = new PendingResponse();
        var continuationBlocked = new ManualResetEventSlim(false);
        var continuationEntered = new ManualResetEventSlim(false);

        var awaiter = Task.Run(async () =>
        {
            await pending.ValueTask;
            continuationEntered.Set();
            // A caller doing slow work in its continuation. Bounded so an inline continuation
            // delays the completer measurably instead of deadlocking the test.
            continuationBlocked.Wait(TimeSpan.FromMilliseconds(400));
        });

        await Task.Delay(50); // let the awaiter subscribe

        // Completing is what the reader loop does for every response. If the continuation ran
        // inline, one slow caller would stall every other in-flight response on the connection.
        var start = System.Diagnostics.Stopwatch.StartNew();
        Assert.True(pending.TrySetResult(Response(1)));
        start.Stop();

        continuationBlocked.Set();
        await awaiter.WaitAsync(Timeout);

        Assert.True(continuationEntered.IsSet, "the continuation never ran");
        Assert.True(start.ElapsedMilliseconds < 200,
            $"completing blocked the caller for {start.ElapsedMilliseconds} ms — the continuation ran inline");
    }
}
