using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Kuestenlogik.Surgewave.Transport.Tests.Fakes;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// The borrowed read path on the TCP transport (#80): an uncompressed response is served out of a
/// pooled buffer that the caller gives back, instead of an array the size of the payload.
///
/// <para>Correct bytes alone would prove nothing — a copying implementation returns the same
/// bytes. What separates the two is allocation, so that is what these tests measure, plus the
/// lifetime rules that make pooling safe in the first place.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TcpTransportResponseLeaseTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    private static TransportOptions OptionsFor(int port, bool pipelining) => new()
    {
        Host = "127.0.0.1",
        Port = port,
        EnablePipelining = pipelining,
        EnableCompression = false
    };

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LeasedResponse_CarriesThePayload_AndSurvivesRepeatedUse(bool pipelining)
    {
        await using var server = new EchoNativeServer();
        await using var transport = new TcpTransport(OptionsFor(server.Port, pipelining));
        await transport.ConnectAsync().AsTask().WaitAsync(Timeout);

        // A buffer released too early or handed out twice shows up here as mismatched bytes: each
        // round asks for a different payload while the previous rounds' buffers are back in the pool.
        for (var round = 0; round < 200; round++)
        {
            var request = new byte[512];
            Array.Fill(request, (byte)round);

            using var response = await transport.SendRequestLeasedAsync(
                SurgewaveOpCode.Fetch, request, compress: false).AsTask().WaitAsync(Timeout);

            Assert.Equal(SurgewaveErrorCode.None, response.Header.ErrorCode);
            Assert.Equal(request.Length, response.Payload.Length);
            Assert.True(request.AsSpan().SequenceEqual(response.Payload.Span));
        }
    }

    [Fact]
    public async Task LeasedResponse_DoesNotAllocatePerPayload_UnlikeTheMaterializingPath()
    {
        await using var server = new EchoNativeServer();
        await using var transport = new TcpTransport(OptionsFor(server.Port, pipelining: false));
        await transport.ConnectAsync().AsTask().WaitAsync(Timeout);

        // 64 KiB per response: large enough that a per-response array dominates whatever else the
        // two paths allocate, so the comparison cannot be explained by noise.
        var request = new byte[64 * 1024];
        Random.Shared.NextBytes(request);

        // Warm up both paths — first use JITs them and grows the pool.
        (await transport.SendRequestLeasedAsync(SurgewaveOpCode.Fetch, request, compress: false)).Dispose();
        _ = await transport.SendRequestAsync(SurgewaveOpCode.Fetch, request, compress: false);

        const int iterations = 20;

        // Process-wide, not per-thread: the payload copy happens in the continuation after the
        // socket await, so it lands on a pool thread and a per-thread counter simply does not see it
        // — the earlier version of this test read 39 KB where 1.3 MB was allocated. This assembly
        // runs its tests serially (xunit.runner.json), so nothing else contributes here.
        var before = GC.GetTotalAllocatedBytes(precise: true);
        for (var i = 0; i < iterations; i++)
        {
            using var response = await transport.SendRequestLeasedAsync(SurgewaveOpCode.Fetch, request, compress: false);
            Assert.Equal(request.Length, response.Payload.Length);
        }
        var leasedBytes = GC.GetTotalAllocatedBytes(precise: true) - before;

        before = GC.GetTotalAllocatedBytes(precise: true);
        for (var i = 0; i < iterations; i++)
        {
            var (_, payload) = await transport.SendRequestAsync(SurgewaveOpCode.Fetch, request, compress: false);
            Assert.Equal(request.Length, payload.Length);
        }
        var materializedBytes = GC.GetTotalAllocatedBytes(precise: true) - before;

        Assert.True(materializedBytes > 0 && leasedBytes > 0,
            $"allocation measurement broke (leased {leasedBytes} B, materialized {materializedBytes} B) — a zero or negative reading means the counter did not observe the work");

        // The echo server runs in this process and allocates a request and a response array per
        // round on BOTH paths, so the absolute figures are dominated by it. What separates the two
        // paths is the one payload-sized copy the materializing path has to make per response — so
        // that difference is the assertion, not the ratio.
        var extraPerResponse = (materializedBytes - leasedBytes) / (double)iterations;
        Assert.True(extraPerResponse > request.Length * 0.8,
            $"the materializing path allocated only {extraPerResponse:F0} B more per response than the leased one (expected about {request.Length} B) — the payload is not being borrowed");
    }

    [Fact]
    public async Task MaterializingPath_StillOwnsItsPayload_AfterLaterRequests()
    {
        // SendRequestAsync now copies out of the same pooled read. If it handed the pool buffer out
        // instead, this payload would change under the caller as soon as the buffer is reused.
        await using var server = new EchoNativeServer();
        await using var transport = new TcpTransport(OptionsFor(server.Port, pipelining: true));
        await transport.ConnectAsync().AsTask().WaitAsync(Timeout);

        var first = new byte[1024];
        Array.Fill(first, (byte)0xAB);
        var (_, payload) = await transport.SendRequestAsync(SurgewaveOpCode.Fetch, first, compress: false)
            .AsTask().WaitAsync(Timeout);

        for (var i = 0; i < 20; i++)
        {
            var noise = new byte[1024];
            Array.Fill(noise, (byte)0x5C);
            using var _ = await transport.SendRequestLeasedAsync(SurgewaveOpCode.Fetch, noise, compress: false)
                .AsTask().WaitAsync(Timeout);
        }

        Assert.True(first.AsSpan().SequenceEqual(payload.Span));
    }

    [Fact]
    public async Task CancelledRequest_WhoseResponseArrivesAnyway_ReleasesTheBufferInTheReaderLoop()
    {
        // The branch under test is the one where a response arrives for a waiter that is already
        // gone: only the reader loop can release that lease. Reaching it requires the request to be
        // ON THE WIRE before cancellation — cancelling beforehand merely aborts at the send lock and
        // never produces a response at all, which is why the server answers with a delay here.
        await using var server = new EchoNativeServer(responseDelay: TimeSpan.FromMilliseconds(300));
        await using var transport = new TcpTransport(OptionsFor(server.Port, pipelining: true));
        await transport.ConnectAsync().AsTask().WaitAsync(Timeout);

        // Small requests on purpose: a payload big enough to fill the socket send buffer would have
        // its write aborted MID-FRAME by the cancellation, leaving the connection desynchronised —
        // a different (and pre-existing) problem that would mask the branch under test here.
        for (var i = 0; i < 10; i++)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

            var request = new byte[1024];
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                using var _ = await transport.SendRequestLeasedAsync(
                    SurgewaveOpCode.Fetch, request, compress: false, cts.Token);
            });
        }

        // Give the orphaned responses time to arrive and be released by the reader loop.
        await Task.Delay(TimeSpan.FromSeconds(4));

        // The connection still works and serves correct bytes — a buffer released twice or handed to
        // the wrong waiter would show up here.
        var probe = new byte[1024];
        Array.Fill(probe, (byte)0x77);
        using var response = await transport.SendRequestLeasedAsync(SurgewaveOpCode.Fetch, probe, compress: false)
            .AsTask().WaitAsync(Timeout);

        Assert.True(probe.AsSpan().SequenceEqual(response.Payload.Span));
    }

    [Fact]
    public async Task DisposeWithRequestInFlight_FaultsTheWaiter_InsteadOfHangingForever()
    {
        // A caller that passed CancellationToken.None has no escape of its own: if the reader loop
        // exits without faulting the pending requests, that await never returns. Nothing exercised
        // this before — every other test consumes its response before disposing.
        await using var server = new EchoNativeServer(neverRespond: true);
        var transport = new TcpTransport(OptionsFor(server.Port, pipelining: true));
        await transport.ConnectAsync().AsTask().WaitAsync(Timeout);

        var inFlight = transport.SendRequestLeasedAsync(
            SurgewaveOpCode.Fetch, new byte[1024], compress: false, CancellationToken.None).AsTask();

        // Let the frame reach the server so the request is genuinely pending.
        await Task.Delay(100);
        Assert.False(inFlight.IsCompleted, "the server answered although it was told not to");

        await transport.DisposeAsync();

        await Assert.ThrowsAnyAsync<Exception>(() => inFlight.WaitAsync(Timeout));
    }

    [Fact]
    public async Task CancellingMidWrite_TearsTheConnectionDown_InsteadOfLeavingItSilentlyBroken()
    {
        // #117. The mechanism, measured rather than assumed: an in-flight socket send is NOT
        // cancellable (16 MiB went through in 13 ms with the token already tripped), so a frame is
        // never torn apart mid-write. It tears BETWEEN the two writes: a payload above
        // NativeRequestFrameWriter.MaxCoalescedPayloadBytes is sent as header-then-payload, and the
        // token is checked before each of them. Once the header write has to wait for a slow peer,
        // the token has long since fired by the time the payload write starts — that write throws
        // before sending a byte, and the peer is left counting payload bytes that never arrive.
        //
        // Before the fix the transport reported IsConnected and every later response vanished.
        await using var server = new EchoNativeServer(responseDelay: TimeSpan.FromMilliseconds(300));
        var transport = new TcpTransport(OptionsFor(server.Port, pipelining: true));
        await transport.ConnectAsync().AsTask().WaitAsync(Timeout);

        // A caller that cancelled nothing and is waiting when the connection dies under it.
        var bystander = transport.SendRequestLeasedAsync(
            SurgewaveOpCode.Fetch, new byte[512], compress: false, CancellationToken.None).AsTask();

        // Payload above the coalescing limit → the two-write path. The slow peer makes the header
        // write wait, so the cancellation lands between the writes.
        var oversized = new byte[NativeRequestFrameWriter.MaxCoalescedPayloadBytes + 4096];

        for (var i = 0; i < 20 && transport.IsConnected; i++)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
            try
            {
                using var _ = await transport.SendRequestLeasedAsync(
                    SurgewaveOpCode.Produce, oversized, compress: false, cts.Token);
            }
            catch (OperationCanceledException)
            {
                // expected — this is the caller's own cancellation
            }
        }

        Assert.False(transport.IsConnected,
            "the transport still claims to be connected although a frame was left half-written and the peer can no longer parse the stream");

        // The bystander fails instead of waiting forever, and with the error the client's consumer
        // recognises as a connection error so its reconnect path takes over.
        var bystanderFailure = await Assert.ThrowsAnyAsync<Exception>(() => bystander.WaitAsync(Timeout));
        Assert.True(bystanderFailure is IOException or ObjectDisposedException,
            $"in-flight request failed with {bystanderFailure.GetType().Name} — the consumer would not treat that as a connection error");

        // A later request fails fast rather than disappearing into a dead connection.
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            using var _ = await transport.SendRequestLeasedAsync(
                SurgewaveOpCode.Fetch, new byte[64], compress: false, CancellationToken.None)
                .AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        });

        await transport.DisposeAsync();
    }

    [Fact]
    public async Task CancellingBeforeTheFrameIsWritten_LeavesTheConnectionUsable()
    {
        // The counterpart: a request cancelled while it waits for the send lock has put nothing on
        // the wire, so tearing the connection down would be an over-reaction — that is the common
        // case whenever a client times out a queued request.
        await using var server = new EchoNativeServer();
        await using var transport = new TcpTransport(OptionsFor(server.Port, pipelining: true));
        await transport.ConnectAsync().AsTask().WaitAsync(Timeout);

        for (var i = 0; i < 10; i++)
        {
            using var cts = new CancellationTokenSource();
            await cts.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                using var _ = await transport.SendRequestLeasedAsync(
                    SurgewaveOpCode.Fetch, new byte[256], compress: false, cts.Token);
            });
        }

        Assert.True(transport.IsConnected);

        var probe = new byte[256];
        Array.Fill(probe, (byte)0x5A);
        using var response = await transport.SendRequestLeasedAsync(SurgewaveOpCode.Fetch, probe, compress: false)
            .AsTask().WaitAsync(Timeout);

        Assert.True(probe.AsSpan().SequenceEqual(response.Payload.Span));
    }

    [Fact]
    public async Task RequestIdWraps_TheReservedPushIdIsSkipped_SoTheResponseStillReachesItsWaiter()
    {
        // RequestId 0 is the wire's marker for a server push. The counter is 32-bit and wraps, so a
        // long-lived connection eventually reaches it; a request issued with id 0 would have its
        // response routed to the push path and the caller would wait forever. Forcing the counter to
        // the wrap point is the only way to reach that in a test.
        await using var server = new EchoNativeServer();
        await using var transport = new TcpTransport(OptionsFor(server.Port, pipelining: true));
        await transport.ConnectAsync().AsTask().WaitAsync(Timeout);

        var counter = typeof(TcpTransport).GetField("_requestIdCounter",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(counter);
        counter!.SetValue(transport, uint.MaxValue);

        var probe = new byte[256];
        Array.Fill(probe, (byte)0x42);

        // Without skipping the reserved value this await never completes.
        using var response = await transport.SendRequestLeasedAsync(SurgewaveOpCode.Fetch, probe, compress: false)
            .AsTask().WaitAsync(Timeout);

        Assert.NotEqual(0u, response.Header.RequestId);
        Assert.True(probe.AsSpan().SequenceEqual(response.Payload.Span));
    }
}
