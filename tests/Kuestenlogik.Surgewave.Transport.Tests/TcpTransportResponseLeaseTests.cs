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

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
        {
            using var response = await transport.SendRequestLeasedAsync(SurgewaveOpCode.Fetch, request, compress: false);
            Assert.Equal(request.Length, response.Payload.Length);
        }
        var leasedBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
        {
            var (_, payload) = await transport.SendRequestAsync(SurgewaveOpCode.Fetch, request, compress: false);
            Assert.Equal(request.Length, payload.Length);
        }
        var materializedBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(materializedBytes > iterations * (long)request.Length * 0.9,
            $"the materializing path allocated only {materializedBytes} B — the comparison would be meaningless");
        Assert.True(leasedBytes < materializedBytes / 4,
            $"leased path allocated {leasedBytes} B vs {materializedBytes} B materialized — the payload is not being borrowed");
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
    public async Task CancelledRequest_DoesNotStrandTheBuffer_AndLaterRequestsStillMatch()
    {
        // Cancellation is the case where nobody consumes the response: the reader loop has to
        // release the lease itself, and the connection has to keep working afterwards.
        await using var server = new EchoNativeServer();
        await using var transport = new TcpTransport(OptionsFor(server.Port, pipelining: true));
        await transport.ConnectAsync().AsTask().WaitAsync(Timeout);

        for (var i = 0; i < 20; i++)
        {
            using var cts = new CancellationTokenSource();
            await cts.CancelAsync();

            var request = new byte[2048];
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                using var _ = await transport.SendRequestLeasedAsync(
                    SurgewaveOpCode.Fetch, request, compress: false, cts.Token);
            });
        }

        var probe = new byte[2048];
        Array.Fill(probe, (byte)0x77);
        using var response = await transport.SendRequestLeasedAsync(SurgewaveOpCode.Fetch, probe, compress: false)
            .AsTask().WaitAsync(Timeout);

        Assert.True(probe.AsSpan().SequenceEqual(response.Payload.Span));
    }
}
