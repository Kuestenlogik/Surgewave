using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Transport.Quic;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Kuestenlogik.Surgewave.Transport.Tests.Fakes;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// A client fails what was pending on a connection that has gone (#281): every request that waits for an answer
/// when the broker's end closes or is reset completes with a failure that says so, and so does every request
/// sent on that connection afterwards.
/// </summary>
/// <remarks>
/// Run over both transports the native client has, TCP and QUIC, and in both of their modes: pipelined, where a
/// reader task hands answers to the requests waiting for them, and one request at a time, where the caller reads
/// its own answer. The server reads every request and answers none, and a test drops the connection once the
/// server has the requests: a request that waits is on the broker's side by then, not on its way. The bound on a
/// wait is how long a test may take to fail.
/// </remarks>
public sealed class ConnectionLossTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    public static TheoryData<SurgewaveTransportType, bool, ConnectionDrop> EveryTransportModeAndDrop
    {
        get
        {
            var data = new TheoryData<SurgewaveTransportType, bool, ConnectionDrop>();
            foreach (var type in new[] { SurgewaveTransportType.Tcp, SurgewaveTransportType.Quic })
            {
                foreach (var pipelining in new[] { true, false })
                {
                    foreach (var drop in new[] { ConnectionDrop.Closed, ConnectionDrop.Reset })
                        data.Add(type, pipelining, drop);
                }
            }
            return data;
        }
    }

    public static TheoryData<SurgewaveTransportType, ConnectionDrop> EveryTransportAndDrop => new()
    {
        { SurgewaveTransportType.Tcp, ConnectionDrop.Closed },
        { SurgewaveTransportType.Tcp, ConnectionDrop.Reset },
        { SurgewaveTransportType.Quic, ConnectionDrop.Closed },
        { SurgewaveTransportType.Quic, ConnectionDrop.Reset },
    };

    [Theory]
    [MemberData(nameof(EveryTransportModeAndDrop))]
    public async Task ARequestWaitingForItsAnswerFailsWhenTheConnectionGoes(
        SurgewaveTransportType type, bool pipelining, ConnectionDrop drop)
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return;
        SkipWithout(type);
        var ct = TestContext.Current.CancellationToken;
        await using var server = await DroppingNativeServer.StartAsync(type, ct);
        await using var transport = await ConnectAsync(type, server.Port, pipelining);

        var request = transport.SendRequestAsync(SurgewaveOpCode.Fetch, new byte[64], compress: false, ct).AsTask();
        await server.HasReadAsync(1).WaitAsync(Bound, ct);
        await server.DropAsync(drop);

        var failure = await Assert.ThrowsAnyAsync<IOException>(() => request.WaitAsync(Bound, ct));
        AssertSaysTheConnectionWent(failure, server.Port);
        Assert.False(transport.IsConnected);
    }

    /// <summary>The batch a producer has in flight: every request of it fails, none is left waiting.</summary>
    [Theory]
    [MemberData(nameof(EveryTransportAndDrop))]
    public async Task EveryRequestOfAPipelinedBatchFailsWhenTheConnectionGoes(SurgewaveTransportType type, ConnectionDrop drop)
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return;
        SkipWithout(type);
        var ct = TestContext.Current.CancellationToken;
        await using var server = await DroppingNativeServer.StartAsync(type, ct);
        await using var transport = await ConnectAsync(type, server.Port, pipelining: true);

        const int inFlight = 64;
        var requests = Enumerable.Range(0, inFlight)
            .Select(i => transport.SendRequestLeasedAsync(SurgewaveOpCode.ProduceBatch, new byte[256 + i], compress: false, ct).AsTask())
            .ToArray();
        await server.HasReadAsync(inFlight).WaitAsync(Bound, ct);
        await server.DropAsync(drop);

        foreach (var request in requests)
        {
            var failure = await Assert.ThrowsAnyAsync<IOException>(() => request.WaitAsync(Bound, ct));
            AssertSaysTheConnectionWent(failure, server.Port);
        }
    }

    /// <summary>
    /// A request sent once the connection has gone is not written into a socket nobody reads: it fails as the
    /// requests before it did. The first request's failure is the sign that the client has seen the connection go.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryTransportModeAndDrop))]
    public async Task ARequestSentAfterTheConnectionWentFailsToo(SurgewaveTransportType type, bool pipelining, ConnectionDrop drop)
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return;
        SkipWithout(type);
        var ct = TestContext.Current.CancellationToken;
        await using var server = await DroppingNativeServer.StartAsync(type, ct);
        await using var transport = await ConnectAsync(type, server.Port, pipelining);

        var first = transport.SendRequestAsync(SurgewaveOpCode.Fetch, new byte[64], compress: false, ct).AsTask();
        await server.HasReadAsync(1).WaitAsync(Bound, ct);
        await server.DropAsync(drop);
        await Assert.ThrowsAnyAsync<IOException>(() => first.WaitAsync(Bound, ct));

        for (var i = 0; i < 3; i++)
        {
            var later = transport.SendRequestAsync(SurgewaveOpCode.Fetch, new byte[64], compress: false, ct).AsTask();
            var failure = await Assert.ThrowsAnyAsync<IOException>(() => later.WaitAsync(Bound, ct));
            AssertSaysTheConnectionWent(failure, server.Port);
        }
    }

    /// <summary>
    /// With one request at a time, a request cancelled while its answer is awaited ends the connection (#289): its
    /// answer comes all the same, and the next request read it as its own ("Request ID mismatch"), or waited behind
    /// it. The server here never answers, so the next request waited with no end.
    /// </summary>
    [Theory]
    [InlineData(SurgewaveTransportType.Tcp)]
    [InlineData(SurgewaveTransportType.Quic)]
    public async Task ARequestCancelledWhileItsAnswerIsAwaitedEndsAOneAtATimeConnection(SurgewaveTransportType type)
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return;
        SkipWithout(type);
        var ct = TestContext.Current.CancellationToken;
        await using var server = await DroppingNativeServer.StartAsync(type, ct);
        await using var transport = await ConnectAsync(type, server.Port, pipelining: false);

        using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var request = transport.SendRequestAsync(SurgewaveOpCode.Fetch, new byte[64], compress: false, cancelled.Token).AsTask();
        await server.HasReadAsync(1).WaitAsync(Bound, ct);
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => request.WaitAsync(Bound, ct));

        var next = transport.SendRequestAsync(SurgewaveOpCode.Fetch, new byte[64], compress: false, ct).AsTask();

        var failure = await Assert.ThrowsAnyAsync<IOException>(() => next.WaitAsync(Bound, ct));
        Assert.Contains("cancelled", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains($"127.0.0.1:{server.Port}", failure.Message, StringComparison.Ordinal);
        Assert.False(transport.IsConnected);
        Assert.IsAssignableFrom<IOException>(await transport.ConnectionEnded.WaitAsync(Bound, ct));
    }

    private static void AssertSaysTheConnectionWent(IOException failure, int port)
    {
        Assert.Contains("connection", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains($"127.0.0.1:{port}", failure.Message, StringComparison.Ordinal);
    }

    private static void SkipWithout(SurgewaveTransportType type)
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return;
        if (!DroppingNativeServer.Supports(type))
            Assert.Skip($"{type} is not available on this machine (QUIC needs msquic).");
    }

    private static async Task<ISurgewaveTransport> ConnectAsync(SurgewaveTransportType type, int port, bool pipelining)
    {
        var options = new TransportOptions
        {
            Host = "127.0.0.1",
            Port = port,
            EnablePipelining = pipelining,
            EnableCompression = false,
            TrustAllCertificates = true,
        };

        ISurgewaveTransport transport =
            type == SurgewaveTransportType.Quic && (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
                ? new QuicTransport(options)
                : new TcpTransport(options);

        try
        {
            await transport.ConnectAsync(TestContext.Current.CancellationToken).AsTask()
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
            return transport;
        }
        catch
        {
            await transport.DisposeAsync();
            throw;
        }
    }
}
