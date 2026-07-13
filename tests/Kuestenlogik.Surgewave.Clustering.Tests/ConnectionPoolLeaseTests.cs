using System.Net;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc5 — lease/permit accounting of the <see cref="ConnectionPool"/>. The native controller
/// client leans on two invariants: (1) a REUSED pooled connection honors its next Return/Discard
/// (the returned-flag must be re-armed on hand-out, or every reuse cycle leaks one permit and one
/// socket until the endpoint's semaphore is exhausted and the control plane to that broker dies);
/// (2) Discard closes the connection AND releases its permit exactly once.
/// </summary>
public class ConnectionPoolLeaseTests
{
    private sealed class FakeConnection : IPeerConnection
    {
        public Stream Stream { get; } = new MemoryStream();
        public EndPoint? RemoteEndPoint => null;
        public bool IsConnected { get; private set; } = true;

        public ValueTask<IPeerStreamLease> AcquireStreamAsync(CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask<IPeerStreamLease> AcceptInboundStreamAsync(CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public ValueTask DisposeAsync()
        {
            IsConnected = false;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FakeTransport : IPeerTransport
    {
        public int Connects;
        public string Name => "fake";

        public ValueTask<IPeerConnection> ConnectAsync(string host, int port, CancellationToken cancellationToken = default)
        {
            Connects++;
            return ValueTask.FromResult<IPeerConnection>(new FakeConnection());
        }

        public IPeerListener CreateListener(IPEndPoint endpoint) => throw new NotSupportedException();
    }

    [Fact]
    public async Task ReusedConnection_SecondReturn_RepoolsInsteadOfLeaking()
    {
        var transport = new FakeTransport();
        using var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, transport, maxConnectionsPerBroker: 1);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var c1 = await pool.GetConnectionAsync("h", 1, cts.Token);
        c1.Return();

        var c2 = await pool.GetConnectionAsync("h", 1, cts.Token);
        Assert.Same(c1, c2);
        c2.Return(); // second Return of the same instance — must re-pool, not silently no-op

        // With maxConnectionsPerBroker = 1, a leaked permit would make this call hang on the
        // exhausted semaphore (needing a NEW connection) instead of reusing the pooled one.
        var c3 = await pool.GetConnectionAsync("h", 1, cts.Token);
        Assert.Same(c1, c3);
        Assert.Equal(1, transport.Connects);
        c3.Return();
    }

    [Fact]
    public async Task Discard_OnReusedConnection_ClosesAndReleasesPermit()
    {
        var transport = new FakeTransport();
        using var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, transport, maxConnectionsPerBroker: 1);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var c1 = await pool.GetConnectionAsync("h", 1, cts.Token);
        c1.Return();
        var c2 = await pool.GetConnectionAsync("h", 1, cts.Token);
        Assert.Same(c1, c2);

        c2.Discard(); // poisoned exchange on a REUSED connection
        Assert.False(c2.IsAlive);

        // The permit must be free again: with max = 1 this would hang forever on a leak, and it
        // must be a NEW connection (the discarded one may hold a stale response in its buffer).
        var c3 = await pool.GetConnectionAsync("h", 1, cts.Token);
        Assert.NotSame(c1, c3);
        Assert.Equal(2, transport.Connects);
        c3.Return();
    }

    [Fact]
    public async Task DoubleReturnOrDiscard_WithinOneLease_IsIdempotent()
    {
        var transport = new FakeTransport();
        using var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, transport, maxConnectionsPerBroker: 1);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var c1 = await pool.GetConnectionAsync("h", 1, cts.Token);
        c1.Return();
        c1.Return();  // must not double-enqueue
        c1.Discard(); // must not release a second permit for the same lease

        var c2 = await pool.GetConnectionAsync("h", 1, cts.Token);
        Assert.Same(c1, c2); // exactly one pooled instance came back out
        c2.Return();
    }

    [Fact]
    public async Task ConcurrentGetAndRelease_NeverLeaksOrExhaustsThePermits()
    {
        var transport = new FakeTransport();
        const int maxPerBroker = 4;
        using var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, transport, maxConnectionsPerBroker: maxPerBroker);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Many threads hammer one endpoint with a mixed Get -> Return|Discard loop. A permit leak or
        // over-release (or a torn returned-flag reset racing a reuse) would eventually deadlock a
        // GetConnectionAsync on the exhausted semaphore, tripping the cancellation deadline.
        var tasks = Enumerable.Range(0, 8).Select(t => Task.Run(async () =>
        {
            for (var i = 0; i < 200; i++)
            {
                var conn = await pool.GetConnectionAsync("h", 1, cts.Token);
                if ((t + i) % 2 == 0) conn.Return();
                else conn.Discard();
            }
        }, cts.Token));

        await Task.WhenAll(tasks); // completes only if no call ever blocked on an exhausted permit

        // The endpoint must still be usable — the semaphore was never permanently drained.
        var live = await pool.GetConnectionAsync("h", 1, cts.Token);
        live.Return();
        // Live connections at any instant never exceeded the bound, so total creates are bounded by
        // (leak-free) reuse: with 8×200 ops and max 4 permits, a leak would have forced ~1600 creates.
        Assert.True(transport.Connects <= 1600);
    }
}
