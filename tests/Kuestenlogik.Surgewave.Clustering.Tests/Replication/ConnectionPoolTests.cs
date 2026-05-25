using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Replication;

public class ConnectionPoolTests
{
    [Fact]
    public async Task GetConnection_ToLoopback_ReturnsAliveConnection()
    {
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;

        var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, new TcpPeerTransport());
        var acceptTask = listener.AcceptTcpClientAsync();
        var conn = await pool.GetConnectionAsync("127.0.0.1", port);
        using var accepted = await acceptTask;

        Assert.True(conn.IsAlive);
        Assert.NotNull(conn.Stream);

        conn.Dispose();
        pool.Dispose();
        listener.Stop();
    }

    [Fact]
    public async Task ReturnedConnection_IsReused()
    {
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;

        var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, new TcpPeerTransport());
        var accept1 = listener.AcceptTcpClientAsync();
        var conn1 = await pool.GetConnectionAsync("127.0.0.1", port);
        using var a1 = await accept1;
        var stream1 = conn1.Stream;
        conn1.Return();

        var conn2 = await pool.GetConnectionAsync("127.0.0.1", port);
        Assert.Same(stream1, conn2.Stream);

        conn2.Dispose();
        pool.Dispose();
        listener.Stop();
    }

    [Fact]
    public async Task Dispose_CleanupPool()
    {
        var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, new TcpPeerTransport());
        pool.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            pool.GetConnectionAsync("127.0.0.1", 9999));
    }
}
