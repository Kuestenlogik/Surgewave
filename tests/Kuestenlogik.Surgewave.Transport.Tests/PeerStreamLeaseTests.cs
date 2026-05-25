using Kuestenlogik.Surgewave.Transport.Tcp;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Tests for <see cref="IPeerStreamLease"/> concurrency guarantees:
/// TCP leases must serialise parallel RPC access, QUIC leases should
/// (eventually) return distinct streams.
/// </summary>
public class PeerStreamLeaseTests
{
    [Fact]
    public async Task TcpLease_ParallelAcquire_Serialises()
    {
        // Arrange: open a loopback TCP connection
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;

        using var clientSocket = new System.Net.Sockets.TcpClient();
        await clientSocket.ConnectAsync("127.0.0.1", port);
        using var serverSocket = await listener.AcceptTcpClientAsync();

        var transport = new TcpPeerTransport();
        // Use the raw TcpPeerConnection via the public ConnectAsync path
        var serverPort = ((System.Net.IPEndPoint)serverSocket.Client.LocalEndPoint!).Port;

        // We need a TcpPeerConnection for the test. Since TcpPeerConnection is internal,
        // use the public IPeerTransport.ConnectAsync on a fresh listener port.
        listener.Stop();
        using var listener2 = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener2.Start();
        var port2 = ((System.Net.IPEndPoint)listener2.LocalEndpoint).Port;

        var connectTask = transport.ConnectAsync("127.0.0.1", port2);
        using var accepted = await listener2.AcceptTcpClientAsync();
        var connection = await connectTask;

        try
        {
            // Acquire first lease — should succeed immediately
            var lease1 = await connection.AcquireStreamAsync();

            // Acquire second lease in parallel — should block because TCP serialises
            var lease2Task = connection.AcquireStreamAsync();
            await Task.Delay(50);
            Assert.False(lease2Task.IsCompleted, "Second TCP lease should block while first is held");

            // Release first → second should complete
            await lease1.DisposeAsync();
            var lease2 = await lease2Task.AsTask().WaitAsync(TimeSpan.FromSeconds(2));
            Assert.NotNull(lease2);

            await lease2.DisposeAsync();
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }

    [Fact]
    public async Task TcpLease_StreamIsSameInstance()
    {
        var transport = new TcpPeerTransport();
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;

        var connectTask = transport.ConnectAsync("127.0.0.1", port);
        using var accepted = await listener.AcceptTcpClientAsync();
        var connection = await connectTask;

        try
        {
            await using var lease1 = await connection.AcquireStreamAsync();
            var stream1 = lease1.Stream;
            await lease1.DisposeAsync();

            await using var lease2 = await connection.AcquireStreamAsync();
            var stream2 = lease2.Stream;

            Assert.Same(stream1, stream2);
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }

    [Fact]
    public async Task TcpLease_DoubleDispose_Safe()
    {
        var transport = new TcpPeerTransport();
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;

        var connectTask = transport.ConnectAsync("127.0.0.1", port);
        using var accepted = await listener.AcceptTcpClientAsync();
        var connection = await connectTask;

        try
        {
            var lease = await connection.AcquireStreamAsync();

            await lease.DisposeAsync();
            await lease.DisposeAsync(); // should not throw or double-release
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }
}
