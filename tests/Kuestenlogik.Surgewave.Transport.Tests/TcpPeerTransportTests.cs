using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

public class TcpPeerTransportTests
{
    [Fact]
    public async Task ConnectAsync_ValidEndpoint_ReturnsConnectedPeer()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var transport = new TcpPeerTransport();
        var connectTask = transport.ConnectAsync("127.0.0.1", port);
        using var accepted = await listener.AcceptTcpClientAsync();
        var connection = await connectTask;

        Assert.True(connection.IsConnected);
        Assert.NotNull(connection.Stream);
        Assert.NotNull(connection.RemoteEndPoint);

        await connection.DisposeAsync();
        listener.Stop();
    }

    [Fact]
    public async Task ConnectAsync_InvalidEndpoint_Throws()
    {
        var transport = new TcpPeerTransport();
        await Assert.ThrowsAnyAsync<SocketException>(
            () => transport.ConnectAsync("127.0.0.1", 1).AsTask());
    }

    [Fact]
    public async Task Listener_AcceptAsync_ReturnsConnection()
    {
        var transport = new TcpPeerTransport();
        var peerListener = transport.CreateListener(new IPEndPoint(IPAddress.Loopback, 0));
        await peerListener.StartAsync();

        var port = peerListener.LocalEndPoint.Port;

        using var client = new TcpClient();
        var acceptTask = peerListener.AcceptAsync();
        await client.ConnectAsync("127.0.0.1", port);

        var connection = await acceptTask;
        Assert.True(connection.IsConnected);
        Assert.NotNull(connection.Stream);

        await connection.DisposeAsync();
        await peerListener.DisposeAsync();
        client.Dispose();
    }

    [Fact]
    public async Task Connection_AfterDispose_IsNotConnected()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var transport = new TcpPeerTransport();
        var connectTask = transport.ConnectAsync("127.0.0.1", port);
        using var accepted = await listener.AcceptTcpClientAsync();
        var connection = await connectTask;

        await connection.DisposeAsync();

        Assert.False(connection.IsConnected);
        listener.Stop();
    }

    [Fact]
    public async Task AcceptInboundStreamAsync_ReturnsSharedStream()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var transport = new TcpPeerTransport();
        var connectTask = transport.ConnectAsync("127.0.0.1", port);
        using var accepted = await listener.AcceptTcpClientAsync();
        var connection = await connectTask;

        try
        {
            await using var lease = await connection.AcceptInboundStreamAsync();
            Assert.Same(connection.Stream, lease.Stream);
        }
        finally
        {
            await connection.DisposeAsync();
            listener.Stop();
        }
    }
}
