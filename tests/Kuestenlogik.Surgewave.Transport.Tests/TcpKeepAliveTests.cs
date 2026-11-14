using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// A client's connection to its broker is probed by the operating system while it carries nothing (#289), so that a
/// broker whose host vanished is noticed. That the probes go out and fail the connection is the operating system's
/// part and is not staged here — a loopback peer cannot vanish without saying so; what is held is that the
/// connection asks for them, with the times its options name.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TcpKeepAliveTests
{
    [Fact]
    public async Task AConnectionAsksForTheProbesItsOptionsName()
    {
        var ct = TestContext.Current.CancellationToken;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port, ct);

        TcpKeepAlive.Apply(client.Client, new TransportOptions
        {
            Host = "127.0.0.1",
            Port = 1,
            KeepAliveTime = TimeSpan.FromSeconds(20),
            KeepAliveInterval = TimeSpan.FromSeconds(4),
            KeepAliveRetryCount = 6,
        });

        Assert.NotEqual(0, (int)client.Client.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive)!);
        Assert.Equal(20, (int)client.Client.GetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime)!);
        Assert.Equal(4, (int)client.Client.GetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval)!);
        Assert.Equal(6, (int)client.Client.GetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount)!);
    }

    [Fact]
    public async Task AConnectionWhoseOptionsTurnTheProbesOffAsksForNone()
    {
        var ct = TestContext.Current.CancellationToken;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port, ct);

        TcpKeepAlive.Apply(client.Client, new TransportOptions { Host = "127.0.0.1", Port = 1, KeepAliveTime = Timeout.InfiniteTimeSpan });

        Assert.Equal(0, (int)client.Client.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive)!);
    }
}
