using Kuestenlogik.Surgewave.Testing.Chaos;
using Xunit;

namespace Kuestenlogik.Surgewave.Testing.Chaos.Tests;

public class NetworkLossScenarioTests
{
    [Fact]
    public async Task CreateForUdp_StartsProxy_ReportsStats()
    {
        var upstreamPort = GetFreeUdpPort();
        var listenPort = GetFreeUdpPort();

        await using var scenario = NetworkLossScenario.CreateForUdp(
            listenPort, upstreamPort, dropRate: 0, latencyMs: 0);

        Assert.Equal(listenPort, scenario.ListenPort);
        Assert.Equal(0, scenario.DroppedDatagrams);
    }

    [Fact]
    public async Task CreateForTcp_StartsProxy()
    {
        var upstreamPort = GetFreeTcpPort();
        var listenPort = GetFreeTcpPort();

        await using var scenario = NetworkLossScenario.CreateForTcp(
            listenPort, upstreamPort, latencyMs: 0);

        Assert.Equal(listenPort, scenario.ListenPort);
    }

    [Fact]
    public async Task DisposeAsync_Idempotent()
    {
        var upstreamPort = GetFreeUdpPort();
        var listenPort = GetFreeUdpPort();

        var scenario = NetworkLossScenario.CreateForUdp(
            listenPort, upstreamPort, dropRate: 0.5, latencyMs: 0);

        await scenario.DisposeAsync();
        await scenario.DisposeAsync(); // should be safe
    }

    private static int GetFreeUdpPort()
    {
        using var sock = new System.Net.Sockets.Socket(
            System.Net.Sockets.AddressFamily.InterNetwork,
            System.Net.Sockets.SocketType.Dgram,
            System.Net.Sockets.ProtocolType.Udp);
        sock.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 0));
        return ((System.Net.IPEndPoint)sock.LocalEndPoint!).Port;
    }

    private static int GetFreeTcpPort()
    {
        using var sock = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        sock.Start();
        var port = ((System.Net.IPEndPoint)sock.LocalEndpoint).Port;
        sock.Stop();
        return port;
    }
}
