using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Testing.Network;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Tests for <see cref="LossyUdpProxy"/> drop-rate accuracy and latency injection.
/// </summary>
public class LossyUdpProxyTests
{
    [Theory]
    [InlineData(0.0)]
    [InlineData(0.5)]
    [InlineData(1.0)]
    public async Task DropRate_IsStatisticallyAccurate(double dropRate)
    {
        // Linux GitHub-Actions runners lose loopback UDP packets independent
        // of the proxy when 2000 datagrams hit the socket buffer in a tight
        // loop — observed actual rates of 0.55 with dropRate=0 and 0.15 with
        // dropRate=0.5, far outside any reasonable tolerance. The proxy logic
        // is fine; the OS makes the measurement unreliable. Skip on CI; the
        // test still runs locally where loopback is quiet enough.
        CiSkip.IfRunningOnCi("Loopback UDP packet loss on CI runners makes statistical drop-rate measurement unreliable.");

        const int totalDatagrams = 2000;
        const double tolerance = 0.06; // ±6% acceptable variance

        // Upstream: simple echo server
        using var upstream = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        upstream.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        var upstreamPort = ((IPEndPoint)upstream.LocalEndPoint!).Port;

        // Proxy
        using var probeSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        probeSocket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        var proxyPort = ((IPEndPoint)probeSocket.LocalEndPoint!).Port;
        probeSocket.Dispose();

        await using var proxy = new LossyUdpProxy(proxyPort, upstreamPort, dropRate, latencyMs: 0);
        _ = proxy.Start();

        // Echo loop on the upstream side
        var echoTask = Task.Run(async () =>
        {
            var buf = new byte[64];
            var from = new IPEndPoint(IPAddress.Any, 0) as EndPoint;
            for (int i = 0; i < totalDatagrams * 2; i++)
            {
                try
                {
                    var result = await upstream.ReceiveFromAsync(buf, SocketFlags.None, from);
                    await upstream.SendToAsync(buf.AsMemory(0, result.ReceivedBytes),
                        SocketFlags.None, result.RemoteEndPoint);
                }
                catch { break; }
            }
        });

        // Client: send datagrams through the proxy, count how many echoes come back
        using var client = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        client.ReceiveTimeout = 500;
        var proxyEndpoint = new IPEndPoint(IPAddress.Loopback, proxyPort);
        var payload = new byte[] { 0x42 };
        int received = 0;

        for (int i = 0; i < totalDatagrams; i++)
        {
            await client.SendToAsync(payload, SocketFlags.None, proxyEndpoint);
        }

        // Wait a bit for all echoes to arrive, then drain
        await Task.Delay(500);
        var recvBuf = new byte[64];
        while (true)
        {
            try
            {
                var r = client.Receive(recvBuf);
                if (r > 0) received++;
            }
            catch (SocketException)
            {
                break;
            }
        }

        // Expected: each direction has independent dropRate.
        // P(datagram survives both hops) = (1-dropRate)^2
        var expectedSurvivalRate = Math.Pow(1 - dropRate, 2);
        var expectedReceived = totalDatagrams * expectedSurvivalRate;
        var actualRate = (double)received / totalDatagrams;
        var expectedRate = expectedSurvivalRate;

        Assert.InRange(actualRate, expectedRate - tolerance, expectedRate + tolerance);
    }

    [Fact]
    public async Task ZeroDropRate_AllDatagramsForwarded()
    {
        using var upstream = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        upstream.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        var upstreamPort = ((IPEndPoint)upstream.LocalEndPoint!).Port;

        using var probeSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        probeSocket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        var proxyPort = ((IPEndPoint)probeSocket.LocalEndPoint!).Port;
        probeSocket.Dispose();

        await using var proxy = new LossyUdpProxy(proxyPort, upstreamPort, dropRate: 0, latencyMs: 0);
        _ = proxy.Start();

        // Send 10 datagrams, collect them all on upstream
        using var client = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        var proxyEp = new IPEndPoint(IPAddress.Loopback, proxyPort);
        var payload = new byte[] { 0x01 };

        for (int i = 0; i < 10; i++)
        {
            await client.SendToAsync(payload, SocketFlags.None, proxyEp);
        }

        await Task.Delay(300);
        Assert.True(proxy.ClientToBrokerForwarded >= 10);
        Assert.Equal(0, proxy.ClientToBrokerDropped);
    }
}
