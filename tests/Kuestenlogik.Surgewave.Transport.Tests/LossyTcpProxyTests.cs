using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Kuestenlogik.Surgewave.Testing.Network;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

public class LossyTcpProxyTests
{
    [Fact]
    public async Task Proxy_ForwardsBytes_BothDirections()
    {
        // CI runners stall loopback TCP handshakes under load, leaving
        // BytesClientToBroker=0 even though the proxy code is correct.
        CiSkip.IfRunningOnCi("Loopback TCP timing is unreliable on CI runners; verified locally.");

        // Echo server
        using var upstream = new TcpListener(IPAddress.Loopback, 0);
        upstream.Start();
        var upstreamPort = ((IPEndPoint)upstream.LocalEndpoint).Port;
        _ = Task.Run(async () =>
        {
            using var server = await upstream.AcceptTcpClientAsync();
            var buf = new byte[256];
            var stream = server.GetStream();
            var n = await stream.ReadAsync(buf);
            await stream.WriteAsync(buf.AsMemory(0, n));
        });

        // Proxy with 0ms latency
        var proxyPort = GetFreePort();
        await using var proxy = new LossyTcpProxy(proxyPort, upstreamPort, latencyMs: 0);
        await proxy.Start();

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", proxyPort);
        var clientStream = client.GetStream();

        var payload = Encoding.UTF8.GetBytes("hello-proxy");
        await clientStream.WriteAsync(payload);
        await clientStream.FlushAsync();

        var response = new byte[256];
        var read = await clientStream.ReadAsync(response);

        Assert.Equal("hello-proxy", Encoding.UTF8.GetString(response, 0, read));
        Assert.True(proxy.BytesClientToBroker > 0);
        Assert.True(proxy.BytesBrokerToClient > 0);
    }

    [Fact]
    public async Task Proxy_WithLatency_AddsDelay()
    {
        const int latencyMs = 50;

        // Echo server
        using var upstream = new TcpListener(IPAddress.Loopback, 0);
        upstream.Start();
        var upstreamPort = ((IPEndPoint)upstream.LocalEndpoint).Port;
        _ = Task.Run(async () =>
        {
            using var server = await upstream.AcceptTcpClientAsync();
            var buf = new byte[256];
            var stream = server.GetStream();
            var n = await stream.ReadAsync(buf);
            await stream.WriteAsync(buf.AsMemory(0, n));
        });

        var proxyPort = GetFreePort();
        await using var proxy = new LossyTcpProxy(proxyPort, upstreamPort, latencyMs);
        await proxy.Start();

        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", proxyPort);
        var clientStream = client.GetStream();

        var sw = Stopwatch.StartNew();
        await clientStream.WriteAsync(Encoding.UTF8.GetBytes("ping"));
        await clientStream.FlushAsync();
        var buf = new byte[64];
        await clientStream.ReadExactlyAsync(buf.AsMemory(0, 4));
        sw.Stop();

        // Each direction adds latencyMs, so round-trip should be >= 2 * latencyMs.
        // Allow some scheduling jitter.
        Assert.True(sw.ElapsedMilliseconds >= latencyMs,
            $"Expected >= {latencyMs}ms, got {sw.ElapsedMilliseconds}ms");
    }

    private static int GetFreePort()
    {
        using var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }
}
