using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// A producer built with a constructor connects on its first send (#133). The constructor used to connect
/// synchronously, running the client's retry loop — over a minute against an unreachable broker — on the
/// caller's thread, in an ASP.NET application at startup.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ProducerConnectsOnFirstSendTests
{
    [Fact]
    public async Task TheConstructorDoesNotConnectAndTheFirstSendWaitsOnlyAsLongAsItsToken()
    {
        var bootstrap = $"127.0.0.1:{UnusedPort()}";

        // Would block for the whole retry loop if the constructor still connected.
        await using var producer = new SurgewaveProducer<string, string>(o => o.BootstrapServers = bootstrap);

        using var cts = new CancellationTokenSource();
        var send = producer.ProduceAsync("t", "k", "v", cts.Token);
        Assert.False(send.IsCompleted); // the send is connecting, through the retry loop
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => send);
    }

    /// <summary>A port nothing listens on: bound, read, released.</summary>
    private static int UnusedPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
