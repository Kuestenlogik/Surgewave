using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// A consumer built with a constructor connects on its first use (#200), as the producer does since #133. The
/// constructor used to connect synchronously, running the client's retry loop — over a minute against an
/// unreachable broker — on the caller's thread.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ConsumerConnectsOnFirstUseTests
{
    [Fact]
    public async Task TheConstructorDoesNotConnectAndTheFirstConsumeWaitsOnlyAsLongAsItsToken()
    {
        var bootstrap = $"127.0.0.1:{UnusedPort()}";

        // Would block for the whole retry loop if the constructor still connected.
        await using var consumer = new SurgewaveConsumer<string, string>(o => o.BootstrapServers = bootstrap);
        Assert.False(consumer.IsConnected);
        consumer.Assign("t", 0);

        using var cts = new CancellationTokenSource();
        var consume = consumer.ConsumeAsync(cts.Token);
        Assert.False(consume.IsCompleted); // the consume is connecting, through the retry loop
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => consume);
    }

    [Fact]
    public async Task TheFirstSubscribeConnectsAndWaitsOnlyAsLongAsItsToken()
    {
        var bootstrap = $"127.0.0.1:{UnusedPort()}";

        await using var consumer = new SurgewaveConsumer<string, string>(
            new SurgewaveConsumerOptions<string, string> { BootstrapServers = bootstrap });

        using var cts = new CancellationTokenSource();
        var subscribe = consumer.SubscribeAsync(cts.Token, "t");
        Assert.False(subscribe.IsCompleted); // the subscribe is connecting, through the retry loop
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => subscribe);
    }

    [Fact]
    public void TheClientBuilderHasNoSynchronousBuild()
    {
        // Build() blocked the caller on BuildAsync() and its connection retry loop. Protocol auto-detection is a
        // connection, so the builder builds asynchronously only.
        Assert.Null(typeof(SurgewaveClientBuilder).GetMethod("Build", Type.EmptyTypes));
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
