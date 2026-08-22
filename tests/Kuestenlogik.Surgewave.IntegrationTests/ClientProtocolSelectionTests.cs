using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// #71 — client-side protocol selection (auto / native / kafka) against a real
/// Surgewave broker. Auto is native-first: it probes the native protocol and, if
/// reachable, reuses that connection; a secured or unreachable native falls back
/// to Kafka. Explicit modes force the respective wire. The Kafka wire is spoken by
/// the Surgewave client itself (no Confluent dependency).
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection(nameof(BrokerSpawningCollection))]
public sealed class ClientProtocolSelectionTests
{
    private static async Task<SurgewaveRuntime> StartBrokerAsync(CancellationToken cancellationToken, bool kafka = true)
    {
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics()
            .WithStorageEngine(StorageEngines.Memory);
        if (!kafka)
            builder = builder.WithoutKafka();
        return await builder.Build().StartAsync(cancellationToken);
    }

    [Fact(Timeout = 60000)]
    public async Task Auto_PicksNative_AgainstSurgewaveBroker()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var runtime = await StartBrokerAsync(cancellationToken);

        await using var client = await SurgewaveClient.Create(runtime.BootstrapServers)
            .UseAutoDetect()
            .BuildAsync(cancellationToken);

        Assert.Equal(ProtocolType.SurgewaveNative, client.Protocol);
        Assert.True(client.IsConnected);
    }

    [Fact(Timeout = 60000)]
    public async Task Auto_PicksNative_AgainstNativeOnlyServer()
    {
        // Kafka disabled on the server (#58); native must still be selected. If the
        // native probe failed, Auto would fall back to Kafka and the connect would
        // be rejected — so a green result also proves native works native-only.
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var runtime = await StartBrokerAsync(cancellationToken, kafka: false);

        await using var client = await SurgewaveClient.Create(runtime.BootstrapServers)
            .UseAutoDetect()
            .BuildAsync(cancellationToken);

        Assert.Equal(ProtocolType.SurgewaveNative, client.Protocol);
        Assert.True(client.IsConnected);
    }

    [Fact(Timeout = 60000)]
    public async Task ForceNative_Connects()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var runtime = await StartBrokerAsync(cancellationToken);

        await using var client = await SurgewaveClient.Create(runtime.BootstrapServers)
            .UseSurgewaveProtocol()
            .BuildAsync(cancellationToken);

        Assert.Equal(ProtocolType.SurgewaveNative, client.Protocol);
        Assert.True(client.IsConnected);
    }

    [Fact(Timeout = 60000)]
    public async Task ForceKafka_Connects_ViaSurgewaveKafkaWire()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var runtime = await StartBrokerAsync(cancellationToken);

        // Forces the Kafka wire even against a Surgewave server. BuildAsync connects,
        // so a green result proves the client's own Kafka-wire path reaches the broker.
        await using var client = await SurgewaveClient.Create(runtime.BootstrapServers)
            .UseKafkaProtocol()
            .BuildAsync(cancellationToken);

        Assert.Equal(ProtocolType.Kafka, client.Protocol);
        Assert.True(client.IsConnected);
    }
}
