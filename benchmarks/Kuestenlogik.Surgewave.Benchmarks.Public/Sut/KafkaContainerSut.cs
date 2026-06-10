using Testcontainers.Kafka;

namespace Kuestenlogik.Surgewave.Benchmarks.Public.Sut;

/// <summary>
/// Apache Kafka 7.x in a Confluent <c>cp-kafka</c> container, brought
/// up via Testcontainers.NET. Pinned image so reruns of the public
/// benchmark across hosts compare against the same Kafka version.
/// </summary>
public sealed class KafkaContainerSut : IBrokerSut
{
    private const string DefaultImage = "confluentinc/cp-kafka:7.6.0";

    private readonly KafkaContainer _container;

    private KafkaContainerSut(KafkaContainer container)
    {
        _container = container;
        BootstrapServers = container.GetBootstrapAddress();
    }

    public string DisplayName => "Apache Kafka";
    public bool SupportsNative => false;
    public string BootstrapServers { get; }
    public (string Host, int Port)? NativeEndpoint => null;

    public static async Task<KafkaContainerSut> StartAsync(CancellationToken ct, string image = DefaultImage)
    {
        var container = new KafkaBuilder(image).Build();
        await container.StartAsync(ct).ConfigureAwait(false);
        return new KafkaContainerSut(container);
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync().ConfigureAwait(false);
    }
}
