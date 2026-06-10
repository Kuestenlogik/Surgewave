using Testcontainers.Redpanda;

namespace Kuestenlogik.Surgewave.Benchmarks.Public.Sut;

/// <summary>
/// Redpanda in its official upstream container via Testcontainers.NET.
/// Same Kafka-wire surface as the Kafka container — the differentiator
/// here is the C++/Seastar broker implementation. Listed in the public
/// suite so the Surgewave-vs-Redpanda claim has reproducible numbers,
/// not just blog posts.
/// </summary>
public sealed class RedpandaContainerSut : IBrokerSut
{
    private const string DefaultImage = "redpandadata/redpanda:latest";

    private readonly RedpandaContainer _container;

    private RedpandaContainerSut(RedpandaContainer container)
    {
        _container = container;
        BootstrapServers = container.GetBootstrapAddress();
    }

    public string DisplayName => "Redpanda";
    public bool SupportsNative => false;
    public string BootstrapServers { get; }
    public (string Host, int Port)? NativeEndpoint => null;

    public static async Task<RedpandaContainerSut> StartAsync(CancellationToken ct, string image = DefaultImage)
    {
        var container = new RedpandaBuilder(image).Build();
        await container.StartAsync(ct).ConfigureAwait(false);
        return new RedpandaContainerSut(container);
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync().ConfigureAwait(false);
    }
}
