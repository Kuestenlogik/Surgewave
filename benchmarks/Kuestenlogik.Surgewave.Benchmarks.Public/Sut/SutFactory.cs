namespace Kuestenlogik.Surgewave.Benchmarks.Public.Sut;

/// <summary>
/// Bringup helper that turns the curated SUT list (Surgewave Native +
/// Surgewave Kafka-wire + Apache Kafka + Redpanda) into started
/// instances. Each SUT is independently fallible: if Docker is missing
/// on the host, Kafka + Redpanda return null and the scenario records
/// "container start failed" for that row, rather than aborting the
/// whole run. The two Surgewave SUTs need no Docker — they always
/// run.
/// </summary>
internal static class SutFactory
{
    public static async Task<IBrokerSut?> TryStartAsync(SutKind kind, CancellationToken ct)
    {
        try
        {
            return kind switch
            {
                SutKind.SurgewaveNative => await SurgewaveSut.StartAsync(native: true, ct).ConfigureAwait(false),
                SutKind.SurgewaveKafkaWire => await SurgewaveSut.StartAsync(native: false, ct).ConfigureAwait(false),
                SutKind.ApacheKafka => await KafkaContainerSut.StartAsync(ct).ConfigureAwait(false),
                SutKind.Redpanda => await RedpandaContainerSut.StartAsync(ct).ConfigureAwait(false),
                _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
            };
        }
        catch (Exception ex) when (kind is SutKind.ApacheKafka or SutKind.Redpanda)
        {
            Console.Error.WriteLine($"[!] {kind}: container bringup failed ({ex.GetType().Name}: {ex.Message}). Skipping.");
            return null;
        }
    }
}

internal enum SutKind
{
    SurgewaveNative,
    SurgewaveKafkaWire,
    ApacheKafka,
    Redpanda,
}
