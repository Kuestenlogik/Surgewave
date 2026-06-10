using Kuestenlogik.Surgewave.Runtime;

namespace Kuestenlogik.Surgewave.Benchmarks.Public.Sut;

/// <summary>
/// Embedded Surgewave broker — used for BOTH "Surgewave Native" and
/// "Surgewave Kafka-wire" measurements. Started once per scenario; the
/// scenario picks whether to drive it natively or via Confluent.Kafka.
/// In-memory storage so the benchmark measures protocol + broker
/// pipeline, not disk; that decision matches the upstream
/// Comparison-Benchmark setup and makes the numbers across SUTs
/// directly comparable (Kafka container also keeps log on tmpfs by
/// default).
/// </summary>
public sealed class SurgewaveSut : IBrokerSut
{
    private readonly SurgewaveRuntime _runtime;
    private readonly bool _native;

    private SurgewaveSut(SurgewaveRuntime runtime, bool native)
    {
        _runtime = runtime;
        _native = native;
        BootstrapServers = runtime.BootstrapServers;
        NativeEndpoint = (runtime.Host, runtime.Port);
    }

    public string DisplayName => _native ? "Surgewave Native" : "Surgewave Kafka-wire";
    public bool SupportsNative => _native;
    public string BootstrapServers { get; }
    public (string Host, int Port)? NativeEndpoint { get; }

    public static async Task<SurgewaveSut> StartAsync(bool native, CancellationToken ct)
    {
        var runtime = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .Build()
            .StartAsync(ct)
            .ConfigureAwait(false);
        return new SurgewaveSut(runtime, native);
    }

    public async ValueTask DisposeAsync()
    {
        await _runtime.DisposeAsync().ConfigureAwait(false);
    }
}
