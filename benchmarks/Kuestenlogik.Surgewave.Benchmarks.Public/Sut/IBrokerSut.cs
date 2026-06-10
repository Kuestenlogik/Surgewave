namespace Kuestenlogik.Surgewave.Benchmarks.Public.Sut;

/// <summary>
/// One system-under-test for the G3 public suite: brings up the broker
/// (embedded process, Docker container, …), exposes the wire address(es)
/// the scenarios connect to, tears down on dispose.
///
/// The interface intentionally exposes both a <see cref="BootstrapServers"/>
/// Kafka-wire endpoint and an optional <see cref="NativeEndpoint"/>. Apache
/// Kafka and Redpanda only fill the former; the two Surgewave SUTs fill
/// both (same broker, two protocols).
/// </summary>
public interface IBrokerSut : IAsyncDisposable
{
    /// <summary>Stable label shown in the result table (e.g. "Surgewave Native").</summary>
    string DisplayName { get; }

    /// <summary>
    /// Whether this SUT speaks Surgewave's native protocol. When false, the
    /// scenario must drive the SUT via Confluent.Kafka over the Kafka wire.
    /// </summary>
    bool SupportsNative { get; }

    /// <summary>Kafka-wire bootstrap servers (always populated).</summary>
    string BootstrapServers { get; }

    /// <summary>Host + port for the native protocol (null for non-Surgewave SUTs).</summary>
    (string Host, int Port)? NativeEndpoint { get; }
}
