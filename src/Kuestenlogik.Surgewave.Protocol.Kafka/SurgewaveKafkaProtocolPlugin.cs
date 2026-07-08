using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Protocol plugin advertising the Kafka wire protocol — native-first Surgewave's
/// optional compatibility layer (#58 / #41). Kafka rides the broker's shared TCP
/// listener (auto-detected by the first magic bytes), so this plugin opens no
/// port of its own and, for stage 1, registers no services: the handler array
/// and dispatcher stay wired in the broker host, gated by the same
/// <c>Surgewave:Kafka:Enabled</c> flag, to guarantee zero hot-path change.
/// <para>
/// Its job here is discovery + a config-gate marker so the broker advertises
/// "Kafka Protocol" only when enabled. Stage 2 (#59) moves the handler stack
/// into <see cref="ConfigureServices"/> so the plugin owns the Kafka wire.
/// </para>
/// </summary>
public sealed class SurgewaveKafkaProtocolPlugin : IProtocolPlugin
{
    public string FeatureId => "Surgewave.Protocol.Kafka";
    public string DisplayName => "Kafka Protocol";

    /// <summary>
    /// 0 — Kafka shares the broker's main listener with the native protocol;
    /// there is no separate Kafka port.
    /// </summary>
    public int DefaultPort => 0;

    /// <summary>
    /// Enabled by default (unlike opt-in protocols): Kafka compatibility is on
    /// unless the operator sets <c>Surgewave:Kafka:Enabled=false</c> to run
    /// native-only.
    /// </summary>
    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:Kafka:Enabled", true);

    /// <summary>
    /// No-op for stage 1: the Kafka handler array + dispatcher + shared listener
    /// remain wired in the broker host (guaranteeing zero hot-path change). The
    /// handler stack moves here in stage 2 (#59).
    /// </summary>
    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
    }
}
