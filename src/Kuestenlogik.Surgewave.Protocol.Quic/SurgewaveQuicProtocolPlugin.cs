using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Protocol.Quic;

/// <summary>
/// Protocol plugin for the raw QUIC transport. Runs as a BackgroundService with its
/// own UDP listener and delegates each accepted bidirectional stream to the broker's
/// protocol-agnostic <c>ISurgewaveStreamHandler</c>, which auto-detects Surgewave-native vs
/// Kafka from the first 4 bytes on each stream — same as the TCP path.
/// </summary>
public sealed class SurgewaveQuicProtocolPlugin : IProtocolPlugin
{
    public string FeatureId => "Surgewave.Protocol.Quic";
    public string DisplayName => "Raw QUIC Transport";
    public int DefaultPort => 9094;

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:Quic:Enabled", false);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
        => services.AddSurgewaveQuic(configuration);
}
