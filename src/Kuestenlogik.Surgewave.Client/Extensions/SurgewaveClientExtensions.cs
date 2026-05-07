using Kuestenlogik.Surgewave.Client.Abstractions;

namespace Kuestenlogik.Surgewave.Client.Extensions;

/// <summary>
/// Extension methods for ISurgewaveClient to access protocol-specific features.
/// </summary>
public static class SurgewaveClientExtensions
{
    /// <summary>
    /// Check if the client is using Surgewave Native protocol.
    /// </summary>
    public static bool IsSurgewaveNative(this ISurgewaveClient client)
        => client.Protocol == ProtocolType.SurgewaveNative;

    /// <summary>
    /// Check if the client is using Kafka protocol.
    /// </summary>
    public static bool IsKafka(this ISurgewaveClient client)
        => client.Protocol == ProtocolType.Kafka;

    /// <summary>
    /// Cast to SurgewaveClient if using Surgewave Native protocol.
    /// Returns null if using Kafka protocol.
    /// </summary>
    public static SurgewaveClient? AsSurgewaveClient(this ISurgewaveClient client)
        => client as SurgewaveClient;
}
