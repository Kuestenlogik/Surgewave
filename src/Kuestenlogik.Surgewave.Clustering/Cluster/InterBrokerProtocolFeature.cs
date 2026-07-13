namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// #60 Inc3 — the IBP-style (inter-broker-protocol) feature that negotiates whether inter-broker
/// RPCs travel over the legacy Kafka wire (<see cref="KafkaWire"/>) or the native SRWV protocol
/// (<see cref="Native"/>).
/// <para>
/// It is advertised in the broker-registration feature list (as
/// <see cref="LocalFeatureSpec"/>) and finalized cluster-wide as the MIN over every registered
/// broker's advertised level (see <see cref="ClusterState.FinalizedInterBrokerProtocol"/>). A broker
/// that never advertised the feature (an older build) reads as <see cref="KafkaWire"/>, so the
/// cluster stays pinned to the Kafka wire while any peer that cannot speak native is present. This
/// increment only advertises, stores and exposes the level — nothing selects transport on it yet.
/// </para>
/// </summary>
public static class InterBrokerProtocolFeature
{
    /// <summary>Feature name carried in the broker-registration feature list.</summary>
    public const string FeatureName = "inter.broker.protocol";

    /// <summary>Inter-broker RPCs travel over the legacy Kafka wire. The safe default / anchor.</summary>
    public const short KafkaWire = 0;

    /// <summary>Inter-broker RPCs travel over the native SRWV protocol.</summary>
    public const short Native = 1;

    /// <summary>The highest inter-broker protocol level this build can speak.</summary>
    public const short LocalMaxSupported = Native;

    /// <summary>
    /// The feature range this broker advertises during registration: <c>[KafkaWire, LocalMaxSupported]</c>.
    /// The minimum is always <see cref="KafkaWire"/> so a controller/peer can always negotiate the cluster
    /// down to the Kafka wire.
    /// </summary>
    public static FeatureSpec LocalFeatureSpec => new(FeatureName, KafkaWire, LocalMaxSupported);

    /// <summary>
    /// Resolve the inter-broker protocol level a broker supports from its advertised feature ranges:
    /// the <see cref="FeatureSpec.MaxSupportedVersion"/> of the <see cref="FeatureName"/> feature, or
    /// <see cref="KafkaWire"/> when the feature is absent (an older broker that never advertised it).
    /// </summary>
    public static short LevelFrom(IReadOnlyList<FeatureSpec> features)
    {
        for (var i = 0; i < features.Count; i++)
        {
            if (string.Equals(features[i].Name, FeatureName, StringComparison.Ordinal))
                return features[i].MaxSupportedVersion;
        }

        return KafkaWire;
    }
}
