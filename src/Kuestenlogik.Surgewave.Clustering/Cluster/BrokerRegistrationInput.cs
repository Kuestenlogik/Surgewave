namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Neutral input for a broker registration RPC (#59 b5). Carries only the values the
/// wire request is built from; the Kafka-DTO construction stays inside the concrete
/// <c>BrokerLifecycleManager</c> (in the Kafka plugin, #59 b5).
/// </summary>
/// <param name="BrokerId">The registering broker's id.</param>
/// <param name="ClusterId">The cluster id of the broker process.</param>
/// <param name="IncarnationId">The broker process incarnation id.</param>
/// <param name="Listeners">The listeners advertised by this broker.</param>
/// <param name="Features">The feature ranges supported by this broker.</param>
/// <param name="Rack">The rack this broker is in, or null.</param>
/// <param name="PreviousBrokerEpoch">The epoch before a clean shutdown, or -1.</param>
public sealed record BrokerRegistrationInput(
    int BrokerId,
    string ClusterId,
    Guid IncarnationId,
    IReadOnlyList<ListenerSpec> Listeners,
    IReadOnlyList<FeatureSpec> Features,
    string? Rack,
    long PreviousBrokerEpoch);
