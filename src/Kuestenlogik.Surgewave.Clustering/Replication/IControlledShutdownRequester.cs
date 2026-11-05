using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Sending half of a controlled shutdown (#180): a departing broker asking the controller to take
/// its partition leaderships away.
/// </summary>
/// <remarks>
/// The counterpart of <see cref="IControlledShutdownCoordinator"/>, kept as its own interface so
/// the controller does not depend on a transport and the shutdown path can be exercised without
/// one.
/// </remarks>
public interface IControlledShutdownRequester
{
    /// <summary>
    /// Asks the controller to move every partition <paramref name="brokerId"/> leads. Returns the
    /// partitions it still leads afterwards — empty when all of them moved — or
    /// <see langword="null"/> when the controller could not be reached or refused, in which case
    /// the caller has nothing better than the heartbeat timeout to fall back on.
    /// </summary>
    Task<IReadOnlyList<TopicPartition>?> RequestControlledShutdownAsync(
        int brokerId, long brokerEpoch, CancellationToken ct = default);
}

/// <summary>
/// How a broker that is not the controller has a group config changed (#228): it asks the controller, which writes the
/// change to the metadata log.
/// </summary>
public interface IGroupConfigRequester
{
    /// <summary>
    /// Asks the controller to set one config of a group, or remove it with a null value (#228). True once the
    /// controller committed it; false when it refused or could not be reached — the client retries.
    /// </summary>
    Task<bool> RequestSetGroupConfigAsync(string groupId, string name, string? value, CancellationToken ct = default);
}

/// <summary>
/// How a broker that is not the controller has a topic's config changed (#119): it asks the controller, which writes the
/// change to the metadata log.
/// </summary>
public interface ITopicConfigRequester
{
    /// <summary>
    /// Asks the controller to set and remove configs of a topic. True once the controller committed it; false when it
    /// refused or could not be reached — the client retries.
    /// </summary>
    Task<bool> RequestChangeTopicConfigAsync(
        string topic, IReadOnlyDictionary<string, string> set, IReadOnlyList<string> removed, CancellationToken ct = default);
}

/// <summary>
/// How a broker that is not the controller has a topic created (#207): it asks the controller, which writes the topic
/// to the metadata log. A broker never creates a cluster topic for itself alone.
/// </summary>
public interface ITopicCreationRequester
{
    /// <summary>
    /// Asks the controller to create the topic. True once the controller created it; false when it refused, or could
    /// not be reached — the client is answered with a retriable error and asks again.
    /// </summary>
    Task<bool> RequestCreateTopicAsync(
        string topic, int partitions, short replicationFactor, IReadOnlyDictionary<string, string>? config, CancellationToken ct = default);
}
