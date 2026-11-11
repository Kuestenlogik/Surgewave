using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Where a broker in a cluster creates its topics (#214): through the controller into the metadata log, which every
/// broker reads and replays at its start. <see cref="LogManager.CreateTopicAsync"/> hands every creation to it once it
/// is set, whatever the entry point — Kafka, gRPC, the native protocol, GraphQL or an internal topic.
/// </summary>
public interface ITopicCreator
{
    /// <summary>
    /// Creates the topic and completes once this broker knows it, with the topic as this broker knows it.
    /// </summary>
    /// <param name="replicaAssignment">
    /// The replicas of each partition, by partition index, the preferred leader first, as a client chose them (#202);
    /// <see langword="null"/> to have the controller place them. When given, it has
    /// <paramref name="partitionCount"/> entries of <paramref name="replicationFactor"/> distinct broker ids each.
    /// </param>
    /// <exception cref="InvalidOperationException">The topic exists already.</exception>
    /// <exception cref="TopicNotCreatedException">No controller took the topic now; asking again may.</exception>
    /// <exception cref="InvalidReplicaAssignmentException">The assignment names a broker the cluster does not have.</exception>
    Task<TopicMetadata> CreateTopicAsync(
        string topicName,
        int partitionCount,
        short replicationFactor,
        Dictionary<string, string>? config,
        IReadOnlyList<IReadOnlyList<int>>? replicaAssignment,
        CancellationToken cancellationToken);
}

/// <summary>
/// Where a broker in a cluster changes its topics' configs (#119): through the controller into the metadata log, from
/// which every broker hosting the topic's replicas applies it. <see cref="LogManager.UpdateTopicConfigAsync"/> hands
/// every change to it once it is set.
/// </summary>
public interface ITopicConfigWriter
{
    /// <summary>
    /// Sets and removes configs of the topic and completes once this broker has the change. False when the topic is
    /// unknown.
    /// </summary>
    /// <exception cref="TopicConfigNotChangedException">No controller took the change now; asking again may.</exception>
    Task<bool> ChangeTopicConfigAsync(
        string topicName, IReadOnlyDictionary<string, string> set, IReadOnlyList<string> removed, CancellationToken cancellationToken);
}

/// <summary>No controller took the config change now — it was changing hands or not reachable; asking again may.</summary>
public sealed class TopicConfigNotChangedException(string topicName)
    : Exception($"The controller did not change the config of topic '{topicName}' now; retry")
{
    /// <summary>The topic whose config was not changed.</summary>
    public string TopicName { get; } = topicName;
}

/// <summary>No controller took the topic now — it was changing hands or not reachable; asking again may.</summary>
public sealed class TopicNotCreatedException(string topicName)
    : Exception($"The controller did not create topic '{topicName}' now; retry")
{
    /// <summary>The topic that was not created.</summary>
    public string TopicName { get; } = topicName;
}

/// <summary>
/// A client's replica assignment names a broker the cluster does not have (#202). The message is Kafka's.
/// </summary>
public sealed class InvalidReplicaAssignmentException(string message) : Exception(message)
{
    /// <summary>
    /// The first broker of <paramref name="replicaAssignment"/>, in ascending order per partition as Kafka checks
    /// them, that <paramref name="isRegistered"/> does not know; null when it knows them all.
    /// </summary>
    public static InvalidReplicaAssignmentException? ForUnknownBroker(
        IReadOnlyList<IReadOnlyList<int>> replicaAssignment, Func<int, bool> isRegistered)
    {
        foreach (var replicas in replicaAssignment)
        {
            foreach (var brokerId in replicas.Order())
            {
                if (!isRegistered(brokerId))
                    return new InvalidReplicaAssignmentException(
                        $"The manual partition assignment includes broker {brokerId}, but no such broker is registered.");
            }
        }
        return null;
    }
}
