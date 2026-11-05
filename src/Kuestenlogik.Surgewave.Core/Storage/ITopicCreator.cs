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
    /// <exception cref="InvalidOperationException">The topic exists already.</exception>
    /// <exception cref="TopicNotCreatedException">No controller took the topic now; asking again may.</exception>
    Task<TopicMetadata> CreateTopicAsync(
        string topicName,
        int partitionCount,
        short replicationFactor,
        Dictionary<string, string>? config,
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
