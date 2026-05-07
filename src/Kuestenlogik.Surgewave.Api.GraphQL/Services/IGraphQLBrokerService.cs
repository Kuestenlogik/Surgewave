using Kuestenlogik.Surgewave.Api.GraphQL.Types;

namespace Kuestenlogik.Surgewave.Api.GraphQL.Services;

/// <summary>
/// Abstraction over broker internals for use by GraphQL resolvers.
/// Decouples GraphQL types from the broker's LogManager, RecordBatchSerializer, etc.
/// </summary>
public interface IGraphQLBrokerService
{
    /// <summary>
    /// List all topics with metadata.
    /// </summary>
    Task<IReadOnlyList<TopicType>> GetTopicsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get messages from a topic partition starting at a given offset.
    /// </summary>
    Task<IReadOnlyList<MessageType>> GetMessagesAsync(
        string topic,
        int? partition = null,
        long? offset = null,
        int limit = 10,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// List all consumer groups.
    /// </summary>
    Task<IReadOnlyList<ConsumerGroupType>> GetConsumerGroupsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get cluster information.
    /// </summary>
    Task<ClusterInfoType> GetClusterInfoAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Produce a message to a topic and return the produced message info.
    /// </summary>
    Task<MessageType> ProduceMessageAsync(
        string topic,
        string? key,
        string value,
        int partition = 0,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Create a new topic.
    /// </summary>
    Task<TopicType> CreateTopicAsync(
        string name,
        int partitions = 1,
        int replicationFactor = 1,
        CancellationToken cancellationToken = default);
}
