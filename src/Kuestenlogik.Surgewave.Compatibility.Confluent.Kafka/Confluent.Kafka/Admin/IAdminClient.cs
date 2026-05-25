namespace Confluent.Kafka.Admin;

/// <summary>
/// Defines administrative operations for Apache Kafka.
/// </summary>
public interface IAdminClient : IDisposable
{
    /// <summary>
    /// Gets the admin client name.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Create topics.
    /// </summary>
    /// <param name="topics">Topics to create.</param>
    /// <param name="options">Creation options.</param>
    /// <returns>Task that completes when topics are created.</returns>
    Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions? options = null);

    /// <summary>
    /// Delete topics.
    /// </summary>
    /// <param name="topics">Topic names to delete.</param>
    /// <param name="options">Deletion options.</param>
    /// <returns>Task that completes when topics are deleted.</returns>
    Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions? options = null);

    /// <summary>
    /// Create partitions for existing topics.
    /// </summary>
    /// <param name="partitionsSpecifications">Partition specifications.</param>
    /// <param name="options">Creation options.</param>
    /// <returns>Task that completes when partitions are created.</returns>
    Task CreatePartitionsAsync(
        IEnumerable<PartitionsSpecification> partitionsSpecifications,
        CreatePartitionsOptions? options = null);

    /// <summary>
    /// Describe cluster configuration.
    /// </summary>
    /// <param name="options">Describe options.</param>
    /// <returns>Cluster metadata.</returns>
    Task<DescribeClusterResult> DescribeClusterAsync(DescribeClusterOptions? options = null);

    /// <summary>
    /// Describe topics.
    /// </summary>
    /// <param name="topicCollection">Topics to describe.</param>
    /// <param name="options">Describe options.</param>
    /// <returns>Topic descriptions.</returns>
    Task<DescribeTopicsResult> DescribeTopicsAsync(TopicCollection topicCollection, DescribeTopicsOptions? options = null);

    /// <summary>
    /// List consumer groups.
    /// </summary>
    /// <param name="options">List options.</param>
    /// <returns>Consumer group listings.</returns>
    Task<ListConsumerGroupsResult> ListConsumerGroupsAsync(ListConsumerGroupsOptions? options = null);

    /// <summary>
    /// Describe consumer groups.
    /// </summary>
    /// <param name="groups">Group IDs to describe.</param>
    /// <param name="options">Describe options.</param>
    /// <returns>Consumer group descriptions.</returns>
    Task<DescribeConsumerGroupsResult> DescribeConsumerGroupsAsync(
        IEnumerable<string> groups,
        DescribeConsumerGroupsOptions? options = null);

    /// <summary>
    /// Delete consumer groups.
    /// </summary>
    /// <param name="groups">Group IDs to delete.</param>
    /// <param name="options">Deletion options.</param>
    /// <returns>Task that completes when groups are deleted.</returns>
    Task DeleteGroupsAsync(IEnumerable<string> groups, DeleteGroupsOptions? options = null);

    /// <summary>
    /// List consumer group offsets.
    /// </summary>
    /// <param name="groupPartitions">Groups and their partitions.</param>
    /// <param name="options">List options.</param>
    /// <returns>Consumer group offsets.</returns>
    Task<ListConsumerGroupOffsetsResult> ListConsumerGroupOffsetsAsync(
        IEnumerable<ConsumerGroupTopicPartitions> groupPartitions,
        ListConsumerGroupOffsetsOptions? options = null);

    /// <summary>
    /// Alter consumer group offsets.
    /// </summary>
    /// <param name="groupOffsets">New offsets for groups.</param>
    /// <param name="options">Alter options.</param>
    /// <returns>Task that completes when offsets are altered.</returns>
    Task AlterConsumerGroupOffsetsAsync(
        IEnumerable<ConsumerGroupTopicPartitionOffsets> groupOffsets,
        AlterConsumerGroupOffsetsOptions? options = null);

    /// <summary>
    /// Delete records up to specified offsets.
    /// </summary>
    /// <param name="topicPartitionOffsets">Offsets up to which to delete.</param>
    /// <param name="options">Delete options.</param>
    /// <returns>Delete results.</returns>
    Task<List<DeleteRecordsResult>> DeleteRecordsAsync(
        IEnumerable<Confluent.Kafka.TopicPartitionOffset> topicPartitionOffsets,
        DeleteRecordsOptions? options = null);

    /// <summary>
    /// Get metadata for topics and brokers.
    /// </summary>
    /// <param name="topic">Specific topic or null for all topics.</param>
    /// <param name="timeout">Operation timeout.</param>
    /// <returns>Cluster metadata.</returns>
    Metadata GetMetadata(string? topic, TimeSpan timeout);

    /// <summary>
    /// Get metadata for all topics and brokers.
    /// </summary>
    /// <param name="timeout">Operation timeout.</param>
    /// <returns>Cluster metadata.</returns>
    Metadata GetMetadata(TimeSpan timeout);

    /// <summary>
    /// Adds brokers to the admin client's broker list.
    /// </summary>
    /// <param name="brokers">Comma-separated broker addresses.</param>
    /// <returns>Number of brokers added.</returns>
    int AddBrokers(string brokers);
}
