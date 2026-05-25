using Kuestenlogik.Surgewave.Client.Native.Commands;
using Kuestenlogik.Surgewave.Client.Native.Commands.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Topics;

/// <summary>
/// Topic management operations for Surgewave native client.
/// </summary>
public sealed class SurgewaveTopicOperations
{
    private readonly SurgewaveNativeClient _client;
    private readonly CommandExecutor _executor;

    internal SurgewaveTopicOperations(SurgewaveNativeClient client)
    {
        _client = client;
        _executor = new CommandExecutor(client);
    }

    /// <summary>
    /// Create a new topic.
    /// </summary>
    public Task CreateAsync(string name, int partitions, short replicationFactor = 1, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new CreateTopicCommand(name, partitions, replicationFactor), cancellationToken);

    /// <summary>
    /// Create a topic with fluent configuration.
    /// </summary>
    public TopicCreateBuilder Create(string name) => new(_client, name);

    /// <summary>
    /// Delete a topic.
    /// </summary>
    public Task DeleteAsync(string name, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new DeleteTopicCommand(name), cancellationToken);

    /// <summary>
    /// List all topics.
    /// </summary>
    public Task<List<TopicInfo>> ListAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new ListTopicsCommand(), cancellationToken);

    /// <summary>
    /// Alter topic configuration.
    /// </summary>
    public Task AlterConfigAsync(string name, Dictionary<string, string> config, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new AlterTopicConfigCommand(name, config), cancellationToken);

    /// <summary>
    /// Get topic configuration.
    /// </summary>
    public Task<Dictionary<string, string>> DescribeConfigAsync(string name, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new DescribeTopicConfigCommand(name), cancellationToken);

    /// <summary>
    /// Describe a topic with full partition metadata including leader, replicas, and ISR.
    /// </summary>
    public Task<TopicDescription> DescribeAsync(string name, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new DescribeTopicCommand(name), cancellationToken);

    /// <summary>
    /// Add partitions to an existing topic.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="totalPartitions">The new total number of partitions (must be greater than current count).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public Task CreatePartitionsAsync(string name, int totalPartitions, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new CreatePartitionsCommand(name, totalPartitions), cancellationToken);

    /// <summary>
    /// Delete records from a topic partition up to (but not including) the specified offset.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="partition">The partition number.</param>
    /// <param name="beforeOffset">Records with offsets less than this will be deleted.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new log start offset (low watermark) after deletion.</returns>
    public Task<long> DeleteRecordsAsync(string name, int partition, long beforeOffset, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new DeleteRecordsCommand(name, partition, beforeOffset), cancellationToken);
}
