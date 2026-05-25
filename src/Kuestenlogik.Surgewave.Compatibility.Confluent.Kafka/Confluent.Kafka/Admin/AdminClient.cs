using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A Confluent.Kafka-compatible admin client that wraps Surgewave.Client.
/// </summary>
internal sealed class AdminClient : IAdminClient
{
    private readonly ISurgewaveClient _client;
    private bool _disposed;

    internal AdminClient(ISurgewaveClient client)
    {
        _client = client;
        Name = $"surgewave-admin-{Guid.NewGuid():N}";
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public async Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Surgewave would implement this via native client
        // For now, this is a placeholder that would call Surgewave's topic creation API
        await Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task CreatePartitionsAsync(
        IEnumerable<PartitionsSpecification> partitionsSpecifications,
        CreatePartitionsOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task<DescribeClusterResult> DescribeClusterAsync(DescribeClusterOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Parse bootstrap servers to provide basic cluster info
        var parts = _client.BootstrapServers.Split(':');
        var host = parts[0];
        var port = parts.Length > 1 ? int.Parse(parts[1]) : 9092;

        return await Task.FromResult(new DescribeClusterResult
        {
            ClusterId = "surgewave-cluster",
            Controller = new Node { Id = 0, Host = host, Port = port },
            Nodes = [new Node { Id = 0, Host = host, Port = port }]
        });
    }

    /// <inheritdoc/>
    public async Task<DescribeTopicsResult> DescribeTopicsAsync(
        TopicCollection topicCollection,
        DescribeTopicsOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var descriptions = topicCollection.TopicNames.Select(name => new TopicDescription
        {
            Name = name,
            IsInternal = name.StartsWith("__", StringComparison.Ordinal),
            Partitions = [new TopicPartitionInfo { Partition = 0 }]
        }).ToList();

        return await Task.FromResult(new DescribeTopicsResult { TopicDescriptions = descriptions });
    }

    /// <inheritdoc/>
    public async Task<ListConsumerGroupsResult> ListConsumerGroupsAsync(ListConsumerGroupsOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return await Task.FromResult(new ListConsumerGroupsResult());
    }

    /// <inheritdoc/>
    public async Task<DescribeConsumerGroupsResult> DescribeConsumerGroupsAsync(
        IEnumerable<string> groups,
        DescribeConsumerGroupsOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var descriptions = groups.Select(g => new ConsumerGroupDescription
        {
            GroupId = g,
            State = ConsumerGroupState.Stable
        }).ToList();

        return await Task.FromResult(new DescribeConsumerGroupsResult { ConsumerGroupDescriptions = descriptions });
    }

    /// <inheritdoc/>
    public async Task DeleteGroupsAsync(IEnumerable<string> groups, DeleteGroupsOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task<ListConsumerGroupOffsetsResult> ListConsumerGroupOffsetsAsync(
        IEnumerable<ConsumerGroupTopicPartitions> groupPartitions,
        ListConsumerGroupOffsetsOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return await Task.FromResult(new ListConsumerGroupOffsetsResult());
    }

    /// <inheritdoc/>
    public async Task AlterConsumerGroupOffsetsAsync(
        IEnumerable<ConsumerGroupTopicPartitionOffsets> groupOffsets,
        AlterConsumerGroupOffsetsOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task<List<DeleteRecordsResult>> DeleteRecordsAsync(
        IEnumerable<TopicPartitionOffset> topicPartitionOffsets,
        DeleteRecordsOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return await Task.FromResult(new List<DeleteRecordsResult>());
    }

    /// <inheritdoc/>
    public Metadata GetMetadata(string? topic, TimeSpan timeout)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var parts = _client.BootstrapServers.Split(':');
        var host = parts[0];
        var port = parts.Length > 1 ? int.Parse(parts[1]) : 9092;

        var metadata = new Metadata
        {
            Brokers = [new BrokerMetadata { BrokerId = 0, Host = host, Port = port }],
            OriginatingBrokerId = 0,
            OriginatingBrokerName = $"{host}:{port}"
        };

        if (topic is not null)
        {
            metadata.Topics.Add(new TopicMetadata
            {
                Topic = topic,
                Partitions = [new PartitionMetadata { PartitionId = 0, Leader = 0, Replicas = [0], InSyncReplicas = [0] }]
            });
        }

        return metadata;
    }

    /// <inheritdoc/>
    public Metadata GetMetadata(TimeSpan timeout) => GetMetadata(null, timeout);

    /// <inheritdoc/>
    public int AddBrokers(string brokers) => 0;

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
