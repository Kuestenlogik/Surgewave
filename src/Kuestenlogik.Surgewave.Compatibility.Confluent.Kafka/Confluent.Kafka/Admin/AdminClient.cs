using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Commands;
using NativeCluster = Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A Confluent.Kafka-compatible admin client that wraps Surgewave.Client.
/// All operations are executed against the broker through the Surgewave
/// native protocol (<see cref="SurgewaveNativeClient"/>). Operations the
/// native protocol cannot express throw <see cref="NotSupportedException"/>
/// instead of faking success.
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

    /// <summary>
    /// The connected native client, or a loud failure when the underlying
    /// client speaks the Kafka wire protocol (admin RPCs over the Kafka wire
    /// are not implemented by this compatibility layer).
    /// </summary>
    private SurgewaveNativeClient Native
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _client.NativeClient
                ?? throw new NotSupportedException(
                    $"Admin operations require the Surgewave native protocol, but the underlying client uses '{_client.Protocol}'. " +
                    "Kafka-wire admin RPCs (CreateTopics, DeleteTopics, ...) are not implemented by this compatibility layer. " +
                    "Connect to a Surgewave broker or set 'surgewave.protocol' to 'surgewave'.");
        }
    }

    /// <inheritdoc/>
    public async Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions? options = null)
    {
        var native = Native;

        if (options?.ValidateOnly == true)
        {
            throw new NotSupportedException(
                "CreateTopicsOptions.ValidateOnly is not supported: the Surgewave native protocol has no dry-run topic creation.");
        }

        using var cts = CreateTimeoutSource(options?.RequestTimeout);
        foreach (var spec in topics)
        {
            if (spec.ReplicasAssignments is { Count: > 0 })
            {
                throw new NotSupportedException(
                    $"TopicSpecification.ReplicasAssignments (topic '{spec.Name}') is not supported: " +
                    "Surgewave assigns replicas automatically. Use NumPartitions and ReplicationFactor instead.");
            }

            if (spec.NumPartitions <= 0)
            {
                throw new NotSupportedException(
                    $"NumPartitions={spec.NumPartitions} for topic '{spec.Name}' is not supported: " +
                    "broker-side partition defaults (-1) are not available over the Surgewave native protocol. Specify an explicit count.");
            }

            if (spec.ReplicationFactor <= 0)
            {
                throw new NotSupportedException(
                    $"ReplicationFactor={spec.ReplicationFactor} for topic '{spec.Name}' is not supported: " +
                    "broker-side replication defaults (-1) are not available over the Surgewave native protocol. Specify an explicit factor.");
            }

            await native.Topics.CreateAsync(spec.Name, spec.NumPartitions, spec.ReplicationFactor, cts.Token)
                .ConfigureAwait(false);

            if (spec.Configs is { Count: > 0 })
            {
                await native.Topics.AlterConfigAsync(spec.Name, spec.Configs, cts.Token).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc/>
    public async Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions? options = null)
    {
        var native = Native;

        using var cts = CreateTimeoutSource(options?.RequestTimeout);
        foreach (var topic in topics)
        {
            await native.Topics.DeleteAsync(topic, cts.Token).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async Task CreatePartitionsAsync(
        IEnumerable<PartitionsSpecification> partitionsSpecifications,
        CreatePartitionsOptions? options = null)
    {
        var native = Native;

        if (options?.ValidateOnly == true)
        {
            throw new NotSupportedException(
                "CreatePartitionsOptions.ValidateOnly is not supported: the Surgewave native protocol has no dry-run partition creation.");
        }

        using var cts = CreateTimeoutSource(options?.RequestTimeout);
        foreach (var spec in partitionsSpecifications)
        {
            if (spec.ReplicaAssignments is { Count: > 0 })
            {
                throw new NotSupportedException(
                    $"PartitionsSpecification.ReplicaAssignments (topic '{spec.Topic}') is not supported: " +
                    "Surgewave assigns replicas for new partitions automatically.");
            }

            await native.Topics.CreatePartitionsAsync(spec.Topic, spec.IncreaseTo, cts.Token).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async Task<DescribeClusterResult> DescribeClusterAsync(DescribeClusterOptions? options = null)
    {
        var native = Native;

        using var cts = CreateTimeoutSource(options?.RequestTimeout);
        var clusterInfo = await native.Cluster.GetClusterInfoAsync(cts.Token).ConfigureAwait(false);
        var brokers = await native.Cluster.ListBrokersAsync(cts.Token).ConfigureAwait(false);

        var nodes = brokers.Select(ToNode).ToList();
        var controller = nodes.FirstOrDefault(n => n.Id == clusterInfo.ControllerId);

        return new DescribeClusterResult
        {
            // The native protocol does not expose a cluster-id string; this
            // mirrors the broker's default Kafka-side ClusterId.
            ClusterId = "surgewave-cluster",
            Controller = controller,
            Nodes = nodes
        };
    }

    /// <inheritdoc/>
    public async Task<DescribeTopicsResult> DescribeTopicsAsync(
        TopicCollection topicCollection,
        DescribeTopicsOptions? options = null)
    {
        var native = Native;

        using var cts = CreateTimeoutSource(options?.RequestTimeout);
        var brokersById = (await native.Cluster.ListBrokersAsync(cts.Token).ConfigureAwait(false))
            .ToDictionary(b => b.BrokerId, ToNode);

        Node NodeFor(int brokerId) =>
            brokersById.TryGetValue(brokerId, out var node) ? node : new Node { Id = brokerId };

        var descriptions = new List<TopicDescription>();
        foreach (var name in topicCollection.TopicNames)
        {
            try
            {
                var described = await native.Topics.DescribeAsync(name, cts.Token).ConfigureAwait(false);
                descriptions.Add(new TopicDescription
                {
                    Name = described.Name,
                    IsInternal = described.IsInternal,
                    Partitions = described.Partitions.Select(p => new TopicPartitionInfo
                    {
                        Partition = p.PartitionId,
                        Leader = NodeFor(p.Leader),
                        Replicas = p.Replicas.Select(NodeFor).ToList(),
                        Isr = p.Isr.Select(NodeFor).ToList()
                    }).ToList()
                });
            }
            catch (SurgewaveProtocolException ex)
            {
                // Surface the broker error per topic instead of dropping the row.
                descriptions.Add(new TopicDescription
                {
                    Name = name,
                    Error = new Error(ErrorCode.Unknown, ex.Message)
                });
            }
        }

        return new DescribeTopicsResult { TopicDescriptions = descriptions };
    }

    /// <inheritdoc/>
    public async Task<ListConsumerGroupsResult> ListConsumerGroupsAsync(ListConsumerGroupsOptions? options = null)
    {
        var native = Native;

        using var cts = CreateTimeoutSource(options?.RequestTimeout);
        var groups = await native.Groups.ListAsync(cts.Token).ConfigureAwait(false);

        var listings = groups.Select(g => new ConsumerGroupListing
        {
            GroupId = g.GroupId,
            IsSimpleConsumerGroup = string.IsNullOrEmpty(g.ProtocolType),
            State = ParseGroupState(g.State)
        });

        if (options?.MatchStates is not null)
        {
            var states = options.MatchStates.ToHashSet();
            listings = listings.Where(l => states.Contains(l.State));
        }

        return new ListConsumerGroupsResult { Valid = listings.ToList() };
    }

    /// <inheritdoc/>
    public async Task<DescribeConsumerGroupsResult> DescribeConsumerGroupsAsync(
        IEnumerable<string> groups,
        DescribeConsumerGroupsOptions? options = null)
    {
        var native = Native;

        using var cts = CreateTimeoutSource(options?.RequestTimeout);

        // With the native protocol the connected broker coordinates all groups.
        var clusterInfo = await native.Cluster.GetClusterInfoAsync(cts.Token).ConfigureAwait(false);
        var coordinator = new Node { Id = clusterInfo.BrokerId, Host = clusterInfo.Host, Port = clusterInfo.Port };

        var descriptions = new List<ConsumerGroupDescription>();
        foreach (var groupId in groups)
        {
            var described = await native.Groups.DescribeAsync(groupId, cts.Token).ConfigureAwait(false);
            descriptions.Add(new ConsumerGroupDescription
            {
                GroupId = described.GroupId,
                IsSimpleConsumerGroup = string.IsNullOrEmpty(described.ProtocolType),
                State = ParseGroupState(described.State),
                PartitionAssignor = described.ProtocolName,
                Coordinator = coordinator,
                Members = described.Members.Select(m => new MemberDescription
                {
                    MemberId = m.MemberId,
                    ClientId = m.ClientId,
                    GroupInstanceId = m.GroupInstanceId,
                    Assignment = ParseMemberAssignment(m.Assignment)
                }).ToList(),
                Error = described.ErrorCode != 0
                    ? new Error(ErrorCode.Unknown, $"Broker returned error code {described.ErrorCode} for group '{groupId}'")
                    : null
            });
        }

        return new DescribeConsumerGroupsResult { ConsumerGroupDescriptions = descriptions };
    }

    /// <inheritdoc/>
    public async Task DeleteGroupsAsync(IEnumerable<string> groups, DeleteGroupsOptions? options = null)
    {
        var native = Native;

        using var cts = CreateTimeoutSource(options?.RequestTimeout);
        foreach (var groupId in groups)
        {
            await native.Groups.DeleteAsync(groupId, cts.Token).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async Task<ListConsumerGroupOffsetsResult> ListConsumerGroupOffsetsAsync(
        IEnumerable<ConsumerGroupTopicPartitions> groupPartitions,
        ListConsumerGroupOffsetsOptions? options = null)
    {
        var native = Native;

        using var cts = CreateTimeoutSource(options?.RequestTimeout);
        var result = new ListConsumerGroupOffsetsResult();

        foreach (var group in groupPartitions)
        {
            var offsets = new List<TopicPartitionOffset>();

            if (group.Partitions is not null)
            {
                foreach (var tp in group.Partitions)
                {
                    var offset = await native.Groups
                        .FetchOffsetAsync(group.Group, tp.Topic, tp.Partition.Value, cts.Token)
                        .ConfigureAwait(false);
                    offsets.Add(new TopicPartitionOffset(tp, new Offset(offset)));
                }
            }
            else
            {
                // No explicit partitions: report every partition the group has offsets for.
                var lag = await native.Groups.GetLagAsync(group.Group, cts.Token).ConfigureAwait(false);
                foreach (var topicLag in lag.Topics)
                {
                    foreach (var partitionLag in topicLag.Partitions)
                    {
                        offsets.Add(new TopicPartitionOffset(
                            topicLag.Topic,
                            new Partition(partitionLag.Partition),
                            new Offset(partitionLag.CommittedOffset)));
                    }
                }
            }

            result.Groups.Add(new ConsumerGroupTopicPartitionOffsets
            {
                Group = group.Group,
                Partitions = offsets
            });
        }

        return result;
    }

    /// <inheritdoc/>
    public async Task AlterConsumerGroupOffsetsAsync(
        IEnumerable<ConsumerGroupTopicPartitionOffsets> groupOffsets,
        AlterConsumerGroupOffsetsOptions? options = null)
    {
        var native = Native;

        using var cts = CreateTimeoutSource(options?.RequestTimeout);
        foreach (var group in groupOffsets)
        {
            foreach (var tpo in group.Partitions)
            {
                // Administrative commit: no group membership, matching Kafka's
                // simple-commit semantics (empty member id, generation -1).
                await native.Groups.CommitOffsetAsync(
                    group.Group,
                    memberId: string.Empty,
                    generationId: -1,
                    tpo.Topic,
                    tpo.Partition.Value,
                    tpo.Offset.Value,
                    cts.Token).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc/>
    public async Task<List<DeleteRecordsResult>> DeleteRecordsAsync(
        IEnumerable<TopicPartitionOffset> topicPartitionOffsets,
        DeleteRecordsOptions? options = null)
    {
        var native = Native;

        using var cts = CreateTimeoutSource(options?.RequestTimeout);
        var results = new List<DeleteRecordsResult>();

        foreach (var tpo in topicPartitionOffsets)
        {
            var lowWatermark = await native.Topics
                .DeleteRecordsAsync(tpo.Topic, tpo.Partition.Value, tpo.Offset.Value, cts.Token)
                .ConfigureAwait(false);

            results.Add(new DeleteRecordsResult
            {
                Partition = tpo.TopicPartition,
                Offset = new Offset(lowWatermark)
            });
        }

        return results;
    }

    /// <inheritdoc/>
    public Metadata GetMetadata(string? topic, TimeSpan timeout)
        => GetMetadataAsync(topic, timeout).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public Metadata GetMetadata(TimeSpan timeout) => GetMetadata(null, timeout);

    private async Task<Metadata> GetMetadataAsync(string? topic, TimeSpan timeout)
    {
        var native = Native;

        using var cts = new CancellationTokenSource(timeout);
        var clusterInfo = await native.Cluster.GetClusterInfoAsync(cts.Token).ConfigureAwait(false);
        var brokers = await native.Cluster.ListBrokersAsync(cts.Token).ConfigureAwait(false);

        var metadata = new Metadata
        {
            Brokers = brokers.Select(b => new BrokerMetadata
            {
                BrokerId = b.BrokerId,
                Host = b.Host,
                Port = b.Port
            }).ToList(),
            OriginatingBrokerId = clusterInfo.BrokerId,
            OriginatingBrokerName = $"{clusterInfo.Host}:{clusterInfo.Port}"
        };

        List<string> topicNames = topic is not null
            ? [topic]
            : (await native.Topics.ListAsync(cts.Token).ConfigureAwait(false)).Select(t => t.Name).ToList();

        foreach (var name in topicNames)
        {
            var described = await native.Topics.DescribeAsync(name, cts.Token).ConfigureAwait(false);
            metadata.Topics.Add(new TopicMetadata
            {
                Topic = described.Name,
                Partitions = described.Partitions.Select(p => new PartitionMetadata
                {
                    PartitionId = p.PartitionId,
                    Leader = p.Leader,
                    Replicas = p.Replicas,
                    InSyncReplicas = p.Isr
                }).ToList()
            });
        }

        return metadata;
    }

    /// <inheritdoc/>
    public int AddBrokers(string brokers) =>
        throw new NotSupportedException(
            "AddBrokers is not supported: the Surgewave client connects to the bootstrap servers given at construction time. " +
            "Pass all brokers via BootstrapServers instead.");

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private static CancellationTokenSource CreateTimeoutSource(TimeSpan? requestTimeout)
        => new(requestTimeout ?? Timeout.InfiniteTimeSpan);

    private static Node ToNode(NativeCluster.BrokerInfo broker) => new()
    {
        Id = broker.BrokerId,
        Host = broker.Host,
        Port = broker.Port,
        Rack = broker.Rack
    };

    private static ConsumerGroupState ParseGroupState(string state) =>
        Enum.TryParse<ConsumerGroupState>(
            state.Replace(" ", string.Empty, StringComparison.Ordinal), ignoreCase: true, out var parsed)
            ? parsed
            : ConsumerGroupState.Unknown;

    private static MemberAssignment? ParseMemberAssignment(byte[] assignment)
    {
        if (assignment.Length == 0)
        {
            return null;
        }

        try
        {
            var partitions = ConsumerProtocolCodec.ParseAssignment(assignment);
            return new MemberAssignment
            {
                TopicPartitions = partitions
                    .Select(p => new TopicPartition(p.Topic, new Partition(p.Partition)))
                    .ToList()
            };
        }
        catch (ArgumentOutOfRangeException)
        {
            // Assignment bytes not in Kafka ConsumerProtocol format — do not invent data.
            return null;
        }
    }
}
