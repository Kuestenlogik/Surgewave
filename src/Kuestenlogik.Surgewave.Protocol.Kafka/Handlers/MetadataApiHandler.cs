using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Broker;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;

/// <summary>
/// Handler for metadata and cluster discovery APIs: Metadata, ApiVersions, FindCoordinator, DescribeCluster
/// </summary>
public sealed class MetadataApiHandler : IKafkaRequestHandler
{
    private readonly IBrokerConfigView _config;
    private readonly LogManager _logManager;
    private readonly ILogger<MetadataApiHandler> _logger;
    private ClusterState? _clusterState;
    private Func<int, bool> _isKnownFenced = _ => false;
    private ICoordinatorLocator? _coordinators;

    /// <summary>
    /// Sets the cluster state for returning accurate replica info in cluster mode.
    /// </summary>
    public void SetClusterState(ClusterState clusterState)
    {
        _clusterState = clusterState;
    }

    /// <summary>
    /// Sets how DescribeCluster learns whether a broker is fenced — ClusterMembershipService.IsKnownFenced.
    /// Not IsBrokerFenced: it answers "fenced" for every broker it has not registered, and only the controller
    /// registers brokers, so any other broker would report the whole cluster fenced.
    /// </summary>
    public void SetBrokerFencing(Func<int, bool> isKnownFenced)
    {
        _isKnownFenced = isKnownFenced;
    }

    /// <summary>
    /// Sets who coordinates which group or transactional id (#204): FindCoordinator names that broker. Unset, a broker
    /// on its own, which coordinates everything itself.
    /// </summary>
    public void SetCoordinatorLocator(ICoordinatorLocator coordinators)
    {
        _coordinators = coordinators;
    }

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.Metadata,
        ApiKey.ApiVersions,
        ApiKey.FindCoordinator,
        ApiKey.DescribeCluster,
        ApiKey.DescribeTopicPartitions,
    ];

    public MetadataApiHandler(
        IBrokerConfigView config,
        LogManager logManager,
        ILogger<MetadataApiHandler> logger)
    {
        _config = config;
        _logManager = logManager;
        _logger = logger;
    }

    public async Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return request switch
        {
            MetadataRequest metadataRequest => await HandleMetadataAsync(metadataRequest, cancellationToken),
            ApiVersionsRequest apiVersionsRequest => HandleApiVersions(apiVersionsRequest),
            FindCoordinatorRequest findCoordinatorRequest => HandleFindCoordinator(findCoordinatorRequest),
            DescribeClusterRequest describeClusterRequest => HandleDescribeCluster(describeClusterRequest),
            DescribeTopicPartitionsRequest describeTopicPartitionsRequest => HandleDescribeTopicPartitions(describeTopicPartitionsRequest),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by MetadataApiHandler")
        };
    }

    private ApiVersionsResponse HandleApiVersions(ApiVersionsRequest request)
    {
        // KIP-1242 — at v5+ a client may pin the cluster identity it expects
        // to be talking to (ClusterId + NodeId). If the broker the request
        // landed on doesn't match, return REBOOTSTRAP_REQUIRED so the client
        // re-resolves the bootstrap endpoint instead of silently continuing
        // against the wrong cluster (the motivating case: IP reuse after a
        // cluster is replaced — old DNS still points at it).
        if (request.ApiVersion >= 5)
        {
            var brokerClusterId = _config.ResolvedClusterId;
            if (!string.IsNullOrEmpty(request.ClusterId) &&
                !string.Equals(request.ClusterId, brokerClusterId, StringComparison.Ordinal))
            {
                return new ApiVersionsResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ErrorCode = ErrorCode.RebootstrapRequired,
                    ApiVersions = [],
                    ThrottleTimeMs = 0,
                };
            }
            if (request.NodeId != -1 && request.NodeId != _config.BrokerId)
            {
                return new ApiVersionsResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ErrorCode = ErrorCode.RebootstrapRequired,
                    ApiVersions = [],
                    ThrottleTimeMs = 0,
                };
            }
        }

        return ApiVersionsResponse.CreateDefault(request.CorrelationId, request.ApiVersion);
    }

    private FindCoordinatorResponse HandleFindCoordinator(FindCoordinatorRequest request)
    {
        // The coordinator the cluster agrees on, the same from every broker (#204). It used to be the broker asked,
        // so the members of one group could each find a different coordinator.
        var kind = request.KeyType switch
        {
            1 => CoordinatorKind.Transaction,
            2 => CoordinatorKind.Share,
            _ => CoordinatorKind.Group,
        };

        // v4+ looks up a batch of keys.
        if (request.ApiVersion >= 4)
        {
            var keys = request.CoordinatorKeys ?? (request.Key != null ? [request.Key] : []);
            return new FindCoordinatorResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                Coordinators = [.. keys.Select(key =>
                {
                    var location = Locate(kind, key);
                    var (error, message) = LookupError(location, key);
                    return new FindCoordinatorResponse.Coordinator
                    {
                        Key = key,
                        NodeId = location.BrokerId,
                        Host = location.Host,
                        Port = location.Port,
                        ErrorCode = error,
                        ErrorMessage = message,
                    };
                })],
            };
        }

        var single = Locate(kind, request.Key ?? string.Empty);
        var (singleError, singleMessage) = LookupError(single, request.Key ?? string.Empty);
        return new FindCoordinatorResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = singleError,
            ErrorMessage = singleMessage,
            NodeId = single.BrokerId,
            Host = single.Host,
            Port = single.Port,
        };
    }

    private CoordinatorLocation Locate(CoordinatorKind kind, string key) =>
        _coordinators?.Locate(kind, key)
        ?? new CoordinatorLocation(CoordinatorLookupStatus.Ok, _config.BrokerId, _config.Host, _config.Port);

    /// <summary>COORDINATOR_NOT_AVAILABLE while nobody coordinates the key yet; the client asks again.</summary>
    private static (ErrorCode Error, string? Message) LookupError(CoordinatorLocation location, string key) =>
        location.Status == CoordinatorLookupStatus.Ok
            ? (ErrorCode.None, null)
            : (ErrorCode.CoordinatorNotAvailable, $"No broker coordinates {key} yet.");

    /// <summary>
    /// A topic as the cluster knows it (#208): in a cluster every broker applies the metadata log, so the cluster
    /// state has every topic with its full partition count — the local log manager has only what this broker hosts,
    /// and a broker hosting one partition of two reported a topic of one partition. A broker on its own has only the
    /// log manager.
    /// </summary>
    private Kuestenlogik.Surgewave.Core.Models.TopicMetadata? TopicOf(string topic)
        => _clusterState?.GetTopic(topic) ?? _logManager.GetTopicMetadata(topic);

    // Every topic the cluster knows, and in a cluster those this broker has only locally too.
    private List<string> AllTopicNames()
    {
        var names = new SortedSet<string>(_logManager.ListTopics().Select(t => t.Name), StringComparer.Ordinal);
        if (_clusterState is not null)
            names.UnionWith(_clusterState.Topics.Keys);
        return [.. names];
    }

    private async Task<MetadataResponse> HandleMetadataAsync(MetadataRequest request, CancellationToken cancellationToken)
    {
        List<string> topics;

        if (request.Topics == null)
        {
            // Null means "all topics"
            topics = AllTopicNames();
            Log.MetadataAllTopics(_logger, topics.Count);
        }
        else if (request.Topics.Count == 0)
        {
            // Empty list means "no topics requested", but some clients send this to discover topics
            // Treat it the same as null (all topics)
            topics = AllTopicNames();
            Log.MetadataEmptyList(_logger, topics.Count);
        }
        else
        {
            // Specific topics requested. Metadata v12+ allows the client to address a
            // topic by id only (name == null). KIP-848 next-gen consumers do exactly
            // this when reconciling a heartbeat assignment, so we resolve the id back
            // to a name via LogManager. Older path (name supplied directly) stays
            // unchanged.
            topics = [];
            foreach (var t in request.Topics)
            {
                if (!string.IsNullOrEmpty(t.Name))
                {
                    topics.Add(t.Name);
                }
                else if (t.TopicId != Guid.Empty)
                {
                    var resolved = _logManager.ResolveTopicId(t.TopicId);
                    if (resolved != null) topics.Add(resolved);
                }
            }
            Log.MetadataSpecificTopics(_logger, string.Join(", ", topics));
        }

        var topicMetadata = new List<MetadataResponse.MetadataResponseTopic>();

        foreach (var topic in topics)
        {
            Log.ProcessingMetadata(_logger, topic);
            var metadata = TopicOf(topic);

            if (metadata == null)
            {
                // Auto-create topic
                Log.AutoCreatingTopic(_logger, topic);

                // In a cluster the controller creates the topic, asked by this broker when it is another one (#207), and the
                // creation completes once the topic has reached this broker through the metadata log (#214). A broker
                // that has not applied a topic the cluster already has learns it the same way.
                try
                {
                    metadata = await _logManager.CreateTopicAsync(
                        topic,
                        _config.DefaultNumPartitions,
                        _config.DefaultReplicationFactor,
                        // An auto-created topic has no client-supplied configuration, so there is
                        // nothing to carry here — unlike CreateTopics, which does (#118 follow-up).
                        config: null,
                        cancellationToken);
                }
                catch (InvalidOperationException)
                {
                    // Created meanwhile — by another client, or it reached this broker on the way.
                    metadata = TopicOf(topic);
                }
                catch (TopicNotCreatedException)
                {
                    _logger.LogWarning("Failed to auto-create topic {Topic} through cluster controller", topic);
                }

                // Until the topic is here the client is told to ask again (LEADER_NOT_AVAILABLE, as Kafka answers a
                // topic it is creating).
                if (metadata == null)
                {
                    topicMetadata.Add(new MetadataResponse.MetadataResponseTopic
                    {
                        ErrorCode = ErrorCode.LeaderNotAvailable,
                        Name = topic,
                        TopicId = Guid.Empty,
                        IsInternal = false,
                        Partitions = [],
                    });
                    continue;
                }

                Log.TopicCreated(_logger, topic, metadata.PartitionCount);
            }

            var partitions = new List<MetadataResponse.MetadataResponsePartition>();
            for (int i = 0; i < metadata.PartitionCount; i++)
            {
                var tp = new TopicPartition { Topic = topic, Partition = i };
                var partitionState = _clusterState?.GetPartitionState(tp);

                if (partitionState != null)
                {
                    // Use cluster state for accurate replica info. Snapshot the
                    // ISR under the state lock and copy the replica list: reverse
                    // ISR propagation (#69) mutates Isr from the fetch path while
                    // we serialize the response, so aliasing the live lists risks
                    // a torn read / InvalidOperationException.
                    partitions.Add(new MetadataResponse.MetadataResponsePartition
                    {
                        ErrorCode = ErrorCode.None,
                        PartitionIndex = i,
                        LeaderId = partitionState.LeaderBrokerId,
                        LeaderEpoch = partitionState.LeaderEpoch,
                        ReplicaNodes = [.. partitionState.Replicas],
                        IsrNodes = _clusterState!.GetIsrSnapshot(tp),
                        OfflineReplicas = []
                    });
                }
                else
                {
                    // Standalone mode - this broker is the only replica
                    partitions.Add(new MetadataResponse.MetadataResponsePartition
                    {
                        ErrorCode = ErrorCode.None,
                        PartitionIndex = i,
                        LeaderId = _config.BrokerId,
                        LeaderEpoch = 0,
                        ReplicaNodes = [_config.BrokerId],
                        IsrNodes = [_config.BrokerId],
                        OfflineReplicas = []
                    });
                }
            }

            // KIP-516 / KIP-848: the consumer relies on the per-topic UUID to map a
            // heartbeat assignment back to a name. Surgewave tracks the id in
            // LogManager.GetTopicMetadata; without this line the next-gen
            // consumer logs "Metadata not found for the assigned topic id: ..."
            // and silently drops the assignment.
            var resolvedMetadata = TopicOf(topic);
            topicMetadata.Add(new MetadataResponse.MetadataResponseTopic
            {
                ErrorCode = ErrorCode.None,
                Name = topic,
                TopicId = resolvedMetadata?.TopicId ?? Guid.Empty,
                IsInternal = false,
                Partitions = partitions
            });
        }

        // Build broker list from cluster state if available, otherwise just this broker
        List<MetadataResponse.MetadataResponseBroker> brokers;
        int controllerId;

        if (_clusterState != null && _clusterState.Brokers.Count > 0)
        {
            brokers = _clusterState.Brokers.Values.Select(b => new MetadataResponse.MetadataResponseBroker
            {
                NodeId = b.BrokerId,
                Host = b.Host,
                Port = b.Port,
                Rack = b.Rack
            }).ToList();
            controllerId = _clusterState.ControllerId;
        }
        else
        {
            brokers =
            [
                new MetadataResponse.MetadataResponseBroker
                {
                    NodeId = _config.BrokerId,
                    Host = _config.Host,
                    Port = _config.Port,
                    Rack = null
                }
            ];
            controllerId = _config.BrokerId;
        }

        var response = new MetadataResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            Brokers = brokers,
            ClusterId = _config.ResolvedClusterId,
            ControllerId = controllerId,
            Topics = topicMetadata
        };

        Log.MetadataResponse(_logger, _config.BrokerId, _config.Host, _config.Port);
        foreach (var topic in topicMetadata)
        {
            Log.MetadataTopicInfo(_logger, topic.Name ?? "", topic.ErrorCode, topic.Partitions.Count);
            foreach (var partition in topic.Partitions)
            {
                Log.MetadataPartitionInfo(_logger, partition.PartitionIndex, partition.LeaderId, string.Join(",", partition.ReplicaNodes), string.Join(",", partition.IsrNodes), partition.ErrorCode);
            }
        }

        return response;
    }

    /// <summary>
    /// KIP-966 paginated topic-partition metadata. Used by Java Kafka client
    /// 4.0+ to fetch large clusters' metadata without overflowing a single
    /// response. Surgewave walks the requested topics in deterministic order
    /// (topic name asc, partition index asc), honours the optional starting
    /// cursor, and stops once <see cref="DescribeTopicPartitionsRequest.ResponsePartitionLimit"/>
    /// partitions have been packed — emitting a <c>NextCursor</c> so the
    /// client can resume. Unknown topics surface with
    /// <see cref="ErrorCode.UnknownTopicOrPartition"/> and zero partitions
    /// rather than being dropped, matching Kafka's row-preserving behaviour.
    /// </summary>
    private DescribeTopicPartitionsResponse HandleDescribeTopicPartitions(DescribeTopicPartitionsRequest request)
    {
        var topics = new List<DescribeTopicPartitionsResponse.DescribeTopicPartitionsResponseTopic>();
        DescribeTopicPartitionsResponse.Cursor? nextCursor = null;
        var partitionsEmitted = 0;
        var limit = request.ResponsePartitionLimit > 0 ? request.ResponsePartitionLimit : 2000;

        // Stable ordering: requested topics first by their input order. The
        // cursor narrows the start within that ordering — Kafka clients
        // expect the broker to "remember" where they left off purely by
        // (topicName, partitionIndex), so we don't reshuffle the list.
        var orderedTopics = request.Topics.Select(t => t.Name).Distinct(StringComparer.Ordinal).ToList();

        var startTopicIdx = 0;
        var startPartitionIdx = 0;
        if (request.StartingCursor is not null)
        {
            var idx = orderedTopics.FindIndex(n => string.Equals(n, request.StartingCursor.TopicName, StringComparison.Ordinal));
            startTopicIdx = idx >= 0 ? idx : 0;
            startPartitionIdx = request.StartingCursor.PartitionIndex;
        }

        for (var i = startTopicIdx; i < orderedTopics.Count; i++)
        {
            if (partitionsEmitted >= limit) break;
            var topicName = orderedTopics[i];
            var meta = TopicOf(topicName);

            if (meta is null)
            {
                topics.Add(new DescribeTopicPartitionsResponse.DescribeTopicPartitionsResponseTopic
                {
                    ErrorCode = ErrorCode.UnknownTopicOrPartition,
                    Name = topicName,
                    TopicId = Guid.Empty,
                    IsInternal = false,
                    Partitions = [],
                });
                continue;
            }

            var partitions = new List<DescribeTopicPartitionsResponse.DescribeTopicPartitionsResponsePartition>();
            var firstPartition = (i == startTopicIdx) ? startPartitionIdx : 0;
            for (var p = firstPartition; p < meta.PartitionCount; p++)
            {
                if (partitionsEmitted >= limit)
                {
                    nextCursor = new DescribeTopicPartitionsResponse.Cursor
                    {
                        TopicName = topicName,
                        PartitionIndex = p,
                    };
                    break;
                }

                var partitionState = _clusterState?.GetPartitionState(new TopicPartition { Topic = topicName, Partition = p });
                int leaderId;
                int leaderEpoch;
                List<int> replicas;
                List<int> isr;

                if (partitionState != null)
                {
                    leaderId = partitionState.LeaderBrokerId;
                    leaderEpoch = partitionState.LeaderEpoch;
                    replicas = partitionState.Replicas;
                    isr = partitionState.Isr;
                }
                else
                {
                    leaderId = _config.BrokerId;
                    leaderEpoch = 0;
                    replicas = [_config.BrokerId];
                    isr = [_config.BrokerId];
                }

                partitions.Add(new DescribeTopicPartitionsResponse.DescribeTopicPartitionsResponsePartition
                {
                    ErrorCode = ErrorCode.None,
                    PartitionIndex = p,
                    LeaderId = leaderId,
                    LeaderEpoch = leaderEpoch,
                    ReplicaNodes = replicas,
                    IsrNodes = isr,
                    EligibleLeaderReplicas = [],
                    LastKnownElr = [],
                    OfflineReplicas = [],
                });
                partitionsEmitted++;
            }

            topics.Add(new DescribeTopicPartitionsResponse.DescribeTopicPartitionsResponseTopic
            {
                ErrorCode = ErrorCode.None,
                Name = topicName,
                TopicId = meta.TopicId,
                IsInternal = topicName.StartsWith('_'),
                Partitions = partitions,
            });

            if (nextCursor is not null) break; // hit limit mid-topic
        }

        return new DescribeTopicPartitionsResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            Topics = topics,
            NextCursor = nextCursor,
        };
    }

    private DescribeClusterResponse HandleDescribeCluster(DescribeClusterRequest request)
    {
        // The cluster as this broker knows it, as Metadata reports it; a broker outside a cluster is the whole
        // cluster. It used to report only itself, so an admin client saw one broker in a cluster of many.
        List<DescribeClusterResponse.DescribeClusterBroker> brokers;
        int controllerId;
        if (_clusterState is { Brokers.Count: > 0 } cluster)
        {
            brokers = [.. cluster.Brokers.Values
                .OrderBy(b => b.BrokerId)
                .Select(b => new DescribeClusterResponse.DescribeClusterBroker
                {
                    BrokerId = b.BrokerId,
                    Host = b.Host,
                    Port = b.Port,
                    Rack = b.Rack,
                    IsFenced = _isKnownFenced(b.BrokerId),
                })];
            controllerId = cluster.ControllerId;
        }
        else
        {
            brokers =
            [
                new DescribeClusterResponse.DescribeClusterBroker
                {
                    BrokerId = _config.BrokerId,
                    Host = _config.Host,
                    Port = _config.Port,
                    Rack = _config.Rack,
                    IsFenced = false,
                },
            ];
            controllerId = _config.BrokerId;
        }

        // The same id ApiVersions and Metadata report: with KIP-1242 a client that sees two ids rebootstraps.
        var clusterId = _config.ResolvedClusterId;

        // ClusterAuthorizedOperations: int.MinValue means not requested
        // If requested, return a bitfield of allowed operations
        var clusterAuthorizedOperations = request.IncludeClusterAuthorizedOperations
            ? 0x1F // All operations allowed (Alter, Create, Delete, Describe, ClusterAction)
            : int.MinValue;

        return new DescribeClusterResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            EndpointType = request.EndpointType,
            ClusterId = clusterId,
            ControllerId = controllerId,
            Brokers = brokers,
            ClusterAuthorizedOperations = clusterAuthorizedOperations
        };
    }
}
