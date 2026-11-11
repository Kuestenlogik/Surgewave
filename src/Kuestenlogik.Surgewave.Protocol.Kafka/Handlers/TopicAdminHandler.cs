using Kuestenlogik.Surgewave.Broker.Audit;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Quotas;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;

/// <summary>
/// Handler for topic administration APIs: CreateTopics, DeleteTopics, CreatePartitions, DeleteRecords
/// </summary>
public sealed class TopicAdminHandler : IKafkaRequestHandler
{
    private readonly IBrokerConfigView _config;
    private readonly LogManager _logManager;
    private readonly IQuotaManager _quotaManager;
    private readonly IAuditLogger? _auditLogger;
    private readonly ILogger<TopicAdminHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.CreateTopics,
        ApiKey.DeleteTopics,
        ApiKey.CreatePartitions,
        ApiKey.DeleteRecords,
        ApiKey.DescribeLogDirs,
        ApiKey.AlterReplicaLogDirs,
    ];

    public TopicAdminHandler(
        IBrokerConfigView config,
        LogManager logManager,
        IQuotaManager quotaManager,
        IAuditLogger? auditLogger,
        ILogger<TopicAdminHandler> logger)
    {
        _config = config;
        _logManager = logManager;
        _quotaManager = quotaManager;
        _auditLogger = auditLogger;
        _logger = logger;
    }

    public async Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return request switch
        {
            CreateTopicsRequest createTopicsRequest => await HandleCreateTopicsAsync(createTopicsRequest, context, cancellationToken),
            DeleteTopicsRequest deleteTopicsRequest => await HandleDeleteTopicsAsync(deleteTopicsRequest, context, cancellationToken),
            CreatePartitionsRequest createPartitionsRequest => HandleCreatePartitions(createPartitionsRequest, context),
            DeleteRecordsRequest deleteRecordsRequest => HandleDeleteRecords(deleteRecordsRequest, context),
            DescribeLogDirsRequest describeLogDirsRequest => HandleDescribeLogDirs(describeLogDirsRequest),
            AlterReplicaLogDirsRequest alterReplicaLogDirsRequest => HandleAlterReplicaLogDirs(alterReplicaLogDirsRequest),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by TopicAdminHandler")
        };
    }

    private async Task<CreateTopicsResponse> HandleCreateTopicsAsync(CreateTopicsRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        var results = new List<CreateTopicsResponse.TopicResult>();

        // Before anything is created or validated: a request over the limit fails as a whole, as Kafka's does.
        if (ExceedsPartitionLimit(request))
        {
            foreach (var topic in request.Topics)
            {
                results.Add(new CreateTopicsResponse.TopicResult
                {
                    Name = topic.Name,
                    ErrorCode = ErrorCode.PolicyViolation,
                    ErrorMessage = "Too many partitions in request.",
                });
            }
            return new CreateTopicsResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                Topics = results,
            };
        }

        foreach (var topic in request.Topics)
        {
            try
            {
                var existingMetadata = _logManager.GetTopicMetadata(topic.Name);
                if (existingMetadata != null)
                {
                    results.Add(new CreateTopicsResponse.TopicResult
                    {
                        Name = topic.Name,
                        ErrorCode = ErrorCode.TopicAlreadyExists,
                        ErrorMessage = $"Topic '{topic.Name}' already exists"
                    });
                    continue;
                }

                // A client's own placement (#202) is checked as Kafka's controller checks it, and then honoured.
                IReadOnlyList<IReadOnlyList<int>>? replicaAssignment = null;
                if (topic.Assignments is { Count: > 0 } requested)
                {
                    if (ValidateReplicaAssignment(topic, requested) is { } invalid)
                    {
                        results.Add(new CreateTopicsResponse.TopicResult
                        {
                            Name = topic.Name,
                            ErrorCode = invalid.ErrorCode,
                            ErrorMessage = invalid.Message
                        });
                        continue;
                    }
                    replicaAssignment = [.. requested.OrderBy(a => a.Partition).Select(a => (IReadOnlyList<int>)a.BrokerIds)];
                }

                var numPartitions = replicaAssignment?.Count
                    ?? (topic.NumPartitions > 0 ? topic.NumPartitions : _config.DefaultNumPartitions);
                var replicationFactor = replicaAssignment is not null
                    ? (short)replicaAssignment[0].Count
                    : topic.ReplicationFactor > 0 ? topic.ReplicationFactor : _config.DefaultReplicationFactor;

                if (request.ValidateOnly)
                {
                    results.Add(new CreateTopicsResponse.TopicResult
                    {
                        Name = topic.Name,
                        ErrorCode = ErrorCode.None,
                        NumPartitions = replicaAssignment is not null ? numPartitions : topic.NumPartitions,
                        ReplicationFactor = replicaAssignment is not null ? replicationFactor : topic.ReplicationFactor
                    });
                    continue;
                }

                // In a cluster the controller creates every topic, asked by this broker when it is another one (#207),
                // and the creation completes once the topic has reached this broker (#214); a topic is never created on
                // one broker alone. Only a broker on its own creates it locally.
                try
                {
                    if (replicaAssignment is not null)
                        await _logManager.CreateTopicWithReplicaAssignmentAsync(topic.Name, replicaAssignment, topic.Configs, cancellationToken);
                    else
                        await _logManager.CreateTopicAsync(topic.Name, numPartitions, replicationFactor, topic.Configs, cancellationToken);
                }
                catch (InvalidReplicaAssignmentException ex)
                {
                    // In a cluster: a broker of the assignment is not registered, as the metadata log knows it.
                    results.Add(new CreateTopicsResponse.TopicResult
                    {
                        Name = topic.Name,
                        ErrorCode = ErrorCode.InvalidReplicaAssignment,
                        ErrorMessage = ex.Message
                    });
                    continue;
                }
                catch (TopicNotCreatedException)
                {
                    // Retriable: the controller was changing hands or not reachable; the admin client asks again.
                    results.Add(new CreateTopicsResponse.TopicResult
                    {
                        Name = topic.Name,
                        ErrorCode = ErrorCode.NotController,
                        ErrorMessage = "The controller did not create the topic; retry"
                    });
                    continue;
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("already exists", StringComparison.Ordinal))
                {
                    results.Add(new CreateTopicsResponse.TopicResult
                    {
                        Name = topic.Name,
                        ErrorCode = ErrorCode.TopicAlreadyExists,
                        ErrorMessage = ex.Message
                    });
                    continue;
                }

                Log.TopicCreated(_logger, topic.Name, numPartitions);

                // Audit log the topic creation
                _auditLogger?.LogTopicEvent(
                    AuditEventType.TopicCreated,
                    topic.Name,
                    context.ConnectionState.AuthenticatedUser,
                    context.ConnectionState.ClientHost,
                    context.ClientId,
                    success: true,
                    details: new Dictionary<string, string>
                    {
                        ["partitions"] = numPartitions.ToString(),
                        ["replicationFactor"] = replicationFactor.ToString()
                    });

                results.Add(new CreateTopicsResponse.TopicResult
                {
                    Name = topic.Name,
                    ErrorCode = ErrorCode.None,
                    TopicId = _logManager.GetTopicId(topic.Name),
                    NumPartitions = numPartitions,
                    ReplicationFactor = replicationFactor,
                    Configs = topic.Configs
                });
            }
            catch (Exception ex)
            {
                Log.CreateTopicError(_logger, ex, topic.Name);
                results.Add(new CreateTopicsResponse.TopicResult
                {
                    Name = topic.Name,
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                });
            }
        }

        return new CreateTopicsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, Topics = results };
    }

    /// <summary>
    /// What is wrong with a topic's replica assignment, with Kafka's error code and message
    /// (ReplicationControlManager.createTopic and validateManualPartitionAssignment); null when nothing is.
    /// NumPartitions and ReplicationFactor must be -1 beside one, the partitions numbered 0 to n-1 once each, each with
    /// the same number of distinct brokers. Whether those brokers are registered is checked where the cluster is known:
    /// on its way into the metadata log, or here for a broker on its own, whose only broker is itself.
    /// </summary>
    private (ErrorCode ErrorCode, string Message)? ValidateReplicaAssignment(
        CreateTopicsRequest.TopicToCreate topic, List<CreateTopicsRequest.ReplicaAssignment> assignments)
    {
        if (topic.ReplicationFactor != -1)
            return (ErrorCode.InvalidRequest, "A manual partition assignment was specified, but replication factor was not set to -1.");
        if (topic.NumPartitions != -1)
            return (ErrorCode.InvalidRequest, "A manual partition assignment was specified, but numPartitions was not set to -1.");

        var partitions = new HashSet<int>();
        int? replicationFactor = null;
        foreach (var assignment in assignments)
        {
            if (!partitions.Add(assignment.Partition))
                return (ErrorCode.InvalidReplicaAssignment, $"Found multiple manual partition assignments for partition {assignment.Partition}");
            if (assignment.BrokerIds.Count == 0)
                return (ErrorCode.InvalidReplicaAssignment, "The manual partition assignment includes an empty replica list.");

            int? previous = null;
            foreach (var brokerId in assignment.BrokerIds.Order())
            {
                if (!_logManager.RoutesTopicCreation && brokerId != _config.BrokerId)
                    return (ErrorCode.InvalidReplicaAssignment, $"The manual partition assignment includes broker {brokerId}, but no such broker is registered.");
                if (brokerId == previous)
                    return (ErrorCode.InvalidReplicaAssignment, $"The manual partition assignment includes the broker {brokerId} more than once.");
                previous = brokerId;
            }

            if (replicationFactor is { } expected && assignment.BrokerIds.Count != expected)
                return (ErrorCode.InvalidReplicaAssignment,
                    $"The manual partition assignment includes a partition with {assignment.BrokerIds.Count} replica(s), but this is not " +
                    $"consistent with previous partitions, which have {expected} replica(s).");
            replicationFactor = assignment.BrokerIds.Count;
        }

        for (var p = 0; p < partitions.Count; p++)
        {
            if (!partitions.Contains(p))
                return (ErrorCode.InvalidReplicaAssignment, "partitions should be a consecutive 0-based integer sequence");
        }
        return null;
    }

    /// <summary>
    /// Whether the request's topics together create more partitions than one request may: an explicit assignment
    /// counts its partitions, a topic without one its NumPartitions or, at -1, the default. Summed in 64 bits —
    /// two topics of int.MaxValue partitions overflowed an int to a small number (Kafka e74637ae04).
    /// </summary>
    private bool ExceedsPartitionLimit(CreateTopicsRequest request)
    {
        long total = 0;
        foreach (var topic in request.Topics)
        {
            if (topic.Assignments is { Count: > 0 } assignments)
                total += assignments.Count;
            else if (topic.NumPartitions == -1)
                total += _config.DefaultNumPartitions;
            else if (topic.NumPartitions > 0)
                total += topic.NumPartitions;

            if (total > _config.MaxPartitionsPerCreateTopicsRequest)
                return true;
        }
        return false;
    }

    private async Task<DeleteTopicsResponse> HandleDeleteTopicsAsync(DeleteTopicsRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        var results = new List<DeleteTopicsResponse.TopicResult>();

        foreach (var topicName in request.TopicNames)
        {
            try
            {
                var metadata = _logManager.GetTopicMetadata(topicName);
                if (metadata == null)
                {
                    results.Add(new DeleteTopicsResponse.TopicResult { Name = topicName, ErrorCode = ErrorCode.UnknownTopicOrPartition, ErrorMessage = $"Topic '{topicName}' does not exist" });
                    continue;
                }

                await _logManager.DeleteTopicAsync(topicName, cancellationToken);
                Log.TopicDeleted(_logger, topicName);

                // Audit log the topic deletion
                _auditLogger?.LogTopicEvent(
                    AuditEventType.TopicDeleted,
                    topicName,
                    context.ConnectionState.AuthenticatedUser,
                    context.ConnectionState.ClientHost,
                    request.ClientId,
                    success: true);

                results.Add(new DeleteTopicsResponse.TopicResult { Name = topicName, ErrorCode = ErrorCode.None });
            }
            catch (Exception ex)
            {
                Log.DeleteTopicError(_logger, ex, topicName);
                results.Add(new DeleteTopicsResponse.TopicResult { Name = topicName, ErrorCode = ErrorCode.Unknown, ErrorMessage = ex.Message });
            }
        }

        return new DeleteTopicsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, Responses = results };
    }

    private CreatePartitionsResponse HandleCreatePartitions(CreatePartitionsRequest request, RequestContext context)
    {
        var throttleTimeMs = _quotaManager.CheckProduceQuota(request.ClientId, 0);
        var results = new List<CreatePartitionsResponse.TopicResult>();

        foreach (var topic in request.Topics)
        {
            var metadata = _logManager.GetTopicMetadata(topic.Name);
            if (metadata == null)
            {
                results.Add(new CreatePartitionsResponse.TopicResult { Name = topic.Name, ErrorCode = ErrorCode.UnknownTopicOrPartition, ErrorMessage = $"Topic '{topic.Name}' does not exist" });
                continue;
            }

            if (topic.Count <= metadata.PartitionCount)
            {
                results.Add(new CreatePartitionsResponse.TopicResult { Name = topic.Name, ErrorCode = ErrorCode.InvalidPartitions, ErrorMessage = $"Topic currently has {metadata.PartitionCount} partitions" });
                continue;
            }

            if (request.ValidateOnly)
            {
                results.Add(new CreatePartitionsResponse.TopicResult { Name = topic.Name, ErrorCode = ErrorCode.None });
                continue;
            }

            var success = _logManager.AddPartitions(topic.Name, topic.Count);
            results.Add(new CreatePartitionsResponse.TopicResult { Name = topic.Name, ErrorCode = success ? ErrorCode.None : ErrorCode.Unknown, ErrorMessage = success ? null : "Failed to add partitions" });
        }

        return new CreatePartitionsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, ThrottleTimeMs = throttleTimeMs, Results = results };
    }

    private DeleteRecordsResponse HandleDeleteRecords(DeleteRecordsRequest request, RequestContext context)
    {
        var throttleTimeMs = _quotaManager.CheckProduceQuota(request.ClientId, 0);
        var topicResults = new List<DeleteRecordsResponse.TopicResult>();

        foreach (var topic in request.Topics)
        {
            var partitionResults = new List<DeleteRecordsResponse.PartitionResult>();
            foreach (var partition in topic.Partitions)
            {
                var topicPartition = new TopicPartition { Topic = topic.Topic, Partition = partition.Partition };
                var log = _logManager.GetLog(topicPartition);
                if (log == null)
                {
                    partitionResults.Add(new DeleteRecordsResponse.PartitionResult { Partition = partition.Partition, LowWatermark = -1, ErrorCode = ErrorCode.UnknownTopicOrPartition });
                    continue;
                }

                try
                {
                    if (log is not PartitionLog persistentLog)
                    {
                        partitionResults.Add(new DeleteRecordsResponse.PartitionResult { Partition = partition.Partition, LowWatermark = log.LogStartOffset, ErrorCode = ErrorCode.None });
                        continue;
                    }
                    var newLogStartOffset = persistentLog.DeleteRecordsToOffset(partition.Offset);
                    partitionResults.Add(new DeleteRecordsResponse.PartitionResult { Partition = partition.Partition, LowWatermark = newLogStartOffset, ErrorCode = ErrorCode.None });
                }
                catch
                {
                    partitionResults.Add(new DeleteRecordsResponse.PartitionResult { Partition = partition.Partition, LowWatermark = -1, ErrorCode = ErrorCode.Unknown });
                }
            }
            topicResults.Add(new DeleteRecordsResponse.TopicResult { Topic = topic.Topic, Partitions = partitionResults });
        }

        return new DeleteRecordsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, ThrottleTimeMs = throttleTimeMs, Topics = topicResults };
    }

    /// <summary>
    /// KIP-113 / Kafka log-dirs admin RPC. Surgewave runs with a single data
    /// directory per broker (no JBOD) — the response is therefore a single
    /// <c>LogDirResult</c> entry whose <c>LogDir</c> field surfaces
    /// <see cref="LogManager.DataDirectory"/> and whose <c>Topics</c> list
    /// projects every requested partition's <c>TotalSize</c>. Volume
    /// total/usable bytes (v4+) are filled from <see cref="DriveInfo"/>;
    /// per-partition <c>OffsetLag</c> stays at 0 because Surgewave doesn't run
    /// the legacy "future log" copy machinery (KIP-113 partition-move) —
    /// see also <c>HandleAlterReplicaLogDirs</c> for the matching reject.
    /// Filtering: a null <see cref="DescribeLogDirsRequest.Topics"/> means
    /// "all topics", an empty list means "all topics" (Kafka convention),
    /// any provided list narrows the projection.
    /// </summary>
    private DescribeLogDirsResponse HandleDescribeLogDirs(DescribeLogDirsRequest request)
    {
        // Build the topic→partitions filter. Null OR empty → all topics; an
        // explicit empty topic entry inside means "no partitions for this
        // topic" which Kafka treats as 0-row passthrough.
        Dictionary<string, HashSet<int>?>? filter = null;
        if (request.Topics is { Count: > 0 })
        {
            filter = new(StringComparer.Ordinal);
            foreach (var t in request.Topics)
            {
                filter[t.Topic] = t.Partitions.Count == 0 ? null : [.. t.Partitions];
            }
        }

        var topicResults = new List<DescribeLogDirsResponse.TopicResult>();
        foreach (var topic in _logManager.ListTopics())
        {
            HashSet<int>? partitionFilter = null;
            if (filter is not null)
            {
                if (!filter.TryGetValue(topic.Name, out partitionFilter))
                {
                    continue;
                }
            }

            var partitionResults = new List<DescribeLogDirsResponse.PartitionResult>();
            for (var p = 0; p < topic.PartitionCount; p++)
            {
                if (partitionFilter is not null && !partitionFilter.Contains(p)) continue;

                var log = _logManager.GetLog(new TopicPartition { Topic = topic.Name, Partition = p });
                partitionResults.Add(new DescribeLogDirsResponse.PartitionResult
                {
                    PartitionIndex = p,
                    Size = log?.TotalSize ?? 0,
                    OffsetLag = 0,        // Surgewave has no future-log copy state to lag against.
                    IsFutureKey = false,
                });
            }

            topicResults.Add(new DescribeLogDirsResponse.TopicResult
            {
                Topic = topic.Name,
                Partitions = partitionResults,
            });
        }

        var (totalBytes, usableBytes) = TryGetVolumeBytes(_logManager.DataDirectory);

        return new DescribeLogDirsResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            Results =
            [
                new DescribeLogDirsResponse.LogDirResult
                {
                    ErrorCode = ErrorCode.None,
                    LogDir = Path.GetFullPath(_logManager.DataDirectory),
                    Topics = topicResults,
                    TotalBytes = totalBytes,
                    UsableBytes = usableBytes,
                },
            ],
        };
    }

    /// <summary>
    /// KIP-113 partition log-dir move. Surgewave has a single log dir per broker
    /// — there's no other directory to move a partition to — so the polite
    /// reject is the right shape: every requested partition gets a
    /// per-partition <see cref="ErrorCode.LogDirNotFound"/> with the actual
    /// data directory in the message, so admin tools can surface a precise
    /// reason without inferring "no JBOD" from a generic UNSUPPORTED_VERSION.
    /// </summary>
    private AlterReplicaLogDirsResponse HandleAlterReplicaLogDirs(AlterReplicaLogDirsRequest request)
    {
        // Surgewave has a single log dir per broker — every requested move
        // targets a directory that either doesn't exist or is the same dir
        // (a no-op). Responding with LogDirNotFound per (topic, partition)
        // is the standard Kafka shape for "I can't move this there" and
        // matches what librdkafka admin paths surface to the operator.
        var topicAggregates = new Dictionary<string, List<AlterReplicaLogDirsResponse.PartitionResult>>(StringComparer.Ordinal);
        foreach (var dirReq in request.Dirs)
        {
            foreach (var topic in dirReq.Topics)
            {
                if (!topicAggregates.TryGetValue(topic.Topic, out var list))
                {
                    list = [];
                    topicAggregates[topic.Topic] = list;
                }
                foreach (var partition in topic.Partitions)
                {
                    list.Add(new AlterReplicaLogDirsResponse.PartitionResult
                    {
                        PartitionIndex = partition,
                        ErrorCode = ErrorCode.LogDirNotFound,
                    });
                }
            }
        }

        var results = topicAggregates.Select(kv => new AlterReplicaLogDirsResponse.TopicResult
        {
            Topic = kv.Key,
            Partitions = kv.Value,
        }).ToList();

        return new AlterReplicaLogDirsResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            Results = results,
        };
    }

    private static (long totalBytes, long usableBytes) TryGetVolumeBytes(string path)
    {
        try
        {
            var resolved = Path.GetFullPath(path);
            var root = Path.GetPathRoot(resolved);
            if (string.IsNullOrEmpty(root)) return (-1, -1);
            var info = new DriveInfo(root);
            if (!info.IsReady) return (-1, -1);
            return (info.TotalSize, info.AvailableFreeSpace);
        }
        catch
        {
            return (-1, -1);
        }
    }
}
