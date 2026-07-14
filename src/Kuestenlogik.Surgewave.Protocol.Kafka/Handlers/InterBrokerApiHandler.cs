using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Broker;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;

/// <summary>
/// Handler for inter-broker replication APIs: LeaderAndIsr, StopReplica, UpdateMetadata, ControlledShutdown, WriteTxnMarkers.
/// These APIs are used for controller-to-broker communication in multi-broker clusters.
/// </summary>
public sealed partial class InterBrokerApiHandler : IKafkaRequestHandler
{
    private readonly IBrokerConfigView _config;
    private readonly ClusterState _clusterState;
    private readonly ReplicaManager _replicaManager;
    private readonly LogManager _logManager;
    private readonly ITransactionMarkerSink? _transactionCoordinator;
    private readonly IIsrUpdateApplier? _isrUpdateApplier;
    private readonly ILogger<InterBrokerApiHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.LeaderAndIsr,
        ApiKey.StopReplica,
        ApiKey.UpdateMetadata,
        ApiKey.ControlledShutdown,
        ApiKey.WriteTxnMarkers,
        ApiKey.AlterPartition
    ];

    public InterBrokerApiHandler(
        IBrokerConfigView config,
        ClusterState clusterState,
        ReplicaManager replicaManager,
        LogManager logManager,
        ILogger<InterBrokerApiHandler> logger,
        ITransactionMarkerSink? transactionCoordinator = null,
        IIsrUpdateApplier? isrUpdateApplier = null)
    {
        _config = config;
        _clusterState = clusterState;
        _replicaManager = replicaManager;
        _logManager = logManager;
        _transactionCoordinator = transactionCoordinator;
        _isrUpdateApplier = isrUpdateApplier;
        _logger = logger;
    }

    public async Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return request switch
        {
            LeaderAndIsrRequest leaderAndIsrRequest => await HandleLeaderAndIsrAsync(leaderAndIsrRequest, cancellationToken),
            StopReplicaRequest stopReplicaRequest => await HandleStopReplicaAsync(stopReplicaRequest, cancellationToken),
            UpdateMetadataRequest updateMetadataRequest => await HandleUpdateMetadataAsync(updateMetadataRequest, cancellationToken),
            ControlledShutdownRequest controlledShutdownRequest => HandleControlledShutdown(controlledShutdownRequest),
            WriteTxnMarkersRequest writeTxnMarkersRequest => await HandleWriteTxnMarkersAsync(writeTxnMarkersRequest, cancellationToken),
            AlterPartitionRequest alterPartitionRequest => await HandleAlterPartitionAsync(alterPartitionRequest, cancellationToken),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by InterBrokerApiHandler")
        };
    }

    private async Task<LeaderAndIsrResponse> HandleLeaderAndIsrAsync(LeaderAndIsrRequest request, CancellationToken ct)
    {
        LogLeaderAndIsrReceived(request.ControllerId, request.ControllerEpoch, request.TopicStates.Count);

        // Hold the shared controller-push gate across fence-through-apply so a Kafka-wire push cannot
        // interleave with a concurrent Kafka or native push during a rolling upgrade (#60 Inc6a).
        using var scope = await _clusterState.AcquireControllerPushScopeAsync(ct).ConfigureAwait(false);

        // Validate controller epoch - reject stale requests. The fence is the shared atomic
        // ClusterState.TryAdvanceControllerEpoch (#60 Inc5): the native inter-broker applier fences
        // through the same ClusterState (version-ordered there), so a stale Kafka-wire push cannot
        // regress an epoch the native wire already advanced (and vice versa) during a rolling upgrade.
        // #72 Inc1 — the fence and the Kafka-wire cap update are ONE atomic operation: a fence-passing
        // push from a remote controller proves the controller finalized to the Kafka wire and caps the
        // local finalized level (rolling-downgrade convergence). A self-delivered push passes null.
        if (!_clusterState.TryAdvanceControllerEpoch(request.ControllerId, request.ControllerEpoch,
                request.ControllerId != _config.BrokerId ? ControllerPushWire.KafkaWire : null))
        {
            LogStaleControllerEpoch("LeaderAndIsr", request.ControllerEpoch, _clusterState.ControllerEpoch);
            return new LeaderAndIsrResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.StaleControllerEpoch,
                PartitionErrors = []
            };
        }

        // Update broker list from LiveLeaders — but only for brokers we don't
        // already know. A broker discovered from cluster-node config carries
        // its real replication port; LeaderAndIsr's LiveLeaders only advertises
        // the client host/port, so a rebuild would reset the node to the default
        // ReplicationPort (Port + 1000) and clobber the discovered value. The
        // follower's fetcher would then dial the wrong port and never catch up,
        // so the ISR would never form (#69). Insert-only, atomically (#72 Inc1:
        // UpdateBroker instead of the racy GetBroker+RegisterBroker pre-read);
        // the local node stays owned by startup self-advertisement.
        foreach (var broker in request.LiveLeaders)
        {
            if (broker.BrokerId == _config.BrokerId)
                continue;

            _clusterState.UpdateBroker(
                broker.BrokerId,
                ifAbsent: new BrokerNode { BrokerId = broker.BrokerId, Host = broker.Host, Port = broker.Port },
                mutate: known => known);
        }

        var partitionErrors = new List<LeaderAndIsrResponse.LeaderAndIsrPartitionError>();

        // Process topic states
        foreach (var topicState in request.TopicStates)
        {
            var topicName = topicState.TopicName ?? string.Empty;

            foreach (var partitionState in topicState.PartitionStates)
            {
                var tp = new TopicPartition { Topic = topicName, Partition = partitionState.PartitionIndex };
                var errorCode = ErrorCode.None;

                try
                {
                    // Per-partition ordering, shared with the native wire (#60 Inc6a): apply only when
                    // this entry's leader epoch is not older than the stored one, so a delayed/
                    // reordered same-controller-epoch push cannot regress a partition; then skip the
                    // BecomeLeader/Follower transition so it never runs on a stale epoch.
                    if (!_clusterState.TryApplyControllerPartitionState(
                            tp, partitionState.Leader, partitionState.LeaderEpoch, partitionState.Replicas, partitionState.Isr))
                    {
                        LogStalePartition("LeaderAndIsr", topicName, partitionState.PartitionIndex, partitionState.LeaderEpoch);
                        partitionErrors.Add(new LeaderAndIsrResponse.LeaderAndIsrPartitionError
                        {
                            TopicName = topicName,
                            PartitionIndex = partitionState.PartitionIndex,
                            ErrorCode = ErrorCode.None
                        });
                        continue;
                    }

                    // Determine if this broker is the leader or follower
                    if (partitionState.Leader == _config.BrokerId)
                    {
                        // This broker is the leader
                        await _replicaManager.BecomeLeaderAsync(tp, partitionState.LeaderEpoch, ct);
                        LogBecameLeader(topicName, partitionState.PartitionIndex, partitionState.LeaderEpoch);
                    }
                    else if (partitionState.Replicas.Contains(_config.BrokerId))
                    {
                        // This broker is a follower
                        await _replicaManager.BecomeFollowerAsync(tp, partitionState.Leader, partitionState.LeaderEpoch, ct);
                        LogBecameFollower(topicName, partitionState.PartitionIndex, partitionState.Leader, partitionState.LeaderEpoch);
                    }
                    else
                    {
                        // This broker is not a replica for this partition - should not receive this
                        LogNotReplica(topicName, partitionState.PartitionIndex);
                        errorCode = ErrorCode.ReplicaNotAvailable;
                    }
                }
                catch (Exception ex)
                {
                    LogLeaderAndIsrError(topicName, partitionState.PartitionIndex, ex);
                    errorCode = ErrorCode.Unknown;
                }

                partitionErrors.Add(new LeaderAndIsrResponse.LeaderAndIsrPartitionError
                {
                    TopicName = topicName,
                    PartitionIndex = partitionState.PartitionIndex,
                    ErrorCode = errorCode
                });
            }
        }

        return new LeaderAndIsrResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            PartitionErrors = partitionErrors
        };
    }

    private async Task<AlterPartitionResponse> HandleAlterPartitionAsync(AlterPartitionRequest request, CancellationToken ct)
    {
        LogAlterPartitionReceived(request.BrokerId, request.Topics.Count);

        var responseTopics = new List<AlterPartitionResponse.TopicData>();

        foreach (var topic in request.Topics)
        {
            // v2+ carries only TopicId over the wire (TopicName is null); v0-1
            // carries the name. Resolve to a name the controller knows.
            var topicName = topic.TopicName;
            if (string.IsNullOrEmpty(topicName))
            {
                topicName = _clusterState.GetTopicById(topic.TopicId)?.Name;
            }

            var responsePartitions = new List<AlterPartitionResponse.PartitionData>();

            foreach (var p in topic.Partitions)
            {
                var newIsr = p.NewIsrWithEpochs != null
                    ? p.NewIsrWithEpochs.Select(b => b.BrokerId).ToList()
                    : (p.NewIsr ?? []);

                // Unknown TopicId — likely a race with topic creation.
                if (string.IsNullOrEmpty(topicName))
                {
                    responsePartitions.Add(BuildPartitionError(p, ErrorCode.UnknownTopicId, newIsr));
                    continue;
                }

                // Only the controller may apply ISR updates.
                if (_isrUpdateApplier is null || !_isrUpdateApplier.IsController)
                {
                    responsePartitions.Add(BuildPartitionError(p, ErrorCode.NotController, newIsr));
                    continue;
                }

                var tp = new TopicPartition { Topic = topicName, Partition = p.PartitionIndex };
                var updated = await _isrUpdateApplier.ApplyIsrUpdateAsync(tp, request.BrokerId, p.LeaderEpoch, newIsr, ct);

                if (updated is null)
                {
                    // Controller doesn't track this partition.
                    responsePartitions.Add(BuildPartitionError(p, ErrorCode.UnknownTopicOrPartition, newIsr));
                    continue;
                }

                responsePartitions.Add(new AlterPartitionResponse.PartitionData
                {
                    PartitionIndex = p.PartitionIndex,
                    ErrorCode = ErrorCode.None,
                    LeaderId = updated.LeaderBrokerId,
                    LeaderEpoch = updated.LeaderEpoch,
                    IsrWithEpochs = updated.Isr
                        .Select(id => new AlterPartitionResponse.BrokerState { BrokerId = id, BrokerEpoch = -1 })
                        .ToList(),
                    PartitionEpoch = updated.LeaderEpoch
                });
            }

            responseTopics.Add(new AlterPartitionResponse.TopicData
            {
                TopicName = topic.TopicName,
                TopicId = topic.TopicId,
                Partitions = responsePartitions
            });
        }

        return new AlterPartitionResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            Topics = responseTopics
        };
    }

    private static AlterPartitionResponse.PartitionData BuildPartitionError(
        AlterPartitionRequest.PartitionData p, ErrorCode errorCode, List<int> isr) => new()
    {
        PartitionIndex = p.PartitionIndex,
        ErrorCode = errorCode,
        LeaderId = -1,
        LeaderEpoch = p.LeaderEpoch,
        IsrWithEpochs = isr.Select(id => new AlterPartitionResponse.BrokerState { BrokerId = id, BrokerEpoch = -1 }).ToList(),
        PartitionEpoch = p.PartitionEpoch
    };

    private async Task<StopReplicaResponse> HandleStopReplicaAsync(StopReplicaRequest request, CancellationToken ct)
    {
        // Same shared push gate as LeaderAndIsr/native (#60 Inc6a).
        using var scope = await _clusterState.AcquireControllerPushScopeAsync(ct).ConfigureAwait(false);

        LogStopReplicaReceived(request.ControllerId, request.ControllerEpoch, request.DeletePartitions);

        // Shared fence + atomic Kafka-wire cap (see HandleLeaderAndIsrAsync, #60 Inc5 / #72 Inc1).
        if (!_clusterState.TryAdvanceControllerEpoch(request.ControllerId, request.ControllerEpoch,
                request.ControllerId != _config.BrokerId ? ControllerPushWire.KafkaWire : null))
        {
            LogStaleControllerEpoch("StopReplica", request.ControllerEpoch, _clusterState.ControllerEpoch);
            return new StopReplicaResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.StaleControllerEpoch,
                PartitionErrors = []
            };
        }

        var partitionErrors = new List<StopReplicaResponse.StopReplicaPartitionError>();

        // Collect all partitions to stop from various request formats
        var partitionsToStop = new List<(string Topic, int Partition, bool Delete, int LeaderEpoch)>();

        // Process topic states (v3+)
        if (request.TopicStates != null)
        {
            foreach (var topicState in request.TopicStates)
            {
                foreach (var partitionState in topicState.PartitionStates)
                {
                    partitionsToStop.Add((
                        topicState.TopicName ?? string.Empty,
                        partitionState.PartitionIndex,
                        partitionState.DeletePartition,
                        partitionState.LeaderEpoch
                    ));
                }
            }
        }

        // Process topics array (v1-2)
        if (request.Topics != null)
        {
            foreach (var topic in request.Topics)
            {
                foreach (var partitionIndex in topic.PartitionIndexes)
                {
                    partitionsToStop.Add((
                        topic.Name ?? string.Empty,
                        partitionIndex,
                        request.DeletePartitions,
                        -1 // No leader epoch in v1-2
                    ));
                }
            }
        }

        // Process ungrouped partitions (v0)
        if (request.UngroupedPartitions != null)
        {
            foreach (var partition in request.UngroupedPartitions)
            {
                partitionsToStop.Add((
                    partition.TopicName ?? string.Empty,
                    partition.PartitionIndex,
                    request.DeletePartitions,
                    -1 // No leader epoch in v0
                ));
            }
        }

        // Process each partition
        foreach (var (topic, partition, delete, leaderEpoch) in partitionsToStop)
        {
            var tp = new TopicPartition { Topic = topic, Partition = partition };
            var errorCode = ErrorCode.None;

            // Per-partition ordering, shared with the native wire (#60 Inc6a): a delayed same-epoch
            // stop must not delete a partition that was re-assigned at a higher leader epoch.
            if (!_clusterState.ShouldStopReplica(tp, leaderEpoch))
            {
                LogStaleStopReplica(topic, partition, leaderEpoch);
                partitionErrors.Add(new StopReplicaResponse.StopReplicaPartitionError
                {
                    TopicName = topic,
                    PartitionIndex = partition,
                    ErrorCode = ErrorCode.None
                });
                continue;
            }

            try
            {
                // Stop replication for this partition
                _replicaManager.StopReplica(tp);

                if (delete)
                {
                    // Delete partition data
                    await _logManager.DeleteLogAsync(tp, ct);
                    _clusterState.RemovePartitionState(tp);
                    LogPartitionDeleted(topic, partition);
                }
                else
                {
                    LogReplicaStopped(topic, partition);
                }
            }
            catch (Exception ex)
            {
                LogStopReplicaError(topic, partition, ex);
                errorCode = ErrorCode.Unknown;
            }

            partitionErrors.Add(new StopReplicaResponse.StopReplicaPartitionError
            {
                TopicName = topic,
                PartitionIndex = partition,
                ErrorCode = errorCode
            });
        }

        return new StopReplicaResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            PartitionErrors = partitionErrors
        };
    }

    private async Task<UpdateMetadataResponse> HandleUpdateMetadataAsync(UpdateMetadataRequest request, CancellationToken ct)
    {
        LogUpdateMetadataReceived(request.ControllerId, request.ControllerEpoch,
            request.LiveBrokers?.Count ?? 0, request.TopicStates?.Count ?? 0);

        // Same shared push gate as LeaderAndIsr/native (#60 Inc6a).
        using var scope = await _clusterState.AcquireControllerPushScopeAsync(ct).ConfigureAwait(false);

        // Shared fence + atomic Kafka-wire cap (see HandleLeaderAndIsrAsync, #60 Inc5 / #72 Inc1).
        if (!_clusterState.TryAdvanceControllerEpoch(request.ControllerId, request.ControllerEpoch,
                request.ControllerId != _config.BrokerId ? ControllerPushWire.KafkaWire : null))
        {
            LogStaleControllerEpoch("UpdateMetadata", request.ControllerEpoch, _clusterState.ControllerEpoch);
            return new UpdateMetadataResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.StaleControllerEpoch
            };
        }

        // Update broker endpoints (v1+) — MERGE, never rebuild (#72 Inc1): the previous unconditional
        // RegisterBroker re-created every listed node, resetting its advertised inter.broker.protocol
        // level to KafkaWire and clobbering an explicitly discovered ReplicationPort (#69-class) —
        // permanently for the RECEIVER'S OWN node, whose self-advertisement runs only at startup. The
        // Kafka UpdateMetadata wire carries no per-broker level, so a known broker's level is left
        // untouched (down-convergence is handled structurally via the fence's Kafka-wire cap above);
        // endpoint fields converge, and a derived ReplicationPort re-derives from the new client port.
        if (request.LiveBrokers != null)
        {
            foreach (var broker in request.LiveBrokers)
            {
                if (broker.Id == _config.BrokerId)
                    continue; // the local node is owned by startup self-advertisement

                // Use first endpoint or fall back to legacy host/port
                var endpoint = broker.Endpoints?.FirstOrDefault();
                var host = endpoint?.Host ?? broker.V0Host ?? "localhost";
                var port = endpoint?.Port ?? broker.V0Port;

                _clusterState.UpdateBroker(
                    broker.Id,
                    ifAbsent: new BrokerNode { BrokerId = broker.Id, Host = host, Port = port, Rack = broker.Rack },
                    mutate: known => known with { Host = host, Port = port, Rack = broker.Rack });
            }
        }

        // Update partition states
        if (request.TopicStates != null)
        {
            foreach (var topicState in request.TopicStates)
            {
                var topicName = topicState.TopicName ?? string.Empty;

                foreach (var partitionState in topicState.PartitionStates)
                {
                    var tp = new TopicPartition { Topic = topicName, Partition = partitionState.PartitionIndex };

                    // Per-partition ordering shared with the native wire (#60 Inc6a).
                    if (!_clusterState.TryApplyControllerPartitionState(
                            tp, partitionState.Leader, partitionState.LeaderEpoch, partitionState.Replicas, partitionState.Isr))
                        LogStalePartition("UpdateMetadata", topicName, partitionState.PartitionIndex, partitionState.LeaderEpoch);
                }
            }
        }

        // Handle v0-4 ungrouped partition states
        if (request.UngroupedPartitionStates != null)
        {
            foreach (var partitionState in request.UngroupedPartitionStates)
            {
                var tp = new TopicPartition
                {
                    Topic = partitionState.TopicName ?? string.Empty,
                    Partition = partitionState.PartitionIndex
                };

                if (!_clusterState.TryApplyControllerPartitionState(
                        tp, partitionState.Leader, partitionState.LeaderEpoch, partitionState.Replicas, partitionState.Isr))
                    LogStalePartition("UpdateMetadata", tp.Topic, partitionState.PartitionIndex, partitionState.LeaderEpoch);
            }
        }

        LogUpdateMetadataApplied();

        return new UpdateMetadataResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None
        };
    }

    private ControlledShutdownResponse HandleControlledShutdown(ControlledShutdownRequest request)
    {
        LogControlledShutdownReceived(request.BrokerId);

        // Validate that this request is for this broker or from the controller
        if (request.BrokerId != _config.BrokerId)
        {
            // This is a request from a broker to the controller
            // Find partitions where the requesting broker is the leader
            var remainingPartitions = new List<ControlledShutdownResponse.RemainingPartition>();

            foreach (var (tp, state) in _clusterState.GetAllPartitionStates())
            {
                if (state.LeaderBrokerId == request.BrokerId)
                {
                    remainingPartitions.Add(new ControlledShutdownResponse.RemainingPartition
                    {
                        TopicName = tp.Topic,
                        PartitionIndex = tp.Partition
                    });
                }
            }

            LogControlledShutdownPartitions(request.BrokerId, remainingPartitions.Count);

            return new ControlledShutdownResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None,
                RemainingPartitions = remainingPartitions
            };
        }

        // This broker is shutting down - no remaining partitions since we're the one leaving
        return new ControlledShutdownResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            RemainingPartitions = []
        };
    }

    /// <summary>
    /// Handle WriteTxnMarkers request from transaction coordinator.
    /// Writes transaction commit/abort markers to partitions hosted on this broker.
    /// </summary>
    private async Task<WriteTxnMarkersResponse> HandleWriteTxnMarkersAsync(WriteTxnMarkersRequest request, CancellationToken ct)
    {
        LogWriteTxnMarkersReceived(request.Markers.Count);

        var markerResults = new List<WriteTxnMarkersResponse.MarkerResult>();

        foreach (var marker in request.Markers)
        {
            var topicResults = new List<WriteTxnMarkersResponse.TopicResult>();

            foreach (var topic in marker.Topics)
            {
                var partitionResults = new List<WriteTxnMarkersResponse.PartitionResult>();

                foreach (var partitionIndex in topic.PartitionIndexes)
                {
                    var tp = new TopicPartition { Topic = topic.Topic, Partition = partitionIndex };
                    var errorCode = ErrorCode.None;

                    try
                    {
                        // Verify this broker is the leader for this partition
                        var partitionState = _clusterState.GetPartitionState(tp);
                        if (partitionState?.LeaderBrokerId != _config.BrokerId)
                        {
                            errorCode = ErrorCode.NotLeaderForPartition;
                            LogNotLeaderForPartition(topic.Topic, partitionIndex);
                        }
                        else
                        {
                            // Write the transaction marker to the log
                            var controlRecordType = marker.TransactionResult
                                ? Kuestenlogik.Surgewave.Core.KafkaConstants.ControlRecordType.Commit
                                : Kuestenlogik.Surgewave.Core.KafkaConstants.ControlRecordType.Abort;

                            var markerBatch = Kuestenlogik.Surgewave.Core.Storage.ControlBatchBuilder.BuildTransactionMarker(
                                marker.ProducerId,
                                marker.ProducerEpoch,
                                controlRecordType);

                            var offset = await _logManager.AppendBatchAsync(tp, markerBatch, ct);

                            LogTxnMarkerWritten(
                                topic.Topic,
                                partitionIndex,
                                marker.ProducerId,
                                marker.TransactionResult ? "COMMIT" : "ABORT",
                                offset);

                            // Update transaction index if available
                            if (_transactionCoordinator != null)
                            {
                                if (marker.TransactionResult)
                                {
                                    _transactionCoordinator.CommitTransaction(
                                        marker.ProducerId,
                                        [tp],
                                        offset);
                                }
                                else
                                {
                                    _transactionCoordinator.AbortTransaction(
                                        marker.ProducerId,
                                        [tp],
                                        offset);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        LogWriteTxnMarkerError(topic.Topic, partitionIndex, ex);
                        errorCode = ErrorCode.Unknown;
                    }

                    partitionResults.Add(new WriteTxnMarkersResponse.PartitionResult
                    {
                        PartitionIndex = partitionIndex,
                        ErrorCode = errorCode
                    });
                }

                topicResults.Add(new WriteTxnMarkersResponse.TopicResult
                {
                    Topic = topic.Topic,
                    Partitions = partitionResults
                });
            }

            markerResults.Add(new WriteTxnMarkersResponse.MarkerResult
            {
                ProducerId = marker.ProducerId,
                Topics = topicResults
            });
        }

        return new WriteTxnMarkersResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            Markers = markerResults
        };
    }

    #region Logging

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received WriteTxnMarkers with {MarkerCount} markers")]
    private partial void LogWriteTxnMarkersReceived(int markerCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Wrote {MarkerType} marker for {Topic}-{Partition}, ProducerId={ProducerId}, Offset={Offset}")]
    private partial void LogTxnMarkerWritten(string topic, int partition, long producerId, string markerType, long offset);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Not leader for partition {Topic}-{Partition}, cannot write transaction marker")]
    private partial void LogNotLeaderForPartition(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error writing transaction marker for {Topic}-{Partition}")]
    private partial void LogWriteTxnMarkerError(string topic, int partition, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received LeaderAndIsr from controller {ControllerId} epoch {ControllerEpoch} with {TopicCount} topics")]
    private partial void LogLeaderAndIsrReceived(int controllerId, int controllerEpoch, int topicCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received AlterPartition from broker {BrokerId} with {TopicCount} topics")]
    private partial void LogAlterPartitionReceived(int brokerId, int topicCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Rejecting stale {ApiName} request: epoch {RequestEpoch} < current {CurrentEpoch}")]
    private partial void LogStaleControllerEpoch(string apiName, int requestEpoch, int currentEpoch);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Skipping stale {ApiName} entry for {Topic}-{Partition}: leader epoch {LeaderEpoch} older than stored")]
    private partial void LogStalePartition(string apiName, string topic, int partition, int leaderEpoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Skipping stale StopReplica for {Topic}-{Partition}: leader epoch {LeaderEpoch} older than the current re-assignment")]
    private partial void LogStaleStopReplica(string topic, int partition, int leaderEpoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Became leader for {Topic}-{Partition} epoch {LeaderEpoch}")]
    private partial void LogBecameLeader(string topic, int partition, int leaderEpoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Became follower for {Topic}-{Partition} leader={LeaderId} epoch {LeaderEpoch}")]
    private partial void LogBecameFollower(string topic, int partition, int leaderId, int leaderEpoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Not a replica for {Topic}-{Partition}, ignoring LeaderAndIsr")]
    private partial void LogNotReplica(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error processing LeaderAndIsr for {Topic}-{Partition}")]
    private partial void LogLeaderAndIsrError(string topic, int partition, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received StopReplica from controller {ControllerId} epoch {ControllerEpoch} delete={Delete}")]
    private partial void LogStopReplicaReceived(int controllerId, int controllerEpoch, bool delete);

    [LoggerMessage(Level = LogLevel.Information, Message = "Stopped replica for {Topic}-{Partition}")]
    private partial void LogReplicaStopped(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Information, Message = "Deleted partition {Topic}-{Partition}")]
    private partial void LogPartitionDeleted(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error stopping replica for {Topic}-{Partition}")]
    private partial void LogStopReplicaError(string topic, int partition, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received UpdateMetadata from controller {ControllerId} epoch {ControllerEpoch} with {BrokerCount} brokers, {TopicCount} topics")]
    private partial void LogUpdateMetadataReceived(int controllerId, int controllerEpoch, int brokerCount, int topicCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Applied UpdateMetadata to cluster state")]
    private partial void LogUpdateMetadataApplied();

    [LoggerMessage(Level = LogLevel.Information, Message = "Received ControlledShutdown request for broker {BrokerId}")]
    private partial void LogControlledShutdownReceived(int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker {BrokerId} has {PartitionCount} partitions to transfer before shutdown")]
    private partial void LogControlledShutdownPartitions(int brokerId, int partitionCount);

    #endregion
}
