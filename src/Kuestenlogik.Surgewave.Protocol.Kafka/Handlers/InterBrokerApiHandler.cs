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
/// Handler for inter-broker replication APIs: AlterPartition, ControlledShutdown, WriteTxnMarkers.
/// <para>
/// LeaderAndIsr, StopReplica and UpdateMetadata were the controller pushes. Metadata is a
/// replicated log that every broker applies (#163 step 3), so nothing sends them and the API keys
/// are no longer advertised — a client asking for one gets UnsupportedVersion, which is true.
/// </para>
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
            ControlledShutdownRequest controlledShutdownRequest => HandleControlledShutdown(controlledShutdownRequest),
            WriteTxnMarkersRequest writeTxnMarkersRequest => await HandleWriteTxnMarkersAsync(writeTxnMarkersRequest, cancellationToken),
            AlterPartitionRequest alterPartitionRequest => await HandleAlterPartitionAsync(alterPartitionRequest, cancellationToken),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by InterBrokerApiHandler")
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

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received AlterPartition from broker {BrokerId} with {TopicCount} topics")]
    private partial void LogAlterPartitionReceived(int brokerId, int topicCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Received ControlledShutdown request for broker {BrokerId}")]
    private partial void LogControlledShutdownReceived(int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker {BrokerId} has {PartitionCount} partitions to transfer before shutdown")]
    private partial void LogControlledShutdownPartitions(int brokerId, int partitionCount);

    #endregion
}
