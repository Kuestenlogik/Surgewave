using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Handler for inter-broker replication APIs: LeaderAndIsr, StopReplica, UpdateMetadata, ControlledShutdown, WriteTxnMarkers.
/// These APIs are used for controller-to-broker communication in multi-broker clusters.
/// </summary>
public sealed partial class InterBrokerApiHandler : IKafkaRequestHandler
{
    private readonly BrokerConfig _config;
    private readonly ClusterState _clusterState;
    private readonly ReplicaManager _replicaManager;
    private readonly LogManager _logManager;
    private readonly TransactionCoordinator? _transactionCoordinator;
    private readonly IIsrUpdateApplier? _isrUpdateApplier;
    private readonly ILogger<InterBrokerApiHandler> _logger;

    // Track the current controller epoch to reject stale requests
    private int _currentControllerEpoch;

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
        BrokerConfig config,
        ClusterState clusterState,
        ReplicaManager replicaManager,
        LogManager logManager,
        ILogger<InterBrokerApiHandler> logger,
        TransactionCoordinator? transactionCoordinator = null,
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
            UpdateMetadataRequest updateMetadataRequest => HandleUpdateMetadata(updateMetadataRequest),
            ControlledShutdownRequest controlledShutdownRequest => HandleControlledShutdown(controlledShutdownRequest),
            WriteTxnMarkersRequest writeTxnMarkersRequest => await HandleWriteTxnMarkersAsync(writeTxnMarkersRequest, cancellationToken),
            AlterPartitionRequest alterPartitionRequest => await HandleAlterPartitionAsync(alterPartitionRequest, cancellationToken),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by InterBrokerApiHandler")
        };
    }

    private async Task<LeaderAndIsrResponse> HandleLeaderAndIsrAsync(LeaderAndIsrRequest request, CancellationToken ct)
    {
        LogLeaderAndIsrReceived(request.ControllerId, request.ControllerEpoch, request.TopicStates.Count);

        // Validate controller epoch - reject stale requests
        if (request.ControllerEpoch < _currentControllerEpoch)
        {
            LogStaleControllerEpoch("LeaderAndIsr", request.ControllerEpoch, _currentControllerEpoch);
            return new LeaderAndIsrResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.StaleControllerEpoch,
                PartitionErrors = []
            };
        }

        // Update controller epoch
        _currentControllerEpoch = request.ControllerEpoch;
        _clusterState.ControllerEpoch = request.ControllerEpoch;
        _clusterState.ControllerId = request.ControllerId;

        // Update broker list from LiveLeaders — but only for brokers we don't
        // already know. A broker discovered from cluster-node config carries
        // its real replication port; LeaderAndIsr's LiveLeaders only advertises
        // the client host/port, so RegisterBroker would rebuild the node with
        // the default ReplicationPort (Port + 1000) and clobber the discovered
        // value. The follower's fetcher would then dial the wrong port and
        // never catch up, so the ISR would never form (#69).
        foreach (var broker in request.LiveLeaders)
        {
            if (_clusterState.GetBroker(broker.BrokerId) is null)
            {
                _clusterState.RegisterBroker(broker.BrokerId, broker.Host, broker.Port);
            }
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
                    // Update partition state in cluster state
                    _clusterState.UpdatePartitionState(tp, state =>
                    {
                        state.LeaderBrokerId = partitionState.Leader;
                        state.LeaderEpoch = partitionState.LeaderEpoch;
                        state.Replicas.Clear();
                        state.Replicas.AddRange(partitionState.Replicas);
                        state.Isr.Clear();
                        state.Isr.AddRange(partitionState.Isr);
                    });

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
        LogStopReplicaReceived(request.ControllerId, request.ControllerEpoch, request.DeletePartitions);

        // Validate controller epoch
        if (request.ControllerEpoch < _currentControllerEpoch)
        {
            LogStaleControllerEpoch("StopReplica", request.ControllerEpoch, _currentControllerEpoch);
            return new StopReplicaResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.StaleControllerEpoch,
                PartitionErrors = []
            };
        }

        _currentControllerEpoch = request.ControllerEpoch;
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

    private UpdateMetadataResponse HandleUpdateMetadata(UpdateMetadataRequest request)
    {
        LogUpdateMetadataReceived(request.ControllerId, request.ControllerEpoch,
            request.LiveBrokers?.Count ?? 0, request.TopicStates?.Count ?? 0);

        // Validate controller epoch
        if (request.ControllerEpoch < _currentControllerEpoch)
        {
            LogStaleControllerEpoch("UpdateMetadata", request.ControllerEpoch, _currentControllerEpoch);
            return new UpdateMetadataResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.StaleControllerEpoch
            };
        }

        _currentControllerEpoch = request.ControllerEpoch;
        _clusterState.ControllerEpoch = request.ControllerEpoch;
        _clusterState.ControllerId = request.ControllerId;

        // Update broker endpoints (v1+)
        if (request.LiveBrokers != null)
        {
            foreach (var broker in request.LiveBrokers)
            {
                // Use first endpoint or fall back to legacy host/port
                var endpoint = broker.Endpoints?.FirstOrDefault();
                var host = endpoint?.Host ?? broker.V0Host ?? "localhost";
                var port = endpoint?.Port ?? broker.V0Port;

                _clusterState.RegisterBroker(broker.Id, host, port, broker.Rack);
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

                    _clusterState.UpdatePartitionState(tp, state =>
                    {
                        state.LeaderBrokerId = partitionState.Leader;
                        state.LeaderEpoch = partitionState.LeaderEpoch;
                        state.Replicas.Clear();
                        state.Replicas.AddRange(partitionState.Replicas);
                        state.Isr.Clear();
                        state.Isr.AddRange(partitionState.Isr);
                    });
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

                _clusterState.UpdatePartitionState(tp, state =>
                {
                    state.LeaderBrokerId = partitionState.Leader;
                    state.LeaderEpoch = partitionState.LeaderEpoch;
                    state.Replicas.Clear();
                    state.Replicas.AddRange(partitionState.Replicas);
                    state.Isr.Clear();
                    state.Isr.AddRange(partitionState.Isr);
                });
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

                            var markerBatch = CreateControlBatch(
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
                                    _transactionCoordinator.TransactionIndex.CommitTransaction(
                                        marker.ProducerId,
                                        [tp],
                                        offset);
                                }
                                else
                                {
                                    _transactionCoordinator.TransactionIndex.AbortTransaction(
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

    /// <summary>
    /// Creates a control batch (transaction marker) for inter-broker marker writing.
    /// </summary>
    private static byte[] CreateControlBatch(long producerId, short producerEpoch, short controlType)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        var baseOffset = 0L;
        var baseTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Control record: key = version (int16 = 0), value = {version: int16, type: int16}
        // Use stackalloc for small fixed-size buffers
        Span<byte> controlKey = stackalloc byte[2];
        controlKey[0] = 0;
        controlKey[1] = 0; // Version 0

        Span<byte> controlValue = stackalloc byte[4];
        controlValue[0] = 0;
        controlValue[1] = 0; // Version
        controlValue[2] = (byte)(controlType >> 8);
        controlValue[3] = (byte)(controlType & 0xFF);

        // Build the record
        using var recordStream = new MemoryStream();

        // attributes (int8) = 0
        recordStream.WriteByte(0);

        // timestampDelta (varlong zigzag) = 0
        recordStream.WriteByte(0);

        // offsetDelta (varint zigzag) = 0
        recordStream.WriteByte(0);

        // keyLength (varint zigzag)
        var keyLengthEncoded = KafkaProtocolPrimitives.ZigzagEncode(controlKey.Length);
        WriteVarInt(recordStream, (int)keyLengthEncoded);
        recordStream.Write(controlKey);

        // valueLength (varint zigzag)
        var valueLengthEncoded = KafkaProtocolPrimitives.ZigzagEncode(controlValue.Length);
        WriteVarInt(recordStream, (int)valueLengthEncoded);
        recordStream.Write(controlValue);

        // headers count (varint) = 0
        recordStream.WriteByte(0);

        var recordBytes = recordStream.ToArray();

        // Build the full batch
        short attributes = (short)(Kuestenlogik.Surgewave.Core.KafkaConstants.Attributes.IsTransactionalBit |
                                   Kuestenlogik.Surgewave.Core.KafkaConstants.Attributes.IsControlBatchBit);

        // Calculate batch length
        var recordsLength = 1 + recordBytes.Length;
        var batchLength = Kuestenlogik.Surgewave.Core.KafkaConstants.RecordBatch.HeaderSize -
                         Kuestenlogik.Surgewave.Core.KafkaConstants.RecordBatch.BaseOffsetSize -
                         Kuestenlogik.Surgewave.Core.KafkaConstants.RecordBatch.LengthSize +
                         recordsLength;

        // Write batch header
        WriteBigEndianInt64(writer, baseOffset);
        WriteBigEndianInt32(writer, batchLength);
        WriteBigEndianInt32(writer, 0); // Partition Leader Epoch
        writer.Write(Kuestenlogik.Surgewave.Core.KafkaConstants.Magic.V2);

        // Prepare CRC data
        using var crcStream = new MemoryStream();
        using var crcWriter = new BinaryWriter(crcStream);

        WriteBigEndianInt16(crcWriter, attributes);
        WriteBigEndianInt32(crcWriter, 0); // Last Offset Delta
        WriteBigEndianInt64(crcWriter, baseTimestamp);
        WriteBigEndianInt64(crcWriter, baseTimestamp); // Max Timestamp
        WriteBigEndianInt64(crcWriter, producerId);
        WriteBigEndianInt16(crcWriter, producerEpoch);
        WriteBigEndianInt32(crcWriter, 0); // Base Sequence
        WriteBigEndianInt32(crcWriter, 1); // Record Count

        // Record length (zigzag varint)
        var recordLengthEncoded = KafkaProtocolPrimitives.ZigzagEncode(recordBytes.Length);
        WriteVarInt(crcStream, (int)recordLengthEncoded);
        crcStream.Write(recordBytes, 0, recordBytes.Length);

        var crcData = crcStream.ToArray();
        var crc = Kuestenlogik.Surgewave.Core.Util.Crc32C.Compute(crcData);

        WriteBigEndianUInt32(writer, crc);
        writer.Write(crcData);

        return stream.ToArray();
    }

    private static void WriteVarInt(Stream stream, int value)
    {
        while ((value & ~0x7F) != 0)
        {
            stream.WriteByte((byte)((value & 0x7F) | 0x80));
            value = (int)((uint)value >> 7);
        }
        stream.WriteByte((byte)value);
    }

    private static void WriteBigEndianInt16(BinaryWriter writer, short value)
    {
        Span<byte> buffer = stackalloc byte[2];
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianInt32(BinaryWriter writer, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianInt64(BinaryWriter writer, long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianUInt32(BinaryWriter writer, uint value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        writer.Write(buffer);
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
