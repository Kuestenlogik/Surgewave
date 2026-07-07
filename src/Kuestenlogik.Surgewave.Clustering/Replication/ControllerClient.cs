using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Client for sending Controller API requests to brokers.
/// Used by the controller to broadcast LeaderAndIsr, UpdateMetadata, and StopReplica
/// requests when partition topology changes.
/// </summary>
public sealed partial class ControllerClient : IDisposable, IIsrChangeNotifier
{
    private readonly ConnectionPool _connectionPool;
    private readonly ClusterState _clusterState;
    private readonly ClusteringConfig _config;
    private readonly ILogger<ControllerClient> _logger;
    private int _correlationId;
    private bool _disposed;

    public ControllerClient(
        ConnectionPool connectionPool,
        ClusterState clusterState,
        ClusteringConfig config,
        ILogger<ControllerClient> logger)
    {
        _connectionPool = connectionPool;
        _clusterState = clusterState;
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Send LeaderAndIsr requests to all affected brokers.
    /// Groups partition changes by broker and sends one request per broker.
    /// </summary>
    public async Task<Dictionary<int, LeaderAndIsrResponse>> SendLeaderAndIsrAsync(
        IEnumerable<(TopicPartition Tp, PartitionState State)> partitionChanges,
        CancellationToken ct = default)
    {
        var results = new Dictionary<int, LeaderAndIsrResponse>();

        // Group by affected brokers (leader and all replicas)
        var brokerPartitions = new Dictionary<int, List<(TopicPartition Tp, PartitionState State)>>();
        foreach (var (tp, state) in partitionChanges)
        {
            foreach (var brokerId in state.Replicas)
            {
                if (!brokerPartitions.TryGetValue(brokerId, out var list))
                {
                    list = [];
                    brokerPartitions[brokerId] = list;
                }
                list.Add((tp, state));
            }
        }

        // Send to each broker in parallel
        var tasks = brokerPartitions
            .Where(kvp => kvp.Key != _config.BrokerId) // Don't send to self
            .Select(kvp => SendLeaderAndIsrToBrokerAsync(kvp.Key, kvp.Value, ct));

        var responses = await Task.WhenAll(tasks);
        foreach (var (brokerId, response) in responses)
        {
            if (response != null)
            {
                results[brokerId] = response;
            }
        }

        return results;
    }

    private async Task<(int BrokerId, LeaderAndIsrResponse? Response)> SendLeaderAndIsrToBrokerAsync(
        int brokerId,
        List<(TopicPartition Tp, PartitionState State)> partitions,
        CancellationToken ct)
    {
        var broker = _clusterState.GetBroker(brokerId);
        if (broker == null)
        {
            LogBrokerNotFound(brokerId);
            return (brokerId, null);
        }

        try
        {
            // Build topic states grouped by topic
            var topicGroups = partitions.GroupBy(p => p.Tp.Topic);
            var topicStates = new List<LeaderAndIsrRequest.LeaderAndIsrTopicState>();

            foreach (var group in topicGroups)
            {
                var topicMeta = _clusterState.GetTopic(group.Key);
                var partitionStates = new List<LeaderAndIsrRequest.LeaderAndIsrPartitionState>();

                foreach (var (tp, state) in group)
                {
                    partitionStates.Add(new LeaderAndIsrRequest.LeaderAndIsrPartitionState
                    {
                        PartitionIndex = tp.Partition,
                        ControllerEpoch = _clusterState.ControllerEpoch,
                        Leader = state.LeaderBrokerId,
                        LeaderEpoch = state.LeaderEpoch,
                        Isr = state.Isr.ToList(),
                        PartitionEpoch = state.LeaderEpoch,
                        Replicas = state.Replicas.ToList(),
                        AddingReplicas = [],
                        RemovingReplicas = [],
                        IsNew = false
                    });
                }

                topicStates.Add(new LeaderAndIsrRequest.LeaderAndIsrTopicState
                {
                    TopicName = group.Key,
                    TopicId = topicMeta?.TopicId ?? Guid.Empty,
                    PartitionStates = partitionStates
                });
            }

            // Build live leaders list
            var liveLeaders = _clusterState.Brokers.Values
                .Select(b => new LeaderAndIsrRequest.LeaderAndIsrLiveLeader
                {
                    BrokerId = b.BrokerId,
                    Host = b.Host,
                    Port = b.Port
                })
                .ToList();

            var request = new LeaderAndIsrRequest
            {
                ApiKey = ApiKey.LeaderAndIsr,
                ApiVersion = 4, // v4+ is flexible
                CorrelationId = Interlocked.Increment(ref _correlationId),
                ClientId = $"surgewave-controller-{_config.BrokerId}",
                ControllerId = _config.BrokerId,
                IsKRaftController = _config.UseRaftConsensus,
                ControllerEpoch = _clusterState.ControllerEpoch,
                BrokerEpoch = -1,
                Type = 0, // Full update
                TopicStates = topicStates,
                LiveLeaders = liveLeaders
            };

            var response = await SendRequestAsync<LeaderAndIsrResponse>(
                broker.Host, broker.Port, request,
                (reader, version, corrId) => LeaderAndIsrResponse.ReadFrom(reader, version, corrId),
                ct);

            LogLeaderAndIsrSent(brokerId, partitions.Count, response?.ErrorCode ?? ErrorCode.Unknown);
            return (brokerId, response);
        }
        catch (Exception ex)
        {
            LogLeaderAndIsrFailed(brokerId, ex);
            return (brokerId, null);
        }
    }

    /// <summary>
    /// Reverse ISR propagation (#69): a partition leader reports its new ISR to
    /// the controller. If this broker IS the controller, the ISR is already in
    /// the shared ClusterState (the leader mutated it directly), so this only
    /// re-broadcasts LeaderAndIsr to the other replicas — no self-RPC. Otherwise
    /// it sends an AlterPartition request (v3) to the controller's CLIENT port,
    /// exactly like the forward LeaderAndIsr send. Best-effort: a failure just
    /// means the ISR reconciles on the next fetch report.
    /// </summary>
    public async Task NotifyIsrChangedAsync(
        TopicPartition tp,
        int leaderId,
        int leaderEpoch,
        IReadOnlyList<int> isr,
        CancellationToken ct = default)
    {
        var controllerId = _clusterState.ControllerId;
        if (controllerId < 0)
            return;

        if (controllerId == _config.BrokerId)
        {
            var state = _clusterState.GetPartitionState(tp);
            if (state != null)
            {
                await SendLeaderAndIsrAsync([(tp, state)], ct).ConfigureAwait(false);
            }
            return;
        }

        var controller = _clusterState.GetBroker(controllerId);
        if (controller == null)
        {
            LogBrokerNotFound(controllerId);
            return;
        }

        try
        {
            // Use v1 on purpose: it carries the topic NAME over the wire, which
            // the leader always knows (tp.Topic). v2+ would carry only a TopicId,
            // and a leader that learned the partition via LeaderAndIsr may not
            // hold the topic metadata, so it would send Guid.Empty and the
            // controller could not resolve the topic (ISR update silently
            // dropped as UnknownTopicId). v1's flat NewIsr is sufficient — the
            // reverse path doesn't need per-broker epochs (#69).
            var request = new AlterPartitionRequest
            {
                ApiKey = ApiKey.AlterPartition,
                ApiVersion = 1, // v0-1: TopicName + flat NewIsr
                CorrelationId = Interlocked.Increment(ref _correlationId),
                ClientId = $"surgewave-leader-{_config.BrokerId}",
                BrokerId = _config.BrokerId,
                BrokerEpoch = -1,
                Topics =
                [
                    new AlterPartitionRequest.TopicData
                    {
                        TopicName = tp.Topic,
                        Partitions =
                        [
                            new AlterPartitionRequest.PartitionData
                            {
                                PartitionIndex = tp.Partition,
                                LeaderEpoch = leaderEpoch,
                                PartitionEpoch = leaderEpoch,
                                LeaderRecoveryState = 0,
                                NewIsr = isr.ToList()
                            }
                        ]
                    }
                ]
            };

            var response = await SendRequestAsync<AlterPartitionResponse>(
                controller.Host, controller.Port, request,
                (reader, version, corrId) => AlterPartitionResponse.ReadFrom(reader, version, corrId),
                ct).ConfigureAwait(false);

            LogAlterPartitionSent(controllerId, tp.Topic, tp.Partition, response?.ErrorCode ?? ErrorCode.Unknown);
        }
        catch (Exception ex)
        {
            LogAlterPartitionFailed(controllerId, tp.Topic, tp.Partition, ex);
        }
    }

    /// <summary>
    /// Send UpdateMetadata request to all brokers.
    /// </summary>
    public async Task<Dictionary<int, UpdateMetadataResponse>> SendUpdateMetadataAsync(
        IEnumerable<(TopicPartition Tp, PartitionState State)>? partitionStates = null,
        CancellationToken ct = default)
    {
        var results = new Dictionary<int, UpdateMetadataResponse>();

        // Get all partitions if not specified
        var partitions = partitionStates?.ToList() ?? _clusterState.GetAllPartitionStates().ToList();

        // Send to all brokers except self
        var tasks = _clusterState.Brokers.Values
            .Where(b => b.BrokerId != _config.BrokerId)
            .Select(b => SendUpdateMetadataToBrokerAsync(b, partitions, ct));

        var responses = await Task.WhenAll(tasks);
        foreach (var (brokerId, response) in responses)
        {
            if (response != null)
            {
                results[brokerId] = response;
            }
        }

        return results;
    }

    private async Task<(int BrokerId, UpdateMetadataResponse? Response)> SendUpdateMetadataToBrokerAsync(
        BrokerNode broker,
        List<(TopicPartition Tp, PartitionState State)> partitions,
        CancellationToken ct)
    {
        try
        {
            // Build topic states
            var topicGroups = partitions.GroupBy(p => p.Tp.Topic);
            var topicStates = new List<UpdateMetadataRequest.UpdateMetadataTopicState>();

            foreach (var group in topicGroups)
            {
                var topicMeta = _clusterState.GetTopic(group.Key);
                var partitionStatesList = new List<UpdateMetadataRequest.UpdateMetadataPartitionState>();

                foreach (var (tp, state) in group)
                {
                    partitionStatesList.Add(new UpdateMetadataRequest.UpdateMetadataPartitionState
                    {
                        PartitionIndex = tp.Partition,
                        ControllerEpoch = _clusterState.ControllerEpoch,
                        Leader = state.LeaderBrokerId,
                        LeaderEpoch = state.LeaderEpoch,
                        Isr = state.Isr.ToList(),
                        ZkVersion = 0,
                        Replicas = state.Replicas.ToList(),
                        OfflineReplicas = state.OfflineReplicas?.ToList()
                    });
                }

                topicStates.Add(new UpdateMetadataRequest.UpdateMetadataTopicState
                {
                    TopicName = group.Key,
                    TopicId = topicMeta?.TopicId ?? Guid.Empty,
                    PartitionStates = partitionStatesList
                });
            }

            // Build live brokers list
            var liveBrokers = _clusterState.Brokers.Values
                .Select(b => new UpdateMetadataRequest.UpdateMetadataBroker
                {
                    Id = b.BrokerId,
                    Endpoints =
                    [
                        new UpdateMetadataRequest.UpdateMetadataEndpoint
                        {
                            Port = b.Port,
                            Host = b.Host,
                            Listener = "PLAINTEXT",
                            SecurityProtocol = 0 // PLAINTEXT
                        }
                    ],
                    Rack = b.Rack
                })
                .ToList();

            var request = new UpdateMetadataRequest
            {
                ApiKey = ApiKey.UpdateMetadata,
                ApiVersion = 6, // v6+ is flexible
                CorrelationId = Interlocked.Increment(ref _correlationId),
                ClientId = $"surgewave-controller-{_config.BrokerId}",
                ControllerId = _config.BrokerId,
                IsKRaftController = _config.UseRaftConsensus,
                ControllerEpoch = _clusterState.ControllerEpoch,
                BrokerEpoch = -1,
                TopicStates = topicStates,
                LiveBrokers = liveBrokers
            };

            var response = await SendRequestAsync<UpdateMetadataResponse>(
                broker.Host, broker.Port, request,
                (reader, version, corrId) => UpdateMetadataResponse.ReadFrom(reader, version, corrId),
                ct);

            LogUpdateMetadataSent(broker.BrokerId, partitions.Count, response?.ErrorCode ?? ErrorCode.Unknown);
            return (broker.BrokerId, response);
        }
        catch (Exception ex)
        {
            LogUpdateMetadataFailed(broker.BrokerId, ex);
            return (broker.BrokerId, null);
        }
    }

    /// <summary>
    /// Send StopReplica request to a specific broker.
    /// </summary>
    public async Task<StopReplicaResponse?> SendStopReplicaAsync(
        int brokerId,
        IEnumerable<(TopicPartition Tp, int LeaderEpoch, bool DeletePartition)> partitions,
        CancellationToken ct = default)
    {
        var broker = _clusterState.GetBroker(brokerId);
        if (broker == null)
        {
            LogBrokerNotFound(brokerId);
            return null;
        }

        try
        {
            var partitionList = partitions.ToList();

            // Build topic states (v3+ format)
            var topicGroups = partitionList.GroupBy(p => p.Tp.Topic);
            var topicStates = new List<StopReplicaRequest.StopReplicaTopicState>();

            foreach (var group in topicGroups)
            {
                var partitionStatesList = group.Select(p => new StopReplicaRequest.StopReplicaPartitionState
                {
                    PartitionIndex = p.Tp.Partition,
                    LeaderEpoch = p.LeaderEpoch,
                    DeletePartition = p.DeletePartition
                }).ToList();

                topicStates.Add(new StopReplicaRequest.StopReplicaTopicState
                {
                    TopicName = group.Key,
                    PartitionStates = partitionStatesList
                });
            }

            var request = new StopReplicaRequest
            {
                ApiKey = ApiKey.StopReplica,
                ApiVersion = 3, // v3+ uses TopicStates format
                CorrelationId = Interlocked.Increment(ref _correlationId),
                ClientId = $"surgewave-controller-{_config.BrokerId}",
                ControllerId = _config.BrokerId,
                IsKRaftController = _config.UseRaftConsensus,
                ControllerEpoch = _clusterState.ControllerEpoch,
                BrokerEpoch = -1,
                TopicStates = topicStates
            };

            var response = await SendRequestAsync<StopReplicaResponse>(
                broker.Host, broker.Port, request,
                (reader, version, corrId) => StopReplicaResponse.ReadFrom(reader, version, corrId),
                ct);

            LogStopReplicaSent(brokerId, partitionList.Count, response?.ErrorCode ?? ErrorCode.Unknown);
            return response;
        }
        catch (Exception ex)
        {
            LogStopReplicaFailed(brokerId, ex);
            return null;
        }
    }

    /// <summary>
    /// Send a request and receive a response using the Kafka protocol.
    /// </summary>
    /// <summary>
    /// Upper bound on a single controller-to-broker round-trip. Without it an
    /// unreachable or wedged follower would block the caller forever, because
    /// <see cref="ReadExactlyAsync"/> only observes the supplied token — and
    /// the callers (topic create, leader reelection) await the send on their
    /// critical path. Bounding it keeps a slow broker from stalling the whole
    /// controller; the send is best-effort and the ISR reconciles on the next
    /// fetch cycle anyway (#69).
    /// </summary>
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    private async Task<TResponse?> SendRequestAsync<TResponse>(
        string host,
        int port,
        KafkaRequest request,
        Func<KafkaProtocolReader, short, int, TResponse> responseReader,
        CancellationToken ct) where TResponse : KafkaResponse
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(RequestTimeout);
        var timeoutToken = timeoutCts.Token;

        var connection = await _connectionPool.GetConnectionAsync(host, port, timeoutToken);
        try
        {
            var stream = connection.Stream;

            // Serialize request
            var requestBytes = request.Serialize();

            // Write size-prefixed message
            var sizeBuffer = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(sizeBuffer, requestBytes.Length);
            await stream.WriteAsync(sizeBuffer, timeoutToken);
            await stream.WriteAsync(requestBytes, timeoutToken);
            await stream.FlushAsync(timeoutToken);

            // Read response size
            var responseSizeBuffer = new byte[4];
            await ReadExactlyAsync(stream, responseSizeBuffer, timeoutToken);
            var responseSize = BinaryPrimitives.ReadInt32BigEndian(responseSizeBuffer);

            // Read response body
            var responseBuffer = new byte[responseSize];
            await ReadExactlyAsync(stream, responseBuffer, timeoutToken);

            // Parse response
            var reader = new KafkaProtocolReader(responseBuffer);
            var correlationId = reader.ReadInt32();

            return responseReader(reader, request.ApiVersion, correlationId);
        }
        finally
        {
            connection.Return();
        }
    }

    private static async Task ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken ct)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), ct);
            if (read == 0)
                throw new EndOfStreamException("Connection closed while reading response");
            totalRead += read;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        // ConnectionPool is shared and disposed elsewhere
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Broker {BrokerId} not found in cluster state")]
    private partial void LogBrokerNotFound(int brokerId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "LeaderAndIsr sent to broker {BrokerId} for {PartitionCount} partitions, result: {ErrorCode}")]
    private partial void LogLeaderAndIsrSent(int brokerId, int partitionCount, ErrorCode errorCode);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send LeaderAndIsr to broker {BrokerId}")]
    private partial void LogLeaderAndIsrFailed(int brokerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "UpdateMetadata sent to broker {BrokerId} for {PartitionCount} partitions, result: {ErrorCode}")]
    private partial void LogUpdateMetadataSent(int brokerId, int partitionCount, ErrorCode errorCode);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send UpdateMetadata to broker {BrokerId}")]
    private partial void LogUpdateMetadataFailed(int brokerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "StopReplica sent to broker {BrokerId} for {PartitionCount} partitions, result: {ErrorCode}")]
    private partial void LogStopReplicaSent(int brokerId, int partitionCount, ErrorCode errorCode);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send StopReplica to broker {BrokerId}")]
    private partial void LogStopReplicaFailed(int brokerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AlterPartition sent to controller {ControllerId} for {Topic}-{Partition}, result: {ErrorCode}")]
    private partial void LogAlterPartitionSent(int controllerId, string topic, int partition, ErrorCode errorCode);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send AlterPartition to controller {ControllerId} for {Topic}-{Partition}")]
    private partial void LogAlterPartitionFailed(int controllerId, string topic, int partition, Exception ex);
}
