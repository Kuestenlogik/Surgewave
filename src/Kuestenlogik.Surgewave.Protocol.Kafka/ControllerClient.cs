using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Kafka-wire client for the ONE inter-broker request a broker still originates: a leader's
/// AlterPartition report to the controller (#69). It is the Kafka-wire fallback behind
/// <c>GatedControllerReplicaRpc</c>, used while the cluster has not finalized to the native wire.
/// The controller-push senders (LeaderAndIsr / UpdateMetadata / StopReplica) went with the push
/// model itself (#163 step 3): the controller now replicates its decisions through the Raft log.
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
            // We ARE the controller, so this does not go on the wire. It used to re-broadcast
            // LeaderAndIsr, which was the push model's answer; the native client routes the same
            // case into the metadata log through IIsrUpdateApplier (#176), and this wire client
            // has no applier — the caller on this broker is the native path.
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

    [LoggerMessage(Level = LogLevel.Debug, Message = "AlterPartition sent to controller {ControllerId} for {Topic}-{Partition}, result: {ErrorCode}")]
    private partial void LogAlterPartitionSent(int controllerId, string topic, int partition, ErrorCode errorCode);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send AlterPartition to controller {ControllerId} for {Topic}-{Partition}")]
    private partial void LogAlterPartitionFailed(int controllerId, string topic, int partition, Exception ex);
}
