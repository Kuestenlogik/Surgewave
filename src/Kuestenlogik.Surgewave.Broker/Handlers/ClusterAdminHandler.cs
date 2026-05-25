using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Wire handlers for the controller-driven cluster-admin RPCs:
/// <list type="bullet">
///   <item><c>ElectLeaders</c> (43) — preferred / unclean leader election.</item>
///   <item><c>AlterPartitionReassignments</c> (45) — schedule replica moves.</item>
///   <item><c>ListPartitionReassignments</c> (46) — observe in-flight moves.</item>
/// </list>
/// All three require a running controller — when this broker is not the
/// controller (single-broker dev mode without consensus) the handler answers
/// with <see cref="ErrorCode.NotController"/> and the client retries against
/// whichever broker <c>Metadata</c> currently advertises.
/// </summary>
public sealed partial class ClusterAdminHandler : IKafkaRequestHandler
{
    private readonly ClusterController _controller;
    private readonly PartitionReassignmentManager? _reassignmentManager;
    private readonly ClusterState _clusterState;
    private readonly ILogger<ClusterAdminHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.ElectLeaders,
        ApiKey.AlterPartitionReassignments,
        ApiKey.ListPartitionReassignments,
    ];

    public ClusterAdminHandler(
        ClusterController controller,
        PartitionReassignmentManager? reassignmentManager,
        ClusterState clusterState,
        ILogger<ClusterAdminHandler> logger)
    {
        _controller = controller;
        _reassignmentManager = reassignmentManager;
        _clusterState = clusterState;
        _logger = logger;
    }

    public async Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return request switch
        {
            ElectLeadersRequest electLeadersRequest => await HandleElectLeadersAsync(electLeadersRequest, cancellationToken),
            AlterPartitionReassignmentsRequest alterReq => await HandleAlterPartitionReassignmentsAsync(alterReq, cancellationToken),
            ListPartitionReassignmentsRequest listReq => HandleListPartitionReassignments(listReq),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by ClusterAdminHandler")
        };
    }

    private async Task<ElectLeadersResponse> HandleElectLeadersAsync(ElectLeadersRequest request, CancellationToken cancellationToken)
    {
        // ElectionType: 0 = preferred replica election, 1 = unclean. Surgewave's
        // ClusterController.ElectLeaderAsync chooses preferred-from-ISR or
        // first-ISR-member by default and only falls back to unclean when
        // ISR is empty; client-requested unclean (1) takes the same path.
        // Honouring ElectionType=0 strictly (refuse fallback) would need an
        // extra param on ClusterController — tracked but not blocking.

        // Null TopicPartitionsList means "elect for all" per Kafka spec.
        var requested = ResolveRequestedTopicPartitions(request.TopicPartitionsList);

        // Group results by topic for the response shape.
        var perTopic = new Dictionary<string, List<ElectLeadersResponse.PartitionResult>>(StringComparer.Ordinal);
        foreach (var (topic, partitions) in requested)
        {
            if (!perTopic.TryGetValue(topic, out var list))
            {
                list = [];
                perTopic[topic] = list;
            }

            foreach (var partition in partitions)
            {
                var tp = new TopicPartition { Topic = topic, Partition = partition };
                bool ok;
                ErrorCode error;
                string? message;
                try
                {
                    ok = await _controller.ElectLeaderAsync(tp, preferredLeader: null, cancellationToken).ConfigureAwait(false);
                    error = ok ? ErrorCode.None : ErrorCode.PreferredLeaderNotAvailable;
                    message = ok ? null : "Election failed (no eligible replica or not controller)";
                }
                catch (Exception ex)
                {
                    LogElectLeaderFailure(ex, topic, partition);
                    error = ErrorCode.Unknown;
                    message = ex.Message;
                }

                list.Add(new ElectLeadersResponse.PartitionResult
                {
                    PartitionId = partition,
                    ErrorCode = error,
                    ErrorMessage = message,
                });
            }
        }

        var results = perTopic.Select(kv => new ElectLeadersResponse.ReplicaElectionResult
        {
            Topic = kv.Key,
            PartitionResults = kv.Value,
        }).ToList();

        return new ElectLeadersResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = _controller.IsController ? ErrorCode.None : ErrorCode.NotController,
            ReplicaElectionResults = results,
        };
    }

    private IEnumerable<(string Topic, IEnumerable<int> Partitions)> ResolveRequestedTopicPartitions(
        List<ElectLeadersRequest.TopicPartitions>? topicPartitionsList)
    {
        if (topicPartitionsList is { Count: > 0 })
        {
            foreach (var tp in topicPartitionsList)
            {
                yield return (tp.Topic, tp.Partitions);
            }
            yield break;
        }

        // Null / empty → elect for every partition the cluster knows.
        var grouped = _clusterState.PartitionStates
            .GroupBy(kv => kv.Key.Topic, StringComparer.Ordinal);
        foreach (var g in grouped)
        {
            yield return (g.Key, g.Select(kv => kv.Key.Partition));
        }
    }

    private async Task<AlterPartitionReassignmentsResponse> HandleAlterPartitionReassignmentsAsync(
        AlterPartitionReassignmentsRequest request,
        CancellationToken cancellationToken)
    {
        // Build the underlying ReassignmentPlan from the Kafka-wire request.
        // A null Replicas list (Kafka convention) means "cancel any in-flight
        // reassignment for that partition". The PartitionReassignmentManager
        // in Surgewave's clustering layer does not yet expose a cancel API, so
        // we surface a per-partition error rather than silently no-op'ing.
        if (_reassignmentManager is null || !_controller.IsController)
        {
            return new AlterPartitionReassignmentsResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.NotController,
                ErrorMessage = "Not the controller — retry against the broker advertised by Metadata",
                Responses = [],
            };
        }

        var plan = new ReassignmentPlan { Version = 1, Partitions = [] };
        var perPartitionErrors = new List<(string Topic, int Partition, ErrorCode Error, string? Message)>();

        foreach (var topicReq in request.Topics)
        {
            foreach (var partitionReq in topicReq.Partitions)
            {
                if (partitionReq.Replicas is null || partitionReq.Replicas.Count == 0)
                {
                    perPartitionErrors.Add((topicReq.Name, partitionReq.PartitionIndex,
                        ErrorCode.InvalidRequest,
                        "Cancelling an in-flight reassignment is not yet supported"));
                    continue;
                }
                plan.Partitions.Add(new PartitionReassignment
                {
                    Topic = topicReq.Name,
                    Partition = partitionReq.PartitionIndex,
                    Replicas = partitionReq.Replicas,
                });
            }
        }

        if (plan.Partitions.Count > 0)
        {
            try
            {
                await _reassignmentManager.ExecuteReassignmentAsync(plan, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogAlterReassignmentFailure(ex);
                // Mark every partition that wasn't already an error with Unknown.
                foreach (var p in plan.Partitions)
                {
                    perPartitionErrors.Add((p.Topic, p.Partition, ErrorCode.Unknown, ex.Message));
                }
            }
        }

        var perTopic = new Dictionary<string, List<AlterPartitionReassignmentsResponse.ReassignablePartitionResponse>>(StringComparer.Ordinal);
        foreach (var topicReq in request.Topics)
        {
            var partitionsResp = new List<AlterPartitionReassignmentsResponse.ReassignablePartitionResponse>();
            foreach (var partitionReq in topicReq.Partitions)
            {
                var err = perPartitionErrors.FirstOrDefault(e => e.Topic == topicReq.Name && e.Partition == partitionReq.PartitionIndex);
                partitionsResp.Add(new AlterPartitionReassignmentsResponse.ReassignablePartitionResponse
                {
                    PartitionIndex = partitionReq.PartitionIndex,
                    ErrorCode = err.Error,
                    ErrorMessage = err.Message,
                });
            }
            perTopic[topicReq.Name] = partitionsResp;
        }

        return new AlterPartitionReassignmentsResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Responses = perTopic.Select(kv => new AlterPartitionReassignmentsResponse.ReassignableTopicResponse
            {
                Name = kv.Key,
                Partitions = kv.Value,
            }).ToList(),
        };
    }

    private ListPartitionReassignmentsResponse HandleListPartitionReassignments(ListPartitionReassignmentsRequest request)
    {
        if (_reassignmentManager is null)
        {
            return new ListPartitionReassignmentsResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.None, // empty list is a valid answer
                ErrorMessage = null,
                Topics = [],
            };
        }

        var active = _reassignmentManager.GetActiveReassignments();

        // If the request specified a filter, narrow the projection.
        IEnumerable<Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignmentState> filtered = active;
        if (request.Topics is { Count: > 0 })
        {
            var wanted = request.Topics.ToDictionary(
                t => t.Name,
                t => t.PartitionIndexes.Count == 0 ? null : new HashSet<int>(t.PartitionIndexes),
                StringComparer.Ordinal);
            filtered = active.Where(s =>
                wanted.TryGetValue(s.Topic, out var partitions)
                && (partitions is null || partitions.Contains(s.Partition)));
        }

        var topicGroups = filtered
            .GroupBy(s => s.Topic, StringComparer.Ordinal)
            .Select(g => new ListPartitionReassignmentsResponse.OngoingTopicReassignment
            {
                Name = g.Key,
                Partitions = g.Select(s => new ListPartitionReassignmentsResponse.OngoingPartitionReassignment
                {
                    PartitionIndex = s.Partition,
                    Replicas = s.TargetReplicas,
                    AddingReplicas = s.AddingReplicas,
                    RemovingReplicas = s.RemovingReplicas,
                }).ToList(),
            })
            .ToList();

        return new ListPartitionReassignmentsResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Topics = topicGroups,
        };
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "ElectLeader failed for {Topic}/{Partition}")]
    private partial void LogElectLeaderFailure(Exception ex, string topic, int partition);

    [LoggerMessage(Level = LogLevel.Warning, Message = "AlterPartitionReassignments execute call threw")]
    private partial void LogAlterReassignmentFailure(Exception ex);
}
