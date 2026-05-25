using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Kuestenlogik.Surgewave.Broker.Health;

/// <summary>
/// Health check for Surgewave broker components.
/// </summary>
public sealed class BrokerHealthCheck : IHealthCheck
{
    private readonly ClusterState _clusterState;
    private readonly BrokerConfig _config;
    private readonly RaftNode? _raftNode;

    public BrokerHealthCheck(ClusterState clusterState, BrokerConfig config, RaftNode? raftNode = null)
    {
        _clusterState = clusterState;
        _config = config;
        _raftNode = raftNode;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var data = new Dictionary<string, object>
        {
            ["broker_id"] = _config.BrokerId,
            ["host"] = _config.Host,
            ["port"] = _config.Port,
            ["grpc_port"] = _config.GrpcPort,
            ["topics_count"] = _clusterState.Topics.Count,
            ["brokers_count"] = _clusterState.Brokers.Count,
            ["controller_id"] = _clusterState.ControllerId
        };

        if (_raftNode != null)
        {
            data["raft_state"] = _raftNode.State.ToString();
            data["raft_term"] = _raftNode.CurrentTerm;
            data["raft_leader_id"] = _raftNode.LeaderId ?? -1;
            data["raft_commit_index"] = _raftNode.CommitIndex;

            // Degraded if in candidate state (election in progress)
            if (_raftNode.State == RaftState.Candidate)
            {
                return Task.FromResult(HealthCheckResult.Degraded(
                    "Raft election in progress",
                    data: data));
            }
        }

        return Task.FromResult(HealthCheckResult.Healthy(
            "Broker is healthy",
            data: data));
    }
}
