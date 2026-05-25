using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;
using BrokerInfo = Kuestenlogik.Surgewave.Api.Grpc.BrokerInfo;
using TopicInfo = Kuestenlogik.Surgewave.Api.Grpc.TopicInfo;

namespace Kuestenlogik.Surgewave.Gateway.Services;

/// <summary>
/// gRPC service implementation for cluster operations.
/// Uses Surgewave native client to communicate with the broker.
/// </summary>
public sealed class ClusterGatewayService : ClusterService.ClusterServiceBase
{
    private readonly ClusterRegistry _registry;
    private readonly ILogger<ClusterGatewayService> _logger;

    public ClusterGatewayService(
        ClusterRegistry registry,
        ILogger<ClusterGatewayService> logger)
    {
        _registry = registry;
        _logger = logger;
    }

    public override async Task<PingResponse> Ping(PingRequest request, ServerCallContext context)
    {
        var client = _registry.GetClient(null); // Ping uses default cluster
        var timestamp = await client.Messaging.PingAsync(context.CancellationToken);
        return new PingResponse { Timestamp = timestamp };
    }

    public override async Task<GetMetadataResponse> GetMetadata(GetMetadataRequest request, ServerCallContext context)
    {
        try
        {
            var clusterId = string.IsNullOrEmpty(request.ClusterId) ? _registry.DefaultClusterId : request.ClusterId;
            var client = _registry.GetClient(clusterId);
            var clusterInfo = await client.Cluster.GetClusterInfoAsync(context.CancellationToken);
            var brokers = await client.Cluster.ListBrokersAsync(context.CancellationToken);
            var topics = await client.Topics.ListAsync(context.CancellationToken);

            var response = new GetMetadataResponse
            {
                ClusterId = clusterId,
                ControllerId = clusterInfo.ControllerId
            };

            foreach (var broker in brokers)
            {
                response.Brokers.Add(new BrokerInfo
                {
                    BrokerId = broker.BrokerId,
                    Host = broker.Host,
                    Port = broker.Port,
                    Rack = broker.Rack ?? ""
                });
            }

            var topicFilter = request.Topics.Count > 0 ? request.Topics.ToHashSet() : null;

            foreach (var topic in topics)
            {
                if (topicFilter != null && !topicFilter.Contains(topic.Name))
                    continue;

                response.Topics.Add(new TopicInfo
                {
                    Name = topic.Name,
                    NumPartitions = topic.PartitionCount
                });
            }

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get metadata");
            throw new RpcException(new Status(StatusCode.Internal, ex.Message));
        }
    }

    public override async Task<GetClusterInfoResponse> GetClusterInfo(GetClusterInfoRequest request, ServerCallContext context)
    {
        try
        {
            var clusterId = string.IsNullOrEmpty(request.ClusterId) ? _registry.DefaultClusterId : request.ClusterId;
            var client = _registry.GetClient(clusterId);
            var clusterInfo = await client.Cluster.GetClusterInfoAsync(context.CancellationToken);
            var brokers = await client.Cluster.ListBrokersAsync(context.CancellationToken);

            var response = new GetClusterInfoResponse
            {
                ClusterId = clusterId,
                ControllerId = clusterInfo.ControllerId,
                TopicCount = clusterInfo.TopicCount,
                PartitionCount = clusterInfo.TotalPartitions,
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };

            foreach (var broker in brokers)
            {
                response.Brokers.Add(new BrokerInfo
                {
                    BrokerId = broker.BrokerId,
                    Host = broker.Host,
                    Port = broker.Port,
                    Rack = broker.Rack ?? ""
                });
            }

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get cluster info");
            return new GetClusterInfoResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<ListBrokersResponse> ListBrokers(ListBrokersRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var brokers = await client.Cluster.ListBrokersAsync(context.CancellationToken);

            var response = new ListBrokersResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };

            foreach (var broker in brokers)
            {
                response.Brokers.Add(new BrokerInfo
                {
                    BrokerId = broker.BrokerId,
                    Host = broker.Host,
                    Port = broker.Port,
                    Rack = broker.Rack ?? ""
                });
            }

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list brokers");
            return new ListBrokersResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<AlterReassignmentsResponse> AlterPartitionReassignments(
        AlterReassignmentsRequest request,
        ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var reassignments = request.Reassignments
                .Select(r => new Client.Native.Operations.Cluster.PartitionReassignmentRequest(
                    r.Topic, r.Partition, r.Replicas.ToList()))
                .ToList();

            var result = await client.Cluster.AlterPartitionReassignmentsAsync(reassignments, context.CancellationToken);

            var response = new AlterReassignmentsResponse();

            // The result indicates overall success/failure
            // Add a response entry for each requested reassignment
            foreach (var r in request.Reassignments)
            {
                response.Results.Add(new ReassignmentResult
                {
                    Topic = r.Topic,
                    Partition = r.Partition,
                    Status = new ResponseStatus
                    {
                        ErrorCode = result.Success ? ErrorCode.None : ErrorCode.Unknown
                    }
                });
            }

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to alter partition reassignments");
            throw new RpcException(new Status(StatusCode.Internal, ex.Message));
        }
    }

    public override async Task<ListReassignmentsResponse> ListPartitionReassignments(
        ListReassignmentsRequest request,
        ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var reassignments = await client.Cluster.ListPartitionReassignmentsAsync(context.CancellationToken);

            var response = new ListReassignmentsResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };

            foreach (var r in reassignments)
            {
                response.Reassignments.Add(new OngoingReassignment
                {
                    Topic = r.Topic,
                    Partition = r.Partition,
                    Replicas = { r.TargetReplicas },
                    AddingReplicas = { r.TargetReplicas.Except(r.OriginalReplicas) },
                    RemovingReplicas = { r.OriginalReplicas.Except(r.TargetReplicas) }
                });
            }

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list partition reassignments");
            return new ListReassignmentsResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<TriggerCompactionResponse> TriggerLogCompaction(
        TriggerCompactionRequest request,
        ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            await client.Cluster.TriggerLogCompactionAsync(context.CancellationToken);

            return new TriggerCompactionResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to trigger log compaction");
            return new TriggerCompactionResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<GetCompactionStatusResponse> GetCompactionStatus(
        GetCompactionStatusRequest request,
        ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var statuses = await client.Cluster.GetCompactionStatusAsync(context.CancellationToken);

            var response = new GetCompactionStatusResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };

            foreach (var s in statuses)
            {
                // TopicCompactionStatus has topic-level info, not per-partition
                // Map to per-partition status with estimated values
                var isCompactPolicy = s.CleanupPolicy.Contains("compact", StringComparison.OrdinalIgnoreCase);

                for (int partition = 0; partition < s.PartitionCount; partition++)
                {
                    response.Statuses.Add(new CompactionStatus
                    {
                        Topic = s.Topic,
                        Partition = partition,
                        CompactionEnabled = isCompactPolicy,
                        LastCompactionTime = 0, // Not available from topic-level status
                        CompactionRatio = s.TotalBytes > 0 ? 1.0 : 0.0
                    });
                }
            }

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get compaction status");
            return new GetCompactionStatusResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }
}
