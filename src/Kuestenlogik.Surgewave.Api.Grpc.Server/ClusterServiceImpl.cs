using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Cluster info result from operations.
/// </summary>
public record ClusterInfoDto(
    int BrokerId,
    string Host,
    int Port,
    int TopicCount,
    int PartitionCount);

/// <summary>
/// Broker info for listing.
/// </summary>
public record BrokerInfoDto(
    int BrokerId,
    string Host,
    int Port,
    bool IsController,
    bool IsAlive,
    string? Rack,
    string PeerTransport = "tcp");

/// <summary>
/// Topic metadata for GetMetadata response.
/// </summary>
public record TopicMetadataDto(
    string Name,
    int PartitionCount,
    List<PartitionMetadataDto> Partitions);

/// <summary>
/// Partition metadata for GetMetadata response.
/// </summary>
public record PartitionMetadataDto(
    int PartitionId,
    int Leader,
    List<int> Replicas,
    List<int> Isr);

/// <summary>
/// Reassignment result.
/// </summary>
public record ReassignmentResultDto(string Topic, int Partition, bool Success, string? ErrorMessage);

/// <summary>
/// Ongoing reassignment state.
/// </summary>
public record OngoingReassignmentDto(
    string Topic,
    int Partition,
    List<int> Replicas,
    List<int> AddingReplicas,
    List<int> RemovingReplicas);

/// <summary>
/// Compaction result.
/// </summary>
public record CompactionResultDto(bool Success, int RecordsRemoved, long BytesRemoved, int SegmentsCompacted);

/// <summary>
/// Compaction status for a partition.
/// </summary>
public record CompactionStatusDto(
    string Topic,
    int Partition,
    bool CompactionEnabled,
    long LastCompactionTime,
    double CompactionRatio);

/// <summary>
/// Delegate to get cluster info.
/// </summary>
public delegate ClusterInfoDto GetClusterInfoDelegate();

/// <summary>
/// Delegate to list brokers.
/// </summary>
public delegate List<BrokerInfoDto> ListBrokersDelegate();

/// <summary>
/// Delegate to get topic metadata.
/// </summary>
public delegate List<TopicMetadataDto> GetMetadataDelegate(List<string>? topics);

/// <summary>
/// Delegate to alter partition reassignments.
/// </summary>
public delegate List<ReassignmentResultDto> AlterReassignmentsDelegate(
    List<(string Topic, int Partition, List<int> Replicas)> reassignments);

/// <summary>
/// Delegate to list ongoing reassignments.
/// </summary>
public delegate List<OngoingReassignmentDto> ListReassignmentsDelegate();

/// <summary>
/// Delegate to trigger log compaction.
/// </summary>
public delegate Task<CompactionResultDto> TriggerCompactionDelegate(List<(string Topic, int Partition)>? partitions);

/// <summary>
/// Delegate to get compaction status.
/// </summary>
public delegate List<CompactionStatusDto> GetCompactionStatusDelegate(List<(string Topic, int Partition)>? partitions);

/// <summary>
/// gRPC ClusterService implementation.
/// </summary>
public class ClusterServiceImpl : ClusterService.ClusterServiceBase
{
    private static BrokerInfo ToBrokerInfo(BrokerInfoDto broker) => new()
    {
        BrokerId = broker.BrokerId,
        Host = broker.Host,
        Port = broker.Port,
        Rack = broker.Rack ?? "",
        PeerTransport = broker.PeerTransport
    };

    private readonly GetClusterInfoDelegate _getClusterInfo;
    private readonly ListBrokersDelegate _listBrokers;
    private readonly GetMetadataDelegate _getMetadata;
    private readonly AlterReassignmentsDelegate _alterReassignments;
    private readonly ListReassignmentsDelegate _listReassignments;
    private readonly TriggerCompactionDelegate _triggerCompaction;
    private readonly GetCompactionStatusDelegate _getCompactionStatus;

    public ClusterServiceImpl(
        GetClusterInfoDelegate getClusterInfo,
        ListBrokersDelegate listBrokers,
        GetMetadataDelegate getMetadata,
        AlterReassignmentsDelegate alterReassignments,
        ListReassignmentsDelegate listReassignments,
        TriggerCompactionDelegate triggerCompaction,
        GetCompactionStatusDelegate getCompactionStatus)
    {
        _getClusterInfo = getClusterInfo;
        _listBrokers = listBrokers;
        _getMetadata = getMetadata;
        _alterReassignments = alterReassignments;
        _listReassignments = listReassignments;
        _triggerCompaction = triggerCompaction;
        _getCompactionStatus = getCompactionStatus;
    }

    public override Task<PingResponse> Ping(PingRequest request, ServerCallContext context)
    {
        return Task.FromResult(new PingResponse
        {
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });
    }

    public override Task<GetMetadataResponse> GetMetadata(GetMetadataRequest request, ServerCallContext context)
    {
        var topics = request.Topics.Count > 0 ? request.Topics.ToList() : null;
        var metadata = _getMetadata(topics);
        var brokers = _listBrokers();
        var clusterInfo = _getClusterInfo();

        var response = new GetMetadataResponse
        {
            ControllerId = clusterInfo.BrokerId,
            ClusterId = $"surgewave-{clusterInfo.BrokerId}"
        };

        response.Brokers.AddRange(brokers.Select(ToBrokerInfo));

        foreach (var topic in metadata)
        {
            var topicInfo = new TopicInfo
            {
                Name = topic.Name,
                NumPartitions = topic.PartitionCount,
                ReplicationFactor = 1,
                IsInternal = topic.Name.StartsWith("__", StringComparison.Ordinal)
            };

            foreach (var partition in topic.Partitions)
            {
                var partInfo = new PartitionInfo
                {
                    PartitionId = partition.PartitionId,
                    Leader = partition.Leader
                };
                partInfo.Replicas.AddRange(partition.Replicas);
                partInfo.Isr.AddRange(partition.Isr);
                topicInfo.Partitions.Add(partInfo);
            }

            response.Topics.Add(topicInfo);
        }

        return Task.FromResult(response);
    }

    public override Task<GetClusterInfoResponse> GetClusterInfo(GetClusterInfoRequest request, ServerCallContext context)
    {
        var info = _getClusterInfo();
        var brokers = _listBrokers();

        var response = new GetClusterInfoResponse
        {
            ClusterId = $"surgewave-{info.BrokerId}",
            ControllerId = info.BrokerId,
            TopicCount = info.TopicCount,
            PartitionCount = info.PartitionCount,
            Status = ResponseStatusFactory.Success
        };

        response.Brokers.AddRange(brokers.Select(ToBrokerInfo));

        return Task.FromResult(response);
    }

    public override Task<ListBrokersResponse> ListBrokers(ListBrokersRequest request, ServerCallContext context)
    {
        var brokers = _listBrokers();

        var response = new ListBrokersResponse
        {
            Status = ResponseStatusFactory.Success
        };

        response.Brokers.AddRange(brokers.Select(ToBrokerInfo));

        return Task.FromResult(response);
    }

    public override Task<AlterReassignmentsResponse> AlterPartitionReassignments(
        AlterReassignmentsRequest request,
        ServerCallContext context)
    {
        var reassignments = request.Reassignments
            .Select(r => (r.Topic, r.Partition, r.Replicas.ToList()))
            .ToList();

        var results = _alterReassignments(reassignments);

        var response = new AlterReassignmentsResponse();
        foreach (var result in results)
        {
            response.Results.Add(new ReassignmentResult
            {
                Topic = result.Topic,
                Partition = result.Partition,
                Status = result.Success
                    ? ResponseStatusFactory.Success
                    : ResponseStatusFactory.Error(ErrorCode.Unknown, result.ErrorMessage ?? "")
            });
        }

        return Task.FromResult(response);
    }

    public override Task<ListReassignmentsResponse> ListPartitionReassignments(
        ListReassignmentsRequest request,
        ServerCallContext context)
    {
        var reassignments = _listReassignments();

        var response = new ListReassignmentsResponse
        {
            Status = ResponseStatusFactory.Success
        };

        foreach (var r in reassignments)
        {
            var ongoing = new OngoingReassignment
            {
                Topic = r.Topic,
                Partition = r.Partition
            };
            ongoing.Replicas.AddRange(r.Replicas);
            ongoing.AddingReplicas.AddRange(r.AddingReplicas);
            ongoing.RemovingReplicas.AddRange(r.RemovingReplicas);
            response.Reassignments.Add(ongoing);
        }

        return Task.FromResult(response);
    }

    public override async Task<TriggerCompactionResponse> TriggerLogCompaction(
        TriggerCompactionRequest request,
        ServerCallContext context)
    {
        var partitions = request.Partitions.Count > 0
            ? request.Partitions.Select(p => (p.Topic, p.Partition)).ToList()
            : null;

        var result = await _triggerCompaction(partitions);

        return new TriggerCompactionResponse
        {
            Status = result.Success ? ResponseStatusFactory.Success : ResponseStatusFactory.Error(ErrorCode.Unknown, "")
        };
    }

    public override Task<GetCompactionStatusResponse> GetCompactionStatus(
        GetCompactionStatusRequest request,
        ServerCallContext context)
    {
        var partitions = request.Partitions.Count > 0
            ? request.Partitions.Select(p => (p.Topic, p.Partition)).ToList()
            : null;

        var statuses = _getCompactionStatus(partitions);

        var response = new GetCompactionStatusResponse
        {
            Status = ResponseStatusFactory.Success
        };

        foreach (var status in statuses)
        {
            response.Statuses.Add(new CompactionStatus
            {
                Topic = status.Topic,
                Partition = status.Partition,
                CompactionEnabled = status.CompactionEnabled,
                LastCompactionTime = status.LastCompactionTime,
                CompactionRatio = status.CompactionRatio
            });
        }

        return Task.FromResult(response);
    }

    public override async Task WatchHealth(
        WatchHealthRequest request,
        IServerStreamWriter<HealthUpdate> responseStream,
        ServerCallContext context)
    {
        var interval = Math.Max(request.IntervalSeconds, 1);
        var ct = context.CancellationToken;

        while (!ct.IsCancellationRequested)
        {
            var info = _getClusterInfo();
            var metadata = _getMetadata(null);
            var totalPartitions = metadata.Sum(t => (long)t.PartitionCount);

            await responseStream.WriteAsync(new HealthUpdate
            {
                ClusterId = $"surgewave-{info.BrokerId}",
                BrokerCount = _listBrokers().Count,
                TopicCount = info.TopicCount,
                TotalPartitions = totalPartitions,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            }, ct);

            await Task.Delay(TimeSpan.FromSeconds(interval), ct);
        }
    }
}
