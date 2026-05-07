using Kuestenlogik.Surgewave.Broker.Native.Handlers;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

namespace Kuestenlogik.Surgewave.Broker.Native.Operations.Cluster;

/// <summary>
/// Result for cluster info operation.
/// </summary>
public readonly record struct ClusterInfoResult
{
    public required ClusterInfoPayload Payload { get; init; }
}

/// <summary>
/// Operation to get cluster information.
/// </summary>
public sealed class GetClusterInfoOperation : INoRequestOperationHandler<ClusterInfoResult>
{
    private readonly LogManager _logManager;
    private readonly NativeRequestContext _context;

    public GetClusterInfoOperation(LogManager logManager, NativeRequestContext context)
    {
        _logManager = logManager;
        _context = context;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetClusterInfo;

    public Task<ClusterInfoResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        var topics = _logManager.ListTopics().ToList();
        var totalPartitions = topics.Sum(t => t.PartitionCount);

        // In single-broker mode, this broker is always the controller
        // For multi-broker clusters, this should be updated from ClusterState
        var isController = true;
        var controllerId = _context.Config.BrokerId;

        var payload = new ClusterInfoPayload
        {
            BrokerId = _context.Config.BrokerId,
            Host = _context.Config.Host,
            Port = _context.Config.Port,
            IsController = isController,
            ControllerId = controllerId,
            ControllerEpoch = 1,
            UseRaftConsensus = _context.Config.UseRaftConsensus,
            IsRaftLeader = isController && _context.Config.UseRaftConsensus,
            RaftTerm = _context.Config.UseRaftConsensus ? 1 : 0,
            TopicCount = topics.Count,
            TotalPartitions = totalPartitions
        };

        return Task.FromResult(new ClusterInfoResult { Payload = payload });
    }

    public void WriteResponse(IPayloadWriter writer, in ClusterInfoResult response)
        => response.Payload.WriteTo(writer);
}

/// <summary>
/// Result for list brokers operation.
/// </summary>
public readonly record struct ListBrokersResult
{
    public required ListBrokersPayload Payload { get; init; }
}

/// <summary>
/// Operation to list brokers.
/// </summary>
public sealed class ListBrokersOperation : INoRequestOperationHandler<ListBrokersResult>
{
    private readonly NativeRequestContext _context;

    public ListBrokersOperation(NativeRequestContext context) => _context = context;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListBrokers;

    public Task<ListBrokersResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        var payload = new ListBrokersPayload
        {
            Brokers = new[]
            {
                new BrokerInfoPayload
                {
                    BrokerId = _context.Config.BrokerId,
                    Host = _context.Config.Host,
                    Port = _context.Config.Port,
                    ReplicationPort = _context.Config.ReplicationPort,
                    IsController = true,
                    IsAlive = true,
                    Rack = null
                }
            }
        };

        return Task.FromResult(new ListBrokersResult { Payload = payload });
    }

    public void WriteResponse(IPayloadWriter writer, in ListBrokersResult response)
        => response.Payload.WriteTo(writer);
}

/// <summary>
/// Request for alter partition reassignments.
/// </summary>
public readonly record struct AlterPartitionReassignmentsRequest
{
    public required ReassignmentPlan Plan { get; init; }
}

/// <summary>
/// Result for alter partition reassignments.
/// </summary>
public readonly record struct AlterPartitionReassignmentsResult
{
    public required bool Success { get; init; }
    public required int PartitionCount { get; init; }
}

/// <summary>
/// Operation to alter partition reassignments.
/// </summary>
public sealed class AlterPartitionReassignmentsOperation
    : IOperationHandler<AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResult>
{
    private readonly PartitionReassignmentManager? _reassignmentManager;

    public AlterPartitionReassignmentsOperation(PartitionReassignmentManager? reassignmentManager)
        => _reassignmentManager = reassignmentManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.AlterPartitionReassignments;

    public AlterPartitionReassignmentsRequest ParseRequest(ref SurgewavePayloadReader reader)
    {
        var partitionCount = reader.ReadInt32();
        var plan = new ReassignmentPlan { Version = 1, Partitions = [] };

        for (int i = 0; i < partitionCount; i++)
        {
            var topic = reader.ReadString() ?? string.Empty;
            var partition = reader.ReadInt32();
            var replicaCount = reader.ReadInt32();
            var replicas = new List<int>(replicaCount);

            for (int j = 0; j < replicaCount; j++)
                replicas.Add(reader.ReadInt32());

            plan.Partitions.Add(new PartitionReassignment
            {
                Topic = topic,
                Partition = partition,
                Replicas = replicas
            });
        }

        return new AlterPartitionReassignmentsRequest { Plan = plan };
    }

    public void ValidateRequest(in AlterPartitionReassignmentsRequest request) { }

    public async Task<AlterPartitionReassignmentsResult> ExecuteAsync(
        AlterPartitionReassignmentsRequest request,
        CancellationToken cancellationToken)
    {
        var success = _reassignmentManager != null
            && await _reassignmentManager.ExecuteReassignmentAsync(request.Plan, cancellationToken);

        return new AlterPartitionReassignmentsResult
        {
            Success = success,
            PartitionCount = request.Plan.Partitions.Count
        };
    }

    public void WriteResponse(IPayloadWriter writer, in AlterPartitionReassignmentsResult response)
    {
        writer.WriteUInt8(response.Success ? (byte)1 : (byte)0);
        writer.WriteInt32(response.PartitionCount);
    }
}

/// <summary>
/// Result for list partition reassignments.
/// </summary>
public readonly record struct ListPartitionReassignmentsResult
{
    public required IReadOnlyList<PartitionReassignmentState> Reassignments { get; init; }
}

/// <summary>
/// Operation to list partition reassignments.
/// </summary>
public sealed class ListPartitionReassignmentsOperation : INoRequestOperationHandler<ListPartitionReassignmentsResult>
{
    private readonly PartitionReassignmentManager? _reassignmentManager;

    public ListPartitionReassignmentsOperation(PartitionReassignmentManager? reassignmentManager)
        => _reassignmentManager = reassignmentManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListPartitionReassignments;

    public Task<ListPartitionReassignmentsResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        var reassignments = _reassignmentManager?.GetActiveReassignments() ?? [];
        return Task.FromResult(new ListPartitionReassignmentsResult { Reassignments = reassignments });
    }

    public void WriteResponse(IPayloadWriter writer, in ListPartitionReassignmentsResult response)
    {
        writer.WriteInt32(response.Reassignments.Count);

        foreach (var r in response.Reassignments)
        {
            writer.WriteString(r.Topic);
            writer.WriteInt32(r.Partition);
            writer.WriteUInt8((byte)r.Status);
            writer.WriteInt32(r.ProgressPercent);

            writer.WriteInt32(r.OriginalReplicas.Count);
            foreach (var replica in r.OriginalReplicas)
                writer.WriteInt32(replica);

            writer.WriteInt32(r.TargetReplicas.Count);
            foreach (var replica in r.TargetReplicas)
                writer.WriteInt32(replica);
        }
    }
}

/// <summary>
/// Request for trigger log compaction.
/// </summary>
public readonly record struct TriggerLogCompactionRequest
{
    public required int TopicCount { get; init; }
}

/// <summary>
/// Result for trigger log compaction.
/// </summary>
public readonly record struct TriggerLogCompactionResult
{
    public required bool Success { get; init; }
    public required int RecordsRemoved { get; init; }
    public required long BytesRemoved { get; init; }
    public required int SegmentsCompacted { get; init; }
}

/// <summary>
/// Operation to trigger log compaction.
/// </summary>
public sealed class TriggerLogCompactionOperation
    : IOperationHandler<TriggerLogCompactionRequest, TriggerLogCompactionResult>
{
    private readonly LogManager _logManager;

    public TriggerLogCompactionOperation(LogManager logManager) => _logManager = logManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.TriggerLogCompaction;

    public TriggerLogCompactionRequest ParseRequest(ref SurgewavePayloadReader reader)
        => new TriggerLogCompactionRequest { TopicCount = reader.ReadInt32() };

    public void ValidateRequest(in TriggerLogCompactionRequest request) { }

    public async Task<TriggerLogCompactionResult> ExecuteAsync(
        TriggerLogCompactionRequest request,
        CancellationToken cancellationToken)
    {
        var result = await _logManager.ApplyCompactionAsync(cancellationToken);
        return new TriggerLogCompactionResult
        {
            Success = true,
            RecordsRemoved = (int)result.RecordsRemoved,
            BytesRemoved = result.BytesRemoved,
            SegmentsCompacted = result.SegmentsCompacted
        };
    }

    public void WriteResponse(IPayloadWriter writer, in TriggerLogCompactionResult response)
    {
        writer.WriteUInt8(response.Success ? (byte)1 : (byte)0);
        writer.WriteInt32(response.RecordsRemoved);
        writer.WriteInt64(response.BytesRemoved);
        writer.WriteInt32(response.SegmentsCompacted);
    }
}

/// <summary>
/// Compaction status for a topic.
/// </summary>
public readonly record struct TopicCompactionStatus
{
    public required string Name { get; init; }
    public required int PartitionCount { get; init; }
    public required string CleanupPolicy { get; init; }
    public required int SegmentCount { get; init; }
    public required long TotalBytes { get; init; }
}

/// <summary>
/// Result for get compaction status.
/// </summary>
public readonly record struct GetCompactionStatusResult
{
    public required IReadOnlyList<TopicCompactionStatus> Topics { get; init; }
}

/// <summary>
/// Operation to get compaction status.
/// </summary>
public sealed class GetCompactionStatusOperation : INoRequestOperationHandler<GetCompactionStatusResult>
{
    private readonly LogManager _logManager;

    public GetCompactionStatusOperation(LogManager logManager) => _logManager = logManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetCompactionStatus;

    public Task<GetCompactionStatusResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        var topics = _logManager.ListTopics().ToList();
        var compactableTopics = topics
            .Where(t => t.CleanupPolicy.HasFlag(CleanupPolicy.Compact))
            .ToList();

        var result = new List<TopicCompactionStatus>();

        foreach (var topic in compactableTopics)
        {
            var segmentCount = 0;
            var totalBytes = 0L;

            for (int p = 0; p < topic.PartitionCount; p++)
            {
                var log = _logManager.GetLog(new TopicPartition { Topic = topic.Name, Partition = p });
                if (log is PartitionLog pl)
                {
                    segmentCount += pl.Segments.Count;
                    totalBytes += pl.Segments.Sum(s => s.Size);
                }
                else if (log != null)
                {
                    totalBytes += log.TotalSize;
                }
            }

            result.Add(new TopicCompactionStatus
            {
                Name = topic.Name,
                PartitionCount = topic.PartitionCount,
                CleanupPolicy = topic.CleanupPolicy.ToString(),
                SegmentCount = segmentCount,
                TotalBytes = totalBytes
            });
        }

        return Task.FromResult(new GetCompactionStatusResult { Topics = result });
    }

    public void WriteResponse(IPayloadWriter writer, in GetCompactionStatusResult response)
    {
        writer.WriteInt32(response.Topics.Count);

        foreach (var topic in response.Topics)
        {
            writer.WriteString(topic.Name);
            writer.WriteInt32(topic.PartitionCount);
            writer.WriteString(topic.CleanupPolicy);
            writer.WriteInt32(topic.SegmentCount);
            writer.WriteInt64(topic.TotalBytes);
        }
    }
}
