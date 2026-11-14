using Kuestenlogik.Surgewave.Broker.Native.Operations.Cluster;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol cluster operations: GetClusterInfo, ListBrokers, Reassignments, Compaction.
/// </summary>
public sealed class NativeClusterHandler : NativeHandlerBase
{
    /// <param name="cluster">The view of the cluster as it is when a request arrives; null on a broker that runs alone.</param>
    public NativeClusterHandler(
        LogManager logManager, PartitionReassignmentManager? reassignmentManager = null, Func<ClusterView?>? cluster = null)
    {
        RegisterNoRequest<ClusterInfoResult>(
            SurgewaveOpCode.GetClusterInfo, ctx => new GetClusterInfoOperation(logManager, ctx, cluster?.Invoke()));
        RegisterNoRequest<ListBrokersResult>(SurgewaveOpCode.ListBrokers, ctx => new ListBrokersOperation(ctx, cluster?.Invoke()));
        Register<AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResult>(
            SurgewaveOpCode.AlterPartitionReassignments, _ => new AlterPartitionReassignmentsOperation(reassignmentManager));
        RegisterNoRequest<ListPartitionReassignmentsResult>(
            SurgewaveOpCode.ListPartitionReassignments, _ => new ListPartitionReassignmentsOperation(reassignmentManager));
        Register<TriggerLogCompactionRequest, TriggerLogCompactionResult>(
            SurgewaveOpCode.TriggerLogCompaction, _ => new TriggerLogCompactionOperation(logManager));
        RegisterNoRequest<GetCompactionStatusResult>(SurgewaveOpCode.GetCompactionStatus, _ => new GetCompactionStatusOperation(logManager));
    }
}
