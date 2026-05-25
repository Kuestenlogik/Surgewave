using Kuestenlogik.Surgewave.Broker.Native.Coordination;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;
using ConsumerGroupOps = Kuestenlogik.Surgewave.Broker.Native.Operations.ConsumerGroups;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol consumer group operations.
/// </summary>
public sealed class NativeConsumerGroupHandler : NativeHandlerBase
{
    public NativeConsumerGroupHandler(NativeGroupCoordinator groupCoordinator)
    {
        Register<JoinGroupRequestPayload, ConsumerGroupOps.JoinGroupResult>(
            SurgewaveOpCode.JoinGroup, _ => new ConsumerGroupOps.JoinGroupOperation(groupCoordinator));
        Register<SyncGroupRequestPayload, ConsumerGroupOps.SyncGroupResult>(
            SurgewaveOpCode.SyncGroup, _ => new ConsumerGroupOps.SyncGroupOperation(groupCoordinator));
        Register<HeartbeatRequestPayload, ConsumerGroupOps.HeartbeatResult>(
            SurgewaveOpCode.Heartbeat, _ => new ConsumerGroupOps.HeartbeatOperation(groupCoordinator));
        Register<LeaveGroupRequestPayload, ConsumerGroupOps.LeaveGroupResult>(
            SurgewaveOpCode.LeaveGroup, _ => new ConsumerGroupOps.LeaveGroupOperation(groupCoordinator));
        RegisterNoRequest<ConsumerGroupOps.ListGroupsResult>(
            SurgewaveOpCode.ListGroups, _ => new ConsumerGroupOps.ListGroupsOperation(groupCoordinator));
        Register<DescribeGroupRequestPayload, ConsumerGroupOps.DescribeGroupResult>(
            SurgewaveOpCode.DescribeGroup, _ => new ConsumerGroupOps.DescribeGroupOperation(groupCoordinator));
        Register<DeleteGroupRequestPayload, ConsumerGroupOps.DeleteGroupResult>(
            SurgewaveOpCode.DeleteGroup, _ => new ConsumerGroupOps.DeleteGroupOperation(groupCoordinator));
        Register<FindCoordinatorRequestPayload, ConsumerGroupOps.FindCoordinatorResult>(
            SurgewaveOpCode.FindCoordinator, ctx => new ConsumerGroupOps.FindCoordinatorOperation(groupCoordinator, ctx));
    }
}
