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
    public NativeConsumerGroupHandler(
        NativeGroupCoordinator groupCoordinator,
        Kuestenlogik.Surgewave.Core.Monitoring.ILagCalculator? lagCalculator = null)
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

        // Lag-Ops nur bei vorhandenem Calculator registrieren — Hosts ohne
        // Lag-Infrastruktur (Tests, Embedded) antworten dann mit UnknownOpCode
        // statt mit leeren Fake-Daten.
        if (lagCalculator is not null)
        {
            Register<GetGroupLagRequestPayload, ConsumerGroupOps.GetGroupLagResult>(
                SurgewaveOpCode.GetGroupLag, _ => new ConsumerGroupOps.GetGroupLagOperation(lagCalculator));
            RegisterNoRequest<ConsumerGroupOps.GetLagSummaryResult>(
                SurgewaveOpCode.GetLagSummary, _ => new ConsumerGroupOps.GetLagSummaryOperation(lagCalculator));
        }
    }
}
