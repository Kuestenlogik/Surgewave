using Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Cluster;

/// <summary>
/// Command to get cluster information.
/// </summary>
public sealed class GetClusterInfoCommand : NoRequestCommand<ClusterInfo>
{
    public override SurgewaveOpCode OpCode => SurgewaveOpCode.GetClusterInfo;

    public override ClusterInfo ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var wirePayload = ClusterInfoPayload.Read(ref reader);
        return new ClusterInfo
        {
            BrokerId = wirePayload.BrokerId,
            Host = wirePayload.Host,
            Port = wirePayload.Port,
            IsController = wirePayload.IsController,
            ControllerId = wirePayload.ControllerId,
            ControllerEpoch = wirePayload.ControllerEpoch,
            UseRaftConsensus = wirePayload.UseRaftConsensus,
            IsRaftLeader = wirePayload.IsRaftLeader,
            RaftTerm = wirePayload.RaftTerm,
            TopicCount = wirePayload.TopicCount,
            TotalPartitions = wirePayload.TotalPartitions
        };
    }
}
