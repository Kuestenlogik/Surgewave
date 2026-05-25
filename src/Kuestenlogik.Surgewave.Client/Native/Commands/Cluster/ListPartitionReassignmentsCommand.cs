using Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Cluster;

/// <summary>
/// Command to list active partition reassignments.
/// </summary>
public sealed class ListPartitionReassignmentsCommand : NoRequestCommand<List<PartitionReassignmentStatus>>
{
    public override SurgewaveOpCode OpCode => SurgewaveOpCode.ListPartitionReassignments;

    public override List<PartitionReassignmentStatus> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var wirePayload = ListReassignmentsPayload.Read(ref reader);
        var result = new List<PartitionReassignmentStatus>(wirePayload.Reassignments.Count);

        foreach (var r in wirePayload.Reassignments)
        {
            result.Add(new PartitionReassignmentStatus(
                r.Topic,
                r.Partition,
                (ReassignmentStatusCode)r.Status,
                r.ProgressPercent,
                r.OriginalReplicas.ToList(),
                r.TargetReplicas.ToList()));
        }

        return result;
    }
}
