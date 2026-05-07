using Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Cluster;

/// <summary>
/// Command to execute a partition reassignment plan.
/// </summary>
public sealed class AlterPartitionReassignmentsCommand : ISurgewaveCommand<ReassignmentResult>
{
    private readonly AlterReassignmentsRequestPayload _request;

    public AlterPartitionReassignmentsCommand(List<PartitionReassignmentRequest> reassignments)
    {
        var wireReassignments = reassignments.Select(r => new PartitionReassignmentRequestPayload
        {
            Topic = r.Topic,
            Partition = r.Partition,
            Replicas = r.Replicas
        }).ToArray();

        _request = new AlterReassignmentsRequestPayload { Reassignments = wireReassignments };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.AlterPartitionReassignments;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public ReassignmentResult ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var wireResponse = AlterReassignmentsResponsePayload.Read(ref reader);
        return new ReassignmentResult(wireResponse.Success, wireResponse.PartitionCount, header.ErrorCode);
    }
}
