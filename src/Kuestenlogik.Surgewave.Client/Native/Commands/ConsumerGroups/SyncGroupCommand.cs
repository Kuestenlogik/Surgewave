using Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

/// <summary>
/// Command to sync a consumer group (called by leader to distribute assignments).
/// </summary>
public sealed class SyncGroupCommand : ISurgewaveCommand<SyncGroupResponse>
{
    private readonly SyncGroupRequestPayload _request;

    public SyncGroupCommand(
        string groupId,
        string memberId,
        int generationId,
        List<(string MemberId, byte[] Assignment)> assignments)
    {
        var assignmentPayloads = new MemberAssignmentPayload[assignments.Count];
        for (int i = 0; i < assignments.Count; i++)
        {
            assignmentPayloads[i] = new MemberAssignmentPayload
            {
                MemberId = assignments[i].MemberId,
                Assignment = assignments[i].Assignment
            };
        }

        _request = new SyncGroupRequestPayload
        {
            GroupId = groupId,
            MemberId = memberId,
            GenerationId = generationId,
            Assignments = assignmentPayloads
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.SyncGroup;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public SyncGroupResponse ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = SyncGroupResponsePayload.Read(ref reader);
        return new SyncGroupResponse(response.ErrorCode, response.Assignment);
    }
}
