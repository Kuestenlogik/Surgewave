using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

/// <summary>
/// Command to leave a consumer group.
/// </summary>
public sealed class LeaveGroupCommand : ISurgewaveVoidCommand
{
    private readonly LeaveGroupRequestPayload _request;

    public LeaveGroupCommand(string groupId, string memberId)
    {
        _request = new LeaveGroupRequestPayload
        {
            GroupId = groupId,
            MemberId = memberId
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.LeaveGroup;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public Unit ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        ValidateResponse(ref reader, header);
        return Unit.Value;
    }

    public void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = LeaveGroupResponsePayload.Read(ref reader);
        if (response.ErrorCode != 0)
            throw new InvalidOperationException($"LeaveGroup failed with error code: {response.ErrorCode}");
    }
}
