using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

/// <summary>
/// Command to delete a consumer group.
/// </summary>
public sealed class DeleteGroupCommand : ISurgewaveVoidCommand
{
    private readonly DeleteGroupRequestPayload _request;

    public DeleteGroupCommand(string groupId)
    {
        _request = new DeleteGroupRequestPayload { GroupId = groupId };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DeleteGroup;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public Unit ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        ValidateResponse(ref reader, header);
        return Unit.Value;
    }

    public void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = DeleteGroupResponsePayload.Read(ref reader);
        if (response.ErrorCode != 0)
            throw new InvalidOperationException($"DeleteGroup failed with error code: {response.ErrorCode}");
    }
}
