using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

/// <summary>
/// Command to send a heartbeat to keep group membership alive.
/// </summary>
public sealed class HeartbeatCommand : ISurgewaveCommand<ushort>
{
    private readonly HeartbeatRequestPayload _request;

    public HeartbeatCommand(string groupId, string memberId, int generationId)
    {
        _request = new HeartbeatRequestPayload
        {
            GroupId = groupId,
            MemberId = memberId,
            GenerationId = generationId
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.Heartbeat;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public ushort ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = HeartbeatResponsePayload.Read(ref reader);
        return response.ErrorCode;
    }
}
