using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Topics;

/// <summary>
/// Command to delete a topic.
/// </summary>
public sealed class DeleteTopicCommand : VoidCommand
{
    private readonly DeleteTopicRequestPayload _payload;

    public DeleteTopicCommand(string name)
    {
        _payload = new DeleteTopicRequestPayload { Name = name };
    }

    public override SurgewaveOpCode OpCode => SurgewaveOpCode.DeleteTopic;

    public override void WriteRequest(ref SurgewavePayloadWriter writer) => _payload.Write(ref writer);

    public override int EstimateRequestSize() => _payload.EstimateSize();
}
