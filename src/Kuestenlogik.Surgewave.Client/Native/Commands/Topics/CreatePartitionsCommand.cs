using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Topics;

/// <summary>
/// Command to add partitions to an existing topic.
/// </summary>
public sealed class CreatePartitionsCommand : VoidCommand
{
    private readonly CreatePartitionsRequestPayload _payload;

    public CreatePartitionsCommand(string topicName, int totalPartitions)
    {
        _payload = new CreatePartitionsRequestPayload
        {
            TopicName = topicName,
            TotalPartitions = totalPartitions
        };
    }

    public override SurgewaveOpCode OpCode => SurgewaveOpCode.CreatePartitions;

    public override void WriteRequest(ref SurgewavePayloadWriter writer) => _payload.Write(ref writer);

    public override int EstimateRequestSize() => _payload.EstimateSize();
}
