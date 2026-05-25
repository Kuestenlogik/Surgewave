using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Topics;

/// <summary>
/// Command to create a new topic.
/// </summary>
public sealed class CreateTopicCommand : VoidCommand
{
    private readonly CreateTopicRequestPayload _payload;

    public CreateTopicCommand(string name, int partitions, short replicationFactor = 1, TopicConfigPayload[]? configs = null)
    {
        _payload = new CreateTopicRequestPayload
        {
            Name = name,
            Partitions = partitions,
            ReplicationFactor = replicationFactor,
            Configs = configs
        };
    }

    public override SurgewaveOpCode OpCode => SurgewaveOpCode.CreateTopic;

    public override void WriteRequest(ref SurgewavePayloadWriter writer) => _payload.Write(ref writer);

    public override int EstimateRequestSize() => _payload.EstimateSize();
}
