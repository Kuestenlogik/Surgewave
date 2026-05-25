using Kuestenlogik.Surgewave.Client.Native.Operations.Topics;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Topics;

/// <summary>
/// Command to list all topics.
/// </summary>
public sealed class ListTopicsCommand : NoRequestCommand<List<TopicInfo>>
{
    public override SurgewaveOpCode OpCode => SurgewaveOpCode.ListTopics;

    public override List<TopicInfo> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = ListTopicsResponsePayload.Read(ref reader);
        return response.Topics
            .Select(t => new TopicInfo(t.Name, t.PartitionCount))
            .ToList();
    }
}
