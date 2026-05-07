using Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Cluster;

/// <summary>
/// Command to get compaction status for all compactable topics.
/// </summary>
public sealed class GetCompactionStatusCommand : NoRequestCommand<List<TopicCompactionStatus>>
{
    public override SurgewaveOpCode OpCode => SurgewaveOpCode.GetCompactionStatus;

    public override List<TopicCompactionStatus> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var wirePayload = CompactionStatusPayload.Read(ref reader);
        var result = new List<TopicCompactionStatus>(wirePayload.Topics.Count);

        foreach (var t in wirePayload.Topics)
        {
            result.Add(new TopicCompactionStatus(
                t.Topic,
                t.PartitionCount,
                t.CleanupPolicy,
                t.SegmentCount,
                t.TotalBytes));
        }

        return result;
    }
}
