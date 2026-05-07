using Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Cluster;

/// <summary>
/// Command to trigger log compaction on all compactable topics.
/// </summary>
public sealed class TriggerLogCompactionCommand : ISurgewaveCommand<CompactionResultInfo>
{
    public SurgewaveOpCode OpCode => SurgewaveOpCode.TriggerLogCompaction;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(0); // 0 = all compactable topics
    }

    public int EstimateRequestSize() => 4;

    public CompactionResultInfo ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var wirePayload = CompactionResultPayload.Read(ref reader);
        return new CompactionResultInfo(
            wirePayload.Success,
            wirePayload.RecordsRemoved,
            wirePayload.BytesRemoved,
            wirePayload.SegmentsCompacted);
    }
}
