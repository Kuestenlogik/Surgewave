using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Topics;

/// <summary>
/// Command to delete records up to a specified offset.
/// </summary>
public sealed class DeleteRecordsCommand : ISurgewaveCommand<long>
{
    private readonly DeleteRecordsRequestPayload _payload;

    public DeleteRecordsCommand(string topicName, int partition, long beforeOffset)
    {
        _payload = new DeleteRecordsRequestPayload
        {
            TopicName = topicName,
            Partition = partition,
            BeforeOffset = beforeOffset
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DeleteRecords;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _payload.Write(ref writer);

    public int EstimateRequestSize() => _payload.EstimateSize();

    public long ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = DeleteRecordsResponsePayload.Read(ref reader);
        return response.LowWatermark;
    }
}
