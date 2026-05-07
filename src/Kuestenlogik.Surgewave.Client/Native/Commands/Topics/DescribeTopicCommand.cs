using Kuestenlogik.Surgewave.Client.Native.Operations.Topics;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Topics;

/// <summary>
/// Command to describe a topic with full partition metadata.
/// </summary>
public sealed class DescribeTopicCommand : ISurgewaveCommand<TopicDescription>
{
    private readonly DescribeTopicRequestPayload _request;

    public DescribeTopicCommand(string topicName)
    {
        _request = new DescribeTopicRequestPayload { TopicName = topicName };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DescribeTopic;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        _request.Write(ref writer);
    }

    public int EstimateRequestSize() => _request.EstimateSize();

    public TopicDescription ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = DescribeTopicResponsePayload.Read(ref reader);

        var partitions = response.Partitions.Select(p => new PartitionDescription
        {
            PartitionId = p.PartitionId,
            Leader = p.Leader,
            LeaderEpoch = p.LeaderEpoch,
            Replicas = p.Replicas,
            Isr = p.Isr,
            HighWatermark = p.HighWatermark,
            LogStartOffset = p.LogStartOffset
        }).ToArray();

        return new TopicDescription
        {
            Name = response.TopicName,
            PartitionCount = response.PartitionCount,
            ReplicationFactor = response.ReplicationFactor,
            IsInternal = response.IsInternal,
            Partitions = partitions
        };
    }
}
