using Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

/// <summary>
/// Command to get lag for a consumer group.
/// </summary>
public sealed class GetGroupLagCommand : ISurgewaveCommand<ConsumerGroupLag>
{
    private readonly GetGroupLagRequestPayload _request;

    public GetGroupLagCommand(string groupId)
    {
        _request = new GetGroupLagRequestPayload { GroupId = groupId };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetGroupLag;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public ConsumerGroupLag ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = GetGroupLagResponsePayload.Read(ref reader);

        if (response.ErrorCode != 0)
            throw new InvalidOperationException($"GetGroupLag failed with error code: {response.ErrorCode}");

        var topics = new List<TopicLag>(response.Topics.Length);
        foreach (var topic in response.Topics)
        {
            var partitions = new List<PartitionLag>(topic.Partitions.Length);
            foreach (var partition in topic.Partitions)
            {
                partitions.Add(new PartitionLag(
                    partition.Partition,
                    partition.CommittedOffset,
                    partition.HighWatermark,
                    partition.Lag,
                    partition.LogStartOffset));
            }

            topics.Add(new TopicLag(topic.Topic, topic.TotalLag, partitions));
        }

        return new ConsumerGroupLag(
            response.GroupId,
            response.State,
            response.TotalLag,
            response.PartitionCount,
            response.MemberCount,
            topics);
    }
}
