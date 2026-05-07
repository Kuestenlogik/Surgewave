using Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

/// <summary>
/// Command to describe a consumer group.
/// </summary>
public sealed class DescribeGroupCommand : ISurgewaveCommand<ConsumerGroupDescription>
{
    private readonly DescribeGroupRequestPayload _request;

    public DescribeGroupCommand(string groupId)
    {
        _request = new DescribeGroupRequestPayload { GroupId = groupId };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DescribeGroup;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public ConsumerGroupDescription ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = DescribeGroupResponsePayload.Read(ref reader);

        var members = new List<GroupMemberDescription>(response.Members.Length);
        foreach (var member in response.Members)
        {
            members.Add(new GroupMemberDescription(
                member.MemberId,
                member.GroupInstanceId,
                member.ClientId,
                member.Metadata,
                member.Assignment));
        }

        return new ConsumerGroupDescription(
            response.GroupId,
            response.State,
            response.ProtocolType,
            response.ProtocolName,
            response.GenerationId,
            members,
            response.ErrorCode);
    }
}
