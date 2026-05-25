using Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

/// <summary>
/// Command to list all consumer groups.
/// </summary>
public sealed class ListGroupsCommand : NoRequestCommand<List<ConsumerGroupInfo>>
{
    public override SurgewaveOpCode OpCode => SurgewaveOpCode.ListGroups;

    public override List<ConsumerGroupInfo> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = ListGroupsResponsePayload.Read(ref reader);

        if (response.ErrorCode != 0)
            throw new InvalidOperationException($"ListGroups failed with error code: {response.ErrorCode}");

        var groups = new List<ConsumerGroupInfo>(response.Groups.Length);
        foreach (var group in response.Groups)
        {
            groups.Add(new ConsumerGroupInfo(group.GroupId, group.ProtocolType, group.State));
        }

        return groups;
    }
}
