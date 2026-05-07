using Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

/// <summary>
/// Command to join a consumer group.
/// </summary>
public sealed class JoinGroupCommand : ISurgewaveCommand<JoinGroupResponse>
{
    private readonly JoinGroupRequestPayload _request;

    public JoinGroupCommand(
        string groupId,
        string? memberId,
        string clientId,
        string protocolType,
        int sessionTimeoutMs,
        int rebalanceTimeoutMs,
        List<(string Name, byte[] Metadata)> protocols)
    {
        var protocolPayloads = new GroupProtocol[protocols.Count];
        for (int i = 0; i < protocols.Count; i++)
        {
            protocolPayloads[i] = new GroupProtocol(protocols[i].Name, protocols[i].Metadata);
        }

        _request = new JoinGroupRequestPayload
        {
            GroupId = groupId,
            MemberId = memberId,
            GroupInstanceId = null,
            ClientId = clientId,
            ProtocolType = protocolType,
            SessionTimeoutMs = sessionTimeoutMs,
            RebalanceTimeoutMs = rebalanceTimeoutMs,
            Protocols = protocolPayloads
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.JoinGroup;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public JoinGroupResponse ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = JoinGroupResponsePayload.Read(ref reader);

        var members = new List<JoinGroupMember>(response.Members.Length);
        foreach (var member in response.Members)
        {
            members.Add(new JoinGroupMember(member.MemberId, member.GroupInstanceId, member.Metadata));
        }

        return new JoinGroupResponse(
            response.ErrorCode,
            response.GenerationId,
            response.ProtocolName,
            response.LeaderId,
            response.MemberId,
            members);
    }
}
