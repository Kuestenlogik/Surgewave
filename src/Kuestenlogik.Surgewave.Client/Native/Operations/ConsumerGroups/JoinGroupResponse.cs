namespace Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;

/// <summary>
/// Response from joining a consumer group.
/// </summary>
public record JoinGroupResponse(
    ushort ErrorCode,
    int GenerationId,
    string ProtocolName,
    string LeaderId,
    string MemberId,
    List<JoinGroupMember> Members);
