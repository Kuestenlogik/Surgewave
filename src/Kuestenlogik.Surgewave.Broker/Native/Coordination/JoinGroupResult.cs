namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

public record JoinGroupResult
{
    public ushort ErrorCode { get; init; }
    public int GenerationId { get; init; }
    public string ProtocolName { get; init; } = "";
    public string LeaderId { get; init; } = "";
    public string MemberId { get; init; } = "";
    public List<JoinGroupMemberInfo> Members { get; init; } = new();
}
