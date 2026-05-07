namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

public record DescribeGroupResult
{
    public ushort ErrorCode { get; init; }
    public string GroupId { get; init; } = "";
    public string State { get; init; } = "";
    public string ProtocolType { get; init; } = "";
    public string ProtocolName { get; init; } = "";
    public int GenerationId { get; init; }
    public List<GroupMemberInfo> Members { get; init; } = new();
}
