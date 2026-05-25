namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

public record JoinGroupMemberInfo
{
    public string MemberId { get; init; } = "";
    public string? GroupInstanceId { get; init; }
    public byte[] Metadata { get; init; } = Array.Empty<byte>();
}
