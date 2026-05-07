namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

public record GroupMemberInfo
{
    public string MemberId { get; init; } = "";
    public string? GroupInstanceId { get; init; }
    public string ClientId { get; init; } = "";
    public byte[] Metadata { get; init; } = Array.Empty<byte>();
    public byte[] Assignment { get; init; } = Array.Empty<byte>();
}
