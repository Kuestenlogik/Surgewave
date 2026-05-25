namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

public record GroupInfo
{
    public string GroupId { get; init; } = "";
    public string ProtocolType { get; init; } = "";
    public string State { get; init; } = "";
}
