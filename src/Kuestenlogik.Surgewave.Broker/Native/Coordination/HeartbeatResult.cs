namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

public record HeartbeatResult
{
    public ushort ErrorCode { get; init; }
}
