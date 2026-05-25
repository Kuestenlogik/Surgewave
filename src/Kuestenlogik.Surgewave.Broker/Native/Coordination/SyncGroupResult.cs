namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

public record SyncGroupResult
{
    public ushort ErrorCode { get; init; }
    public byte[] Assignment { get; init; } = Array.Empty<byte>();
}
