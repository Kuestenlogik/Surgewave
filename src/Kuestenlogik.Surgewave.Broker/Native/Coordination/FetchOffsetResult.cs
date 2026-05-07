namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

public record FetchOffsetResult
{
    public ushort ErrorCode { get; init; }
    public long Offset { get; init; }
}
