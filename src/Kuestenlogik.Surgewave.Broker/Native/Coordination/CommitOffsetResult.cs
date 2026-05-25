namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

public record CommitOffsetResult
{
    public ushort ErrorCode { get; init; }
}
