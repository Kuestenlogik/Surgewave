using Kuestenlogik.Surgewave.Core;

namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

public record FindCoordinatorResult
{
    public ushort ErrorCode { get; init; }
    public int CoordinatorId { get; init; }
    public string Host { get; init; } = "";
    public int Port { get; init; }
}
