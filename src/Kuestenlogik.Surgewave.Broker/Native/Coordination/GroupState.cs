namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

public enum GroupState
{
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead
}
