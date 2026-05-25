namespace Kuestenlogik.Surgewave.Broker.Native.Assignors;

/// <summary>
/// Topic and partition identifier for assignment
/// </summary>
public readonly record struct AssignedPartition(string Topic, int Partition);
