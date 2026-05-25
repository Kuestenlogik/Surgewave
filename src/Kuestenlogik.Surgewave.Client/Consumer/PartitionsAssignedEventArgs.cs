namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Event arguments for partitions assigned event.
/// </summary>
public sealed class PartitionsAssignedEventArgs : EventArgs
{
    /// <summary>
    /// The partitions that were assigned.
    /// </summary>
    public IReadOnlyList<(string Topic, int Partition)> Partitions { get; }

    /// <summary>
    /// Creates a new PartitionsAssignedEventArgs.
    /// </summary>
    public PartitionsAssignedEventArgs(IReadOnlyList<(string Topic, int Partition)> partitions)
    {
        Partitions = partitions;
    }
}
