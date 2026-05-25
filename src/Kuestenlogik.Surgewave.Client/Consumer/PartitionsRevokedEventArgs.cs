namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Event arguments for partitions revoked event.
/// </summary>
public sealed class PartitionsRevokedEventArgs : EventArgs
{
    /// <summary>
    /// The partitions that were revoked.
    /// </summary>
    public IReadOnlyList<(string Topic, int Partition)> Partitions { get; }

    /// <summary>
    /// Creates a new PartitionsRevokedEventArgs.
    /// </summary>
    public PartitionsRevokedEventArgs(IReadOnlyList<(string Topic, int Partition)> partitions)
    {
        Partitions = partitions;
    }
}
