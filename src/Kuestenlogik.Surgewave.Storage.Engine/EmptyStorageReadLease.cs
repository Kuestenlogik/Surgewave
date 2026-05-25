namespace Kuestenlogik.Surgewave.Storage.Engine;

/// <summary>
/// Empty read lease singleton.
/// </summary>
public sealed class EmptyStorageReadLease : IStorageReadLease
{
    public static readonly EmptyStorageReadLease Instance = new();

    private EmptyStorageReadLease() { }

    public ISurgewaveBuffer Data => EmptySurgewaveBuffer.Instance;
    public IReadOnlyList<int> BatchOffsets => [];

    public ISurgewaveBuffer GetBatch(int index)
        => throw new ArgumentOutOfRangeException(nameof(index));

    public void Dispose() { } // No-op for singleton
}
