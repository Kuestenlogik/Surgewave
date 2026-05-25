namespace Kuestenlogik.Surgewave.Storage;

/// <summary>
/// A read lease that holds a buffer and batch metadata.
/// Must be disposed to return resources to the pool.
/// </summary>
public interface IStorageReadLease : IDisposable
{
    /// <summary>
    /// The data buffer containing all batches.
    /// Valid until this lease is disposed.
    /// </summary>
    ISurgewaveBuffer Data { get; }

    /// <summary>
    /// Offsets within Data where each batch starts.
    /// </summary>
    IReadOnlyList<int> BatchOffsets { get; }

    /// <summary>
    /// Number of batches in this read.
    /// </summary>
    int BatchCount => BatchOffsets.Count;

    /// <summary>
    /// Whether this lease contains any data.
    /// </summary>
    bool IsEmpty => Data.IsEmpty;

    /// <summary>
    /// Get a specific batch by index.
    /// Returns a slice of the Data buffer.
    /// </summary>
    ISurgewaveBuffer GetBatch(int index);
}
