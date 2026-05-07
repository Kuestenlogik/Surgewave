namespace Kuestenlogik.Surgewave.Storage;

/// <summary>
/// Factory for creating Surgewave buffers.
/// </summary>
public interface ISurgewaveBufferPool
{
    /// <summary>
    /// Rent a buffer of at least the specified size.
    /// The buffer must be returned via Dispose().
    /// </summary>
    ISurgewaveWritableBuffer Rent(int minimumLength);

    /// <summary>
    /// Rent a buffer and copy the source data into it.
    /// </summary>
    ISurgewaveBuffer RentAndCopy(ReadOnlySpan<byte> source);

    /// <summary>
    /// Wrap existing memory without copying.
    /// The caller is responsible for ensuring the memory remains valid.
    /// </summary>
    ISurgewaveBuffer Wrap(ReadOnlyMemory<byte> memory);

    /// <summary>
    /// Create an empty buffer.
    /// </summary>
    ISurgewaveBuffer Empty { get; }
}
