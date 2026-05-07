namespace Kuestenlogik.Surgewave.Storage;

/// <summary>
/// A buffer that can be written to.
/// </summary>
public interface ISurgewaveWritableBuffer : ISurgewaveBuffer
{
    /// <summary>
    /// Get a writable span over the buffer contents.
    /// </summary>
    new Span<byte> Span { get; }

    /// <summary>
    /// Get a writable memory over the buffer contents.
    /// </summary>
    new Memory<byte> Memory { get; }
}
