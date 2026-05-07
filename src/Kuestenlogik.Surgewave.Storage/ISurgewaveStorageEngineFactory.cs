namespace Kuestenlogik.Surgewave.Storage;

/// <summary>
/// Factory for creating storage engines.
/// </summary>
public interface ISurgewaveStorageEngineFactory
{
    /// <summary>
    /// Create a new storage segment.
    /// </summary>
    /// <param name="directory">Directory for storage files</param>
    /// <param name="baseOffset">Base offset for this segment</param>
    /// <param name="maxSize">Maximum segment size in bytes</param>
    /// <returns>A new storage engine instance</returns>
    ISurgewaveStorageEngine Create(string directory, long baseOffset, long maxSize);

    /// <summary>
    /// Open an existing storage segment.
    /// </summary>
    ISurgewaveStorageEngine Open(string directory, long baseOffset);
}
