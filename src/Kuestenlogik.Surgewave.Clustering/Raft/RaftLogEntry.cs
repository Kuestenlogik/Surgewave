namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// A single entry in the Raft log.
/// </summary>
public sealed class RaftLogEntry
{
    /// <summary>
    /// The term when this entry was received by the leader.
    /// </summary>
    public required int Term { get; init; }

    /// <summary>
    /// The index of this entry in the log (1-based).
    /// </summary>
    public required long Index { get; init; }

    /// <summary>
    /// The type of metadata command this entry represents.
    /// </summary>
    public required MetadataCommandType CommandType { get; init; }

    /// <summary>
    /// The serialized command data.
    /// </summary>
    public required byte[] Data { get; init; }

    /// <summary>
    /// Timestamp when this entry was created.
    /// </summary>
    public long Timestamp { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
