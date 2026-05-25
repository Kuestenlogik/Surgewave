namespace Kuestenlogik.Surgewave.Core.Transforms;

/// <summary>
/// Context passed to an inline transform containing the record data and metadata.
/// </summary>
public sealed class TransformContext
{
    /// <summary>
    /// The topic the record belongs to.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The partition index within the topic.
    /// </summary>
    public required int Partition { get; init; }

    /// <summary>
    /// The record key bytes.
    /// </summary>
    public required byte[] Key { get; init; }

    /// <summary>
    /// The record value bytes.
    /// </summary>
    public required byte[] Value { get; init; }

    /// <summary>
    /// Record headers as key-value byte arrays.
    /// </summary>
    public Dictionary<string, byte[]> Headers { get; init; } = [];

    /// <summary>
    /// Record timestamp in milliseconds since epoch.
    /// </summary>
    public long Timestamp { get; init; }

    /// <summary>
    /// Whether this transform is executing in the produce or fetch path.
    /// </summary>
    public TransformPhase Phase { get; init; }
}
