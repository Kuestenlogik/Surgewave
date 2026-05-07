namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Interface for reading offsets stored by source connectors.
/// </summary>
public interface IOffsetStorageReader
{
    /// <summary>
    /// Get the offset for the given partition.
    /// </summary>
    IDictionary<string, object>? Offset(IDictionary<string, object> partition);

    /// <summary>
    /// Get offsets for multiple partitions.
    /// </summary>
    IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
        IReadOnlyCollection<IDictionary<string, object>> partitions);
}
