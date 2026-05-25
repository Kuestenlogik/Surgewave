namespace Kuestenlogik.Surgewave.Broker.KeyValue;

/// <summary>
/// Metadata about a stored object.
/// </summary>
public sealed record ObjectInfo(
    string Name,
    long Size,
    int Chunks,
    string? ContentType,
    DateTimeOffset Created);

/// <summary>
/// Result returned when retrieving an object — metadata plus reassembled data.
/// </summary>
public sealed record ObjectResult(
    ObjectInfo Info,
    byte[] Data);
