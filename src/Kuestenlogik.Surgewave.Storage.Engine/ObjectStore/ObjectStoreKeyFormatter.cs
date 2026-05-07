namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Shared helper for formatting and parsing object store keys.
/// Used by all cloud object store providers (S3, Azure Blob, GCP Cloud Storage)
/// to ensure consistent key/path formatting.
/// </summary>
public static class ObjectStoreKeyFormatter
{
    private const string SegmentExtension = ".segment";

    /// <summary>
    /// Formats an object key for a segment in cloud storage.
    /// Key format: {prefix}/topics/{topic}/partitions/{partition}/{baseOffset:D20}.segment
    /// </summary>
    /// <param name="prefix">Optional prefix (e.g., "surgewave/data"). Null or empty means no prefix.</param>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="baseOffset">Base offset of the segment, zero-padded to 20 digits</param>
    /// <returns>Formatted object key</returns>
    public static string FormatKey(string? prefix, string topic, int partition, long baseOffset)
    {
        var key = $"topics/{topic}/partitions/{partition}/{baseOffset:D20}{SegmentExtension}";
        return string.IsNullOrEmpty(prefix) ? key : $"{prefix}/{key}";
    }

    /// <summary>
    /// Formats the prefix for listing segments in a topic-partition.
    /// </summary>
    /// <param name="prefix">Optional prefix</param>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <returns>Formatted listing prefix</returns>
    public static string FormatListPrefix(string? prefix, string topic, int partition)
    {
        var key = $"topics/{topic}/partitions/{partition}/";
        return string.IsNullOrEmpty(prefix) ? key : $"{prefix}/{key}";
    }

    /// <summary>
    /// Parses the base offset from a segment object key.
    /// Expects the filename portion to be a 20-digit zero-padded number with ".segment" extension.
    /// </summary>
    /// <param name="key">Full object key (path)</param>
    /// <returns>Parsed offset, or null if the key doesn't match the expected format</returns>
    public static long? ParseOffsetFromKey(string key)
    {
        // Extract filename without extension from the key path
        var lastSlash = key.LastIndexOf('/');
        var fileName = lastSlash >= 0 ? key[(lastSlash + 1)..] : key;

        // Remove the .segment extension
        if (!fileName.EndsWith(SegmentExtension, StringComparison.Ordinal))
            return null;

        var offsetStr = fileName[..^SegmentExtension.Length];
        return long.TryParse(offsetStr, out var offset) ? offset : null;
    }
}
