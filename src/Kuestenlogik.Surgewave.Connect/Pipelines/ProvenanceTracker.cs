using System.Text;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Tracks record provenance through pipeline nodes via headers.
/// </summary>
public static class ProvenanceTracker
{
    public const string PathHeader = "_provenance_path";
    public const string TimestampsHeader = "_provenance_timestamps";

    /// <summary>
    /// Whether provenance tracking is enabled globally.
    /// </summary>
    public static bool Enabled { get; set; }

    /// <summary>
    /// Append a provenance step to a record's headers.
    /// </summary>
    public static void AppendProvenance(Dictionary<string, byte[]> headers, string nodeId, DateTimeOffset timestamp)
    {
        var existingPath = headers.TryGetValue(PathHeader, out var pathBytes)
            ? Encoding.UTF8.GetString(pathBytes)
            : "";

        var existingTimestamps = headers.TryGetValue(TimestampsHeader, out var tsBytes)
            ? Encoding.UTF8.GetString(tsBytes)
            : "";

        var newPath = string.IsNullOrEmpty(existingPath)
            ? nodeId
            : $"{existingPath}|{nodeId}";

        var tsStr = timestamp.ToUnixTimeMilliseconds().ToString();
        var newTimestamps = string.IsNullOrEmpty(existingTimestamps)
            ? tsStr
            : $"{existingTimestamps}|{tsStr}";

        headers[PathHeader] = Encoding.UTF8.GetBytes(newPath);
        headers[TimestampsHeader] = Encoding.UTF8.GetBytes(newTimestamps);
    }

    /// <summary>
    /// Extract provenance information from headers.
    /// </summary>
    public static ProvenanceInfo? ExtractProvenance(IReadOnlyDictionary<string, byte[]>? headers)
    {
        if (headers == null)
            return null;

        if (!headers.TryGetValue(PathHeader, out var pathBytes))
            return null;

        var path = Encoding.UTF8.GetString(pathBytes);
        var nodeIds = path.Split('|', StringSplitOptions.RemoveEmptyEntries);

        string[] timestamps = [];
        if (headers.TryGetValue(TimestampsHeader, out var tsBytes))
        {
            timestamps = Encoding.UTF8.GetString(tsBytes).Split('|', StringSplitOptions.RemoveEmptyEntries);
        }

        var steps = new List<ProvenanceStep>();
        for (var i = 0; i < nodeIds.Length; i++)
        {
            var ts = i < timestamps.Length && long.TryParse(timestamps[i], out var ms)
                ? DateTimeOffset.FromUnixTimeMilliseconds(ms)
                : DateTimeOffset.UtcNow;
            steps.Add(new ProvenanceStep(nodeIds[i], ts));
        }

        return new ProvenanceInfo(steps);
    }
}
