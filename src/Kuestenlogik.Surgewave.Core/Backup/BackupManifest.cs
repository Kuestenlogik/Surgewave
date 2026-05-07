using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Core.Backup;

/// <summary>
/// Manifest file stored in each backup containing metadata about the backup.
/// </summary>
public sealed class BackupManifest
{
    /// <summary>
    /// Manifest format version for compatibility checking.
    /// </summary>
    public int Version { get; set; } = 1;

    /// <summary>
    /// Unique identifier for this backup.
    /// </summary>
    public string BackupId { get; set; } = Guid.NewGuid().ToString("N");

    /// <summary>
    /// UTC timestamp when the backup was created.
    /// </summary>
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Surgewave version that created this backup.
    /// </summary>
    public string SurgewaveVersion { get; set; } = "1.0.0";

    /// <summary>
    /// Source broker ID if available.
    /// </summary>
    public int? SourceBrokerId { get; set; }

    /// <summary>
    /// Source broker address.
    /// </summary>
    public string? SourceBroker { get; set; }

    /// <summary>
    /// Topics included in this backup.
    /// </summary>
    public List<BackupTopicInfo> Topics { get; set; } = [];

    /// <summary>
    /// Total number of files in the backup.
    /// </summary>
    public int TotalFiles { get; set; }

    /// <summary>
    /// Total bytes of all files in the backup.
    /// </summary>
    public long TotalBytes { get; set; }

    /// <summary>
    /// Whether the backup includes metadata files.
    /// </summary>
    public bool IncludesMetadata { get; set; } = true;

    /// <summary>
    /// Whether backup was verified after creation.
    /// </summary>
    public bool Verified { get; set; }

    /// <summary>
    /// Optional description for this backup.
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// File checksums for verification (path -> SHA256 hash).
    /// </summary>
    public Dictionary<string, string> FileChecksums { get; set; } = [];

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Serialize manifest to JSON.
    /// </summary>
    public string ToJson() => JsonSerializer.Serialize(this, JsonOptions);

    /// <summary>
    /// Deserialize manifest from JSON.
    /// </summary>
    public static BackupManifest FromJson(string json) =>
        JsonSerializer.Deserialize<BackupManifest>(json, JsonOptions)
        ?? throw new InvalidOperationException("Failed to deserialize backup manifest");

    /// <summary>
    /// Save manifest to file.
    /// </summary>
    public async Task SaveAsync(string path, CancellationToken cancellationToken = default)
    {
        var json = ToJson();
        await File.WriteAllTextAsync(path, json, cancellationToken);
    }

    /// <summary>
    /// Load manifest from file.
    /// </summary>
    public static async Task<BackupManifest> LoadAsync(string path, CancellationToken cancellationToken = default)
    {
        var json = await File.ReadAllTextAsync(path, cancellationToken);
        return FromJson(json);
    }

    /// <summary>
    /// Manifest filename constant.
    /// </summary>
    public const string FileName = "manifest.json";
}

/// <summary>
/// Information about a topic in the backup.
/// </summary>
public sealed class BackupTopicInfo
{
    /// <summary>
    /// Topic name.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Topic unique identifier.
    /// </summary>
    public Guid TopicId { get; set; }

    /// <summary>
    /// Number of partitions.
    /// </summary>
    public int PartitionCount { get; set; }

    /// <summary>
    /// Partition details.
    /// </summary>
    public List<BackupPartitionInfo> Partitions { get; set; } = [];

    /// <summary>
    /// Topic configuration.
    /// </summary>
    public Dictionary<string, string> Config { get; set; } = [];

    /// <summary>
    /// Total bytes for this topic.
    /// </summary>
    public long TotalBytes { get; set; }

    /// <summary>
    /// Total number of segments.
    /// </summary>
    public int TotalSegments { get; set; }
}

/// <summary>
/// Information about a partition in the backup.
/// </summary>
public sealed class BackupPartitionInfo
{
    /// <summary>
    /// Partition ID.
    /// </summary>
    public int PartitionId { get; set; }

    /// <summary>
    /// Log start offset.
    /// </summary>
    public long LogStartOffset { get; set; }

    /// <summary>
    /// High watermark (committed offset).
    /// </summary>
    public long HighWatermark { get; set; }

    /// <summary>
    /// Number of segments.
    /// </summary>
    public int SegmentCount { get; set; }

    /// <summary>
    /// Total bytes for this partition.
    /// </summary>
    public long TotalBytes { get; set; }

    /// <summary>
    /// Segment files included.
    /// </summary>
    public List<BackupSegmentInfo> Segments { get; set; } = [];
}

/// <summary>
/// Information about a segment in the backup.
/// </summary>
public sealed class BackupSegmentInfo
{
    /// <summary>
    /// Base offset of the segment.
    /// </summary>
    public long BaseOffset { get; set; }

    /// <summary>
    /// Size of the log file in bytes.
    /// </summary>
    public long LogSize { get; set; }

    /// <summary>
    /// Size of the index file in bytes.
    /// </summary>
    public long IndexSize { get; set; }

    /// <summary>
    /// Size of the time index file in bytes.
    /// </summary>
    public long TimeIndexSize { get; set; }

    /// <summary>
    /// Log file name.
    /// </summary>
    public string LogFile { get; set; } = string.Empty;

    /// <summary>
    /// Index file name.
    /// </summary>
    public string IndexFile { get; set; } = string.Empty;

    /// <summary>
    /// Time index file name.
    /// </summary>
    public string TimeIndexFile { get; set; } = string.Empty;

    /// <summary>
    /// Largest timestamp known for any record in this segment, in Unix
    /// milliseconds. Read from the last entry of the segment's
    /// <c>.timeindex</c> at backup time. Used by point-in-time restore to
    /// decide whether the segment is fully before / after / spanning the
    /// cutoff timestamp. <c>0</c> means the segment had no time-index entries
    /// (rare — empty or pre-rotation segment).
    /// </summary>
    public long MaxTimestampMs { get; set; }
}
