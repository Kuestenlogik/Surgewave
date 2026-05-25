namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Result of comparing two schema versions.
/// </summary>
public sealed class SchemaDiffResult
{
    public int OldVersion { get; set; }
    public int NewVersion { get; set; }
    public List<SchemaDiffEntry> Entries { get; set; } = [];
    public bool HasBreakingChanges => Entries.Any(e => e.IsBreaking);
    public int AddedCount => Entries.Count(e => e.ChangeType == DiffChangeType.Added);
    public int RemovedCount => Entries.Count(e => e.ChangeType == DiffChangeType.Removed);
    public int ModifiedCount => Entries.Count(e => e.ChangeType == DiffChangeType.Modified);
}

/// <summary>
/// A single difference entry between two schema versions.
/// </summary>
public sealed class SchemaDiffEntry
{
    public string Path { get; set; } = "";
    public DiffChangeType ChangeType { get; set; }
    public string? OldValue { get; set; }
    public string? NewValue { get; set; }
    public bool IsBreaking { get; set; }
    public string? Description { get; set; }
}

public enum DiffChangeType
{
    Added,
    Removed,
    Modified,
    Unchanged
}

/// <summary>
/// Schema usage information: which topics reference a schema subject.
/// </summary>
public sealed class SchemaUsageInfo
{
    public string Subject { get; set; } = "";
    public int LatestVersion { get; set; }
    public string SchemaType { get; set; } = "AVRO";
    public int VersionCount { get; set; }
    public List<string> Topics { get; set; } = [];
    public int FieldCount { get; set; }
    public bool IsOrphaned => Topics.Count == 0;
    public string? CompatibilityLevel { get; set; }
}

/// <summary>
/// Topic catalog entry with business metadata.
/// </summary>
public sealed class CatalogEntry
{
    public string TopicName { get; set; } = "";
    public string? Description { get; set; }
    public string? Owner { get; set; }
    public string? Team { get; set; }
    public List<string> Tags { get; set; } = [];
    public PiiClassification PiiClassification { get; set; } = PiiClassification.None;
    public List<string> PiiFields { get; set; } = [];
    public string? SchemaSubject { get; set; }
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;
    public string? Notes { get; set; }
    public DataQuality Quality { get; set; } = DataQuality.Unknown;
}

public enum PiiClassification
{
    None,
    Low,
    Medium,
    High,
    Critical
}

public enum DataQuality
{
    Unknown,
    Bronze,
    Silver,
    Gold,
    Platinum
}

/// <summary>
/// Full catalog with all entries and tag aggregation.
/// </summary>
public sealed class DataCatalogState
{
    public Dictionary<string, CatalogEntry> Entries { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public List<string> AvailableTags { get; set; } = ["streaming", "events", "commands", "snapshots", "dlq", "changelog", "metrics", "logs", "internal", "public-api", "private", "deprecated"];
    public List<string> AvailableTeams { get; set; } = [];
}
