namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// Represents the detected changes between two versions of a schema subject.
/// </summary>
public sealed class SchemaChange
{
    /// <summary>
    /// The schema subject name (e.g., "orders-value").
    /// </summary>
    public required string SubjectName { get; init; }

    /// <summary>
    /// The old schema version number.
    /// </summary>
    public required int OldVersion { get; init; }

    /// <summary>
    /// The new schema version number.
    /// </summary>
    public required int NewVersion { get; init; }

    /// <summary>
    /// The overall type of change.
    /// </summary>
    public required SchemaChangeType ChangeType { get; init; }

    /// <summary>
    /// Detailed field-level changes.
    /// </summary>
    public required List<FieldChange> FieldChanges { get; init; } = [];

    /// <summary>
    /// The overall breaking level (maximum across all field changes).
    /// </summary>
    public BreakingLevel Breaking { get; init; } = BreakingLevel.None;

    /// <summary>
    /// When this change was detected.
    /// </summary>
    public DateTimeOffset DetectedAt { get; init; } = DateTimeOffset.UtcNow;
}
