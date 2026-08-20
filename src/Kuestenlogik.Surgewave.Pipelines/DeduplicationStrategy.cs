namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>Which record wins when duplicates arrive within the deduplication window.</summary>
public enum DeduplicationStrategy
{
    /// <summary>The first record passes; later duplicates are dropped.</summary>
    First,

    /// <summary>Each duplicate replaces the previous one; the last record is emitted.</summary>
    Last,
}
