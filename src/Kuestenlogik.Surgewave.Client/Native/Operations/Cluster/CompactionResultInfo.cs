namespace Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;

/// <summary>
/// Result of a log compaction operation.
/// </summary>
public record CompactionResultInfo(bool Success, long RecordsRemoved, long BytesRemoved, int SegmentsCompacted);
