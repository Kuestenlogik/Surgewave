namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Result of a deduplication check. Lives in Broker.Abstractions (namespace kept as
/// <c>Kuestenlogik.Surgewave.Broker</c>) so protocol plugins can consume the neutral
/// <see cref="IDeduplicationManager"/> surface without referencing the broker engine
/// (#59 b4-tier2).
/// </summary>
public readonly record struct DeduplicationResult(bool IsDuplicate, long OriginalOffset);
