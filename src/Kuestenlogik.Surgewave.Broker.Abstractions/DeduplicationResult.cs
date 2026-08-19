namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Result of a deduplication check. Lives in Broker.Abstractions (namespace kept as
/// <c>Kuestenlogik.Surgewave.Broker</c>) so protocol plugins can consume the neutral
/// <see cref="IDeduplicationManager"/> surface without referencing the broker engine
/// (#59 b4-tier2).
/// </summary>
/// <param name="IsDuplicate">Whether the window already holds this content.</param>
/// <param name="OriginalOffset">Offset the earlier copy landed at, or -1.</param>
/// <param name="ContentHash">
/// The hash computed for the checked bytes, so a later <see cref="IDeduplicationManager.Register"/>
/// can reuse it instead of hashing the payload a second time — and, more importantly, without
/// needing the bytes again. Zero when the batch was too small to hash.
/// </param>
public readonly record struct DeduplicationResult(bool IsDuplicate, long OriginalOffset, ulong ContentHash);
