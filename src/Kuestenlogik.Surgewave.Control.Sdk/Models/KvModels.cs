namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Summary information about a KV bucket (mirror of the broker's KvBucketInfo).
/// </summary>
public sealed record KvBucketModel(
    string Name,
    int KeyCount,
    long TotalValueBytes,
    long LatestRevision,
    KvBucketConfigModel? Config);

/// <summary>
/// Configuration of a KV bucket (mirror of the broker's KvBucketConfig).
/// </summary>
public sealed record KvBucketConfigModel
{
    public int MaxHistoryPerKey { get; init; } = 1;
    public TimeSpan? Ttl { get; init; }
    public int MaxValueSize { get; init; } = 1024 * 1024;
    public long? MaxBucketSize { get; init; }
    public int Replicas { get; init; } = 1;
    public string? Description { get; init; }
}

/// <summary>
/// Request body for creating a KV bucket (mirror of the broker's CreateBucketRequest).
/// </summary>
public sealed record CreateKvBucketRequest
{
    public string Name { get; init; } = "";
    public int? MaxHistoryPerKey { get; init; }
    public double? TtlSeconds { get; init; }
    public int? MaxValueSize { get; init; }
    public long? MaxBucketSize { get; init; }
    public int? Replicas { get; init; }
    public string? Description { get; init; }
}

/// <summary>
/// A KV entry revision; the value is base64-encoded for JSON transport.
/// </summary>
public sealed record KvEntryModel(
    string Key,
    string ValueBase64,
    long Revision,
    DateTimeOffset Created,
    string Operation);
