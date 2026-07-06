using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for the broker's Key-Value Store REST API (/api/kv/buckets).
/// </summary>
public interface IKvApiClient
{
    /// <summary>List all KV buckets with their summary stats.</summary>
    Task<IReadOnlyList<KvBucketModel>> ListBucketsAsync(CancellationToken cancellationToken = default);

    /// <summary>Create a new KV bucket. Returns null when creation failed (e.g. name conflict).</summary>
    Task<KvBucketModel?> CreateBucketAsync(CreateKvBucketRequest request, CancellationToken cancellationToken = default);

    /// <summary>Delete a KV bucket including all its keys.</summary>
    Task<bool> DeleteBucketAsync(string bucket, CancellationToken cancellationToken = default);

    /// <summary>List all keys in a bucket.</summary>
    Task<IReadOnlyList<string>> ListKeysAsync(string bucket, CancellationToken cancellationToken = default);

    /// <summary>Get the latest entry for a key, or null when the key does not exist.</summary>
    Task<KvEntryModel?> GetEntryAsync(string bucket, string key, CancellationToken cancellationToken = default);

    /// <summary>Put a value (raw bytes) for a key. Returns the stored entry, or null on failure.</summary>
    Task<KvEntryModel?> PutEntryAsync(string bucket, string key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>Delete a key. Returns the tombstone entry, or null on failure.</summary>
    Task<KvEntryModel?> DeleteEntryAsync(string bucket, string key, CancellationToken cancellationToken = default);

    /// <summary>Get the revision history for a key (newest first as delivered by the broker).</summary>
    Task<IReadOnlyList<KvEntryModel>> GetHistoryAsync(string bucket, string key, CancellationToken cancellationToken = default);
}
