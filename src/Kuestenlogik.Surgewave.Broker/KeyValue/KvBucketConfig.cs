using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Broker.KeyValue;

/// <summary>
/// Configuration for a KV bucket.
/// </summary>
public sealed class KvBucketConfig : IValidatableConfig
{
    /// <summary>Maximum number of historical revisions to retain per key (default 1).</summary>
    [Range(1, int.MaxValue)]
    public int MaxHistoryPerKey { get; set; } = 1;

    /// <summary>Optional TTL for entries. Null means no expiration.</summary>
    public TimeSpan? Ttl { get; set; }

    /// <summary>Maximum value size in bytes (default 1 MB).</summary>
    [Range(1, int.MaxValue)]
    public int MaxValueSize { get; set; } = 1024 * 1024;

    /// <summary>Maximum total bucket size in bytes. Null means unlimited.</summary>
    public long? MaxBucketSize { get; set; }

    /// <summary>Replica count (default 1, single-broker).</summary>
    [Range(1, short.MaxValue)]
    public int Replicas { get; set; } = 1;

    /// <summary>Optional human-readable description.</summary>
    public string? Description { get; set; }

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (Ttl is { } ttl && ttl <= TimeSpan.Zero)
        {
            errors.Add($"{nameof(Ttl)}: must be positive when set.");
        }

        if (MaxBucketSize is { } size && size <= 0)
        {
            errors.Add($"{nameof(MaxBucketSize)}: must be positive when set.");
        }

        if (MaxBucketSize is { } max && max < MaxValueSize)
        {
            errors.Add($"{nameof(MaxBucketSize)} ({max}) must be >= {nameof(MaxValueSize)} ({MaxValueSize}).");
        }

        return errors;
    }
}
