namespace Kuestenlogik.Surgewave.Storage.Disaggregated;

/// <summary>
/// Validates a topic's <c>storage.mode</c> choice against the topic-
/// create request and the cluster's current configuration. Failures
/// surface as <see cref="StorageModeValidationException"/> so callers
/// (Native CreateTopic handler, Kafka CreateTopics handler, Control UI)
/// can render the same diagnostic string everywhere.
/// </summary>
public static class StorageModeValidator
{
    /// <summary>
    /// Validate that the requested mode + replication-factor combination
    /// is acceptable. ADR-014 forbids <c>replication.factor &gt; 1</c>
    /// for any disaggregated mode (S3 is the durability layer).
    /// </summary>
    public static void Validate(
        StorageMode mode,
        short replicationFactor,
        bool objectStoreConfigured,
        bool isEmbeddedRuntime)
    {
        if (mode.IsDisaggregated() && replicationFactor > 1)
        {
            throw new StorageModeValidationException(
                $"storage.mode='{mode.ToWireString()}' requires replication.factor=1 "
                + $"(got {replicationFactor}). Disaggregated topics derive durability from the "
                + "object store; in-cluster replication on top would double the storage cost without "
                + "adding durability. See ADR-014.");
        }

        if (mode == StorageMode.DisaggregatedStateless && isEmbeddedRuntime)
        {
            throw new StorageModeValidationException(
                "storage.mode='disaggregated-stateless' is not supported in embedded mode "
                + "(no in-process object store). Use 'disaggregated-wal' for embedded scenarios "
                + "or run a standalone broker. See ADR-014.");
        }

        if (mode.IsDisaggregated() && !objectStoreConfigured)
        {
            throw new StorageModeValidationException(
                $"storage.mode='{mode.ToWireString()}' requires an object-store endpoint "
                + "configured on this cluster (Surgewave:Storage:Disaggregated:* settings). "
                + "Configure the bucket + credentials, or pick storage.mode='replicated'.");
        }
    }

    /// <summary>
    /// Read + parse the <c>storage.mode</c> entry from a config dictionary.
    /// Missing entry returns <see cref="StorageMode.Replicated"/>.
    /// </summary>
    public static StorageMode ResolveFromConfig(IReadOnlyDictionary<string, string> config)
    {
        if (config.TryGetValue(StorageModeKeys.ConfigKey, out var value))
        {
            return StorageModeKeys.Parse(value);
        }
        return StorageMode.Replicated;
    }
}

/// <summary>Raised when a topic-create / alter-config request violates ADR-014 invariants.</summary>
public sealed class StorageModeValidationException : Exception
{
    public StorageModeValidationException(string message) : base(message) { }
    public StorageModeValidationException(string message, Exception innerException) : base(message, innerException) { }
}
