namespace Kuestenlogik.Surgewave.Storage.Disaggregated;

/// <summary>
/// Topic-config key + wire-string constants for the <c>storage.mode</c>
/// property. The string values are the contract between Kafka-wire
/// clients (which only ever see strings) and the broker — do not
/// rename them once shipped.
/// </summary>
public static class StorageModeKeys
{
    /// <summary>Topic-config key, matches Kafka naming convention.</summary>
    public const string ConfigKey = "storage.mode";

    /// <summary><see cref="StorageMode.Replicated"/> wire string.</summary>
    public const string Replicated = "replicated";

    /// <summary><see cref="StorageMode.DisaggregatedWal"/> wire string.</summary>
    public const string DisaggregatedWal = "disaggregated-wal";

    /// <summary><see cref="StorageMode.DisaggregatedStateless"/> wire string.</summary>
    public const string DisaggregatedStateless = "disaggregated-stateless";

    /// <summary>Parse a config-value string into the enum. Unknown values throw.</summary>
    public static StorageMode Parse(string value) => value switch
    {
        Replicated => StorageMode.Replicated,
        DisaggregatedWal => StorageMode.DisaggregatedWal,
        DisaggregatedStateless => StorageMode.DisaggregatedStateless,
        _ => throw new ArgumentException(
            $"Unknown storage.mode '{value}'. Valid: '{Replicated}', '{DisaggregatedWal}', '{DisaggregatedStateless}'.",
            nameof(value)),
    };

    /// <summary>Render the enum value into its config-value string.</summary>
    public static string ToWireString(this StorageMode mode) => mode switch
    {
        StorageMode.Replicated => Replicated,
        StorageMode.DisaggregatedWal => DisaggregatedWal,
        StorageMode.DisaggregatedStateless => DisaggregatedStateless,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
    };

    /// <summary>Whether this mode bypasses the existing ISR replication path.</summary>
    public static bool IsDisaggregated(this StorageMode mode) =>
        mode is StorageMode.DisaggregatedWal or StorageMode.DisaggregatedStateless;
}
