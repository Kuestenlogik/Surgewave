namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;

/// <summary>
/// Defines the 8 benchmark platform configurations for comparison testing.
/// Each configuration specifies the broker deployment and client protocol combination.
/// </summary>
public enum BenchmarkPlatform
{
    /// <summary>1. In-Process embedded Surgewave broker, Surgewave native client.</summary>
    SurgewaveEmbeddedNative,

    /// <summary>2. In-Process embedded Surgewave broker, Confluent.Kafka client.</summary>
    SurgewaveEmbeddedKafka,

    /// <summary>3. Separate process Surgewave broker, Surgewave native client.</summary>
    SurgewaveStandaloneNative,

    /// <summary>4. Separate process Surgewave broker, Confluent.Kafka client.</summary>
    SurgewaveStandaloneKafka,

    /// <summary>5. Surgewave Docker container, Surgewave native client.</summary>
    SurgewaveContainerNative,

    /// <summary>6. Surgewave Docker container, Confluent.Kafka client.</summary>
    SurgewaveContainerKafka,

    /// <summary>7. Apache Kafka Docker container, Confluent.Kafka client.</summary>
    ApacheKafkaContainer,

    /// <summary>8. Redpanda Docker container, Confluent.Kafka client.</summary>
    RedpandaContainer
}

/// <summary>
/// Extension methods for <see cref="BenchmarkPlatform"/>.
/// </summary>
public static class BenchmarkPlatformExtensions
{
    /// <summary>Returns a human-readable display name for the platform.</summary>
    public static string DisplayName(this BenchmarkPlatform platform) => platform switch
    {
        BenchmarkPlatform.SurgewaveEmbeddedNative => "Surgewave Embedded + Native",
        BenchmarkPlatform.SurgewaveEmbeddedKafka => "Surgewave Embedded + Kafka",
        BenchmarkPlatform.SurgewaveStandaloneNative => "Surgewave Standalone + Native",
        BenchmarkPlatform.SurgewaveStandaloneKafka => "Surgewave Standalone + Kafka",
        BenchmarkPlatform.SurgewaveContainerNative => "Surgewave Container + Native",
        BenchmarkPlatform.SurgewaveContainerKafka => "Surgewave Container + Kafka",
        BenchmarkPlatform.ApacheKafkaContainer => "Apache Kafka Container",
        BenchmarkPlatform.RedpandaContainer => "Redpanda Container",
        _ => platform.ToString()
    };

    /// <summary>Returns the Spectre.Console color markup for the platform.</summary>
    public static string Color(this BenchmarkPlatform platform) => platform switch
    {
        BenchmarkPlatform.SurgewaveEmbeddedNative => "green",
        BenchmarkPlatform.SurgewaveEmbeddedKafka => "cyan",
        BenchmarkPlatform.SurgewaveStandaloneNative => "blue",
        BenchmarkPlatform.SurgewaveStandaloneKafka => "teal",
        BenchmarkPlatform.SurgewaveContainerNative => "purple",
        BenchmarkPlatform.SurgewaveContainerKafka => "magenta",
        BenchmarkPlatform.ApacheKafkaContainer => "yellow",
        BenchmarkPlatform.RedpandaContainer => "orange1",
        _ => "white"
    };

    /// <summary>Returns whether this platform uses an embedded Surgewave broker.</summary>
    public static bool IsEmbedded(this BenchmarkPlatform platform) =>
        platform is BenchmarkPlatform.SurgewaveEmbeddedNative or BenchmarkPlatform.SurgewaveEmbeddedKafka;

    /// <summary>Returns whether this platform uses Docker containers.</summary>
    public static bool IsContainer(this BenchmarkPlatform platform) =>
        platform is BenchmarkPlatform.SurgewaveContainerNative or BenchmarkPlatform.SurgewaveContainerKafka
                  or BenchmarkPlatform.ApacheKafkaContainer or BenchmarkPlatform.RedpandaContainer;

    /// <summary>Returns whether this platform uses the native Surgewave client.</summary>
    public static bool IsNativeClient(this BenchmarkPlatform platform) =>
        platform is BenchmarkPlatform.SurgewaveEmbeddedNative or BenchmarkPlatform.SurgewaveStandaloneNative
                  or BenchmarkPlatform.SurgewaveContainerNative;

    /// <summary>Returns whether this platform uses the Confluent.Kafka client.</summary>
    public static bool IsKafkaClient(this BenchmarkPlatform platform) => !platform.IsNativeClient();

    /// <summary>Parses a CLI-friendly string into a platform enum.</summary>
    public static bool TryParse(string value, out BenchmarkPlatform platform)
    {
        platform = value.ToLowerInvariant().Replace("-", "").Replace("_", "") switch
        {
            "embeddednative" or "surgewaveembeddednative" => BenchmarkPlatform.SurgewaveEmbeddedNative,
            "embeddedkafka" or "surgewaveembeddedkafka" => BenchmarkPlatform.SurgewaveEmbeddedKafka,
            "standalonenative" or "surgewavestandalonenative" => BenchmarkPlatform.SurgewaveStandaloneNative,
            "standalonekafka" or "surgewavestandalonekafka" => BenchmarkPlatform.SurgewaveStandaloneKafka,
            "containernative" or "surgewavecontainernative" => BenchmarkPlatform.SurgewaveContainerNative,
            "containerkafka" or "surgewavecontainerkafka" => BenchmarkPlatform.SurgewaveContainerKafka,
            "kafka" or "apachekafka" or "apachekafkacontainer" => BenchmarkPlatform.ApacheKafkaContainer,
            "redpanda" or "redpandacontainer" => BenchmarkPlatform.RedpandaContainer,
            _ => default
        };

        // Check if we actually matched something (default case returns SurgewaveEmbeddedNative which is 0)
        if (platform == default && value.ToLowerInvariant().Replace("-", "").Replace("_", "")
            is not ("embeddednative" or "surgewaveembeddednative"))
            return false;

        return true;
    }

    /// <summary>Parses a preset name into a set of platforms.</summary>
    public static HashSet<BenchmarkPlatform>? ParsePreset(string preset) => preset.ToLowerInvariant() switch
    {
        "all" => [.. Enum.GetValues<BenchmarkPlatform>()],
        "containers" => [
            BenchmarkPlatform.SurgewaveContainerNative,
            BenchmarkPlatform.SurgewaveContainerKafka,
            BenchmarkPlatform.ApacheKafkaContainer,
            BenchmarkPlatform.RedpandaContainer
        ],
        "fair" => [
            BenchmarkPlatform.SurgewaveContainerKafka,
            BenchmarkPlatform.ApacheKafkaContainer,
            BenchmarkPlatform.RedpandaContainer
        ],
        "surgewave" => [
            BenchmarkPlatform.SurgewaveEmbeddedNative,
            BenchmarkPlatform.SurgewaveEmbeddedKafka,
            BenchmarkPlatform.SurgewaveStandaloneNative,
            BenchmarkPlatform.SurgewaveStandaloneKafka,
            BenchmarkPlatform.SurgewaveContainerNative,
            BenchmarkPlatform.SurgewaveContainerKafka
        ],
        "embedded" => [
            BenchmarkPlatform.SurgewaveEmbeddedNative,
            BenchmarkPlatform.SurgewaveEmbeddedKafka
        ],
        "standalone" => [
            BenchmarkPlatform.SurgewaveStandaloneNative,
            BenchmarkPlatform.SurgewaveStandaloneKafka
        ],
        _ => null
    };
}
