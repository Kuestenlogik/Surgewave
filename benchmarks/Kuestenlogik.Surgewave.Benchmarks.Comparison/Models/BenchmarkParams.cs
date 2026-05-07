namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;

/// <summary>
/// Parameters controlling a comparison benchmark run.
/// Shared across all scenarios for consistent test conditions.
/// </summary>
public sealed class BenchmarkParams
{
    /// <summary>Number of messages to produce per test.</summary>
    public int MessageCount { get; set; } = 100_000;

    /// <summary>Size of each message payload in bytes.</summary>
    public int MessageSizeBytes { get; set; } = 100;

    /// <summary>Batch size for batched producer tests.</summary>
    public int BatchSize { get; set; } = 1000;

    /// <summary>Number of partitions per topic.</summary>
    public int Partitions { get; set; } = 3;

    /// <summary>Number of concurrent producers for multi-producer tests.</summary>
    public int ProducerCount { get; set; } = 1;

    /// <summary>Number of concurrent consumers for multi-consumer tests.</summary>
    public int ConsumerCount { get; set; } = 1;

    /// <summary>Kafka bootstrap server address (for Testcontainers or external).</summary>
    public string KafkaBootstrap { get; set; } = "localhost:29092";

    /// <summary>Whether to skip Kafka tests (Surgewave-only mode).</summary>
    public bool SurgewaveOnly { get; set; }

    /// <summary>Path to save JSON results.</summary>
    public string? OutputPath { get; set; }

    /// <summary>Path to save Markdown report.</summary>
    public string? ReportPath { get; set; }

    /// <summary>Set of platforms to benchmark. Defaults to embedded-native + kafka.</summary>
    public HashSet<BenchmarkPlatform> Platforms { get; set; } =
    [
        BenchmarkPlatform.SurgewaveEmbeddedNative,
        BenchmarkPlatform.ApacheKafkaContainer
    ];

    /// <summary>Address for connecting to a standalone (external) Surgewave broker.</summary>
    public string SurgewaveStandaloneAddress { get; set; } = "localhost:9092";

    /// <summary>Docker image for the Surgewave container platform.</summary>
    public string SurgewaveContainerImage { get; set; } = "surgewave:latest";

    /// <summary>Docker image for the Apache Kafka container platform.</summary>
    public string KafkaContainerImage { get; set; } = "confluentinc/cp-kafka:7.6.0";

    /// <summary>Docker image for the Redpanda container platform.</summary>
    public string RedpandaContainerImage { get; set; } = "redpandadata/redpanda:latest";

    /// <summary>Checks whether a specific platform is enabled for this run.</summary>
    public bool IsPlatformEnabled(BenchmarkPlatform platform) => Platforms.Contains(platform);
}
