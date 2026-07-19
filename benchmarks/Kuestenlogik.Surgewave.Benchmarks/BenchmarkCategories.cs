namespace Kuestenlogik.Surgewave.Benchmarks;

/// <summary>
/// Benchmark Category Documentation
/// =================================
///
/// This file documents the benchmark taxonomy and helps developers understand
/// which benchmarks to run for different scenarios.
///
/// CATEGORY 1: UNIT BENCHMARKS (No Broker Required)
/// -------------------------------------------------
/// These benchmarks test individual components in isolation.
/// Run with: dotnet run -- --filter "*{ClassName}*"
///
/// - SerializationBenchmarks: RecordBatch serialization/deserialization
/// - CompressionBenchmarks: LZ4/Snappy compression performance
/// - SimdBigEndianBenchmarks: SIMD-accelerated big-endian conversions
/// - BufferPoolBenchmarks: ArrayPool performance
/// - ByteArrayComparerBenchmarks: Key comparison algorithms
/// - StorageBenchmarks: Log append/read operations (uses temp files)
/// - MemoryMappedReadBenchmarks: Memory-mapped file read performance
/// - ThroughputBenchmarks: Internal serialization throughput
/// - ProtocolBenchmarks: Wire protocol encoding/decoding
/// - StartupBenchmarks: Cold start performance
///
/// CATEGORY 2: INTEGRATION BENCHMARKS (Embedded Broker)
/// ----------------------------------------------------
/// These benchmarks use the EmbeddedSurgewave broker for self-contained testing.
/// They start/stop the broker automatically.
/// Run with: dotnet run -- embedded
///
/// - EmbeddedThroughputBenchmark: Producer/consumer throughput with embedded broker
///
/// CATEGORY 3: INTEGRATION BENCHMARKS (Standalone Broker Required)
/// ---------------------------------------------------------------
/// These benchmarks require a manually started broker.
/// Start broker first: dotnet run --project src/Kuestenlogik.Surgewave.Broker
/// Run with: dotnet run -- quick [msgCount] [msgSize]
///
/// - EndToEndBenchmarks: BenchmarkDotNet-based produce/consume via Kafka client
/// - QuickThroughputTest: Console-based quick throughput test via Kafka client
/// - ClientComparisonBenchmark: Surgewave native client vs Kafka client (same broker)
///
/// CATEGORY 4: CROSS-SYSTEM COMPARISON BENCHMARKS
/// ----------------------------------------------
/// These benchmarks compare Surgewave with Kafka brokers.
/// Requires both Surgewave and Kafka brokers running.
///
/// - BrokerComparisonBenchmark: Surgewave broker vs Kafka broker performance
/// - KafkaOnlyBenchmark: Kafka client against real Kafka broker (baseline)
/// - KafkaClientToSurgewaveBenchmark: Kafka client against Surgewave broker (compatibility)
///
/// BENCHMARK DIMENSIONS
/// ====================
/// When running benchmarks, consider testing across these dimensions:
///
/// - Protocol: Native vs Kafka wire protocol
/// - Message Size: Small (100B), Medium (1KB), Large (10KB+)
/// - Batch Size: Single vs batched (100, 1000, 10000 messages)
/// - Compression: None vs LZ4 vs Snappy
/// - SIMD: Enabled vs disabled (SimdBigEndian.MinBatchSize)
/// - Broker Mode: Embedded vs Standalone
/// - Cluster: Single node vs multi-node replication
/// </summary>
public static class BenchmarkCategories
{
    /// <summary>
    /// Unit benchmarks that require no running broker.
    /// </summary>
    public static readonly string[] UnitBenchmarks =
    [
        "SerializationBenchmarks",
        "CompressionBenchmarks",
        "SimdBigEndianBenchmarks",
        "BufferPoolBenchmarks",
        "ByteArrayComparerBenchmarks",
        "StorageBenchmarks",
        "MemoryMappedReadBenchmarks",
        "ThroughputBenchmarks",
        "ProtocolBenchmarks",
        "StartupBenchmarks"
    ];

    /// <summary>
    /// Integration benchmarks using embedded broker (self-contained).
    /// </summary>
    public static readonly string[] EmbeddedBenchmarks =
    [
        "EmbeddedThroughputBenchmark"
    ];

    /// <summary>
    /// Integration benchmarks requiring standalone broker.
    /// </summary>
    public static readonly string[] StandaloneBenchmarks =
    [
        "EndToEndBenchmarks",
        "QuickThroughputTest",
        "ClientComparisonBenchmark"
    ];

    /// <summary>
    /// Cross-system comparison benchmarks (Surgewave vs Kafka).
    /// </summary>
    public static readonly string[] ComparisonBenchmarks =
    [
        "BrokerComparisonBenchmark",
        "KafkaOnlyBenchmark",
        "KafkaClientToSurgewaveBenchmark"
    ];

    /// <summary>
    /// Transport layer benchmarks (ring buffer, TCP, SharedMemory).
    /// </summary>
    public static readonly string[] TransportBenchmarks =
    [
        "TransportBenchmarks",
        "TransportLatencyBenchmarks",
        "SpscRingBufferBenchmarks",
        "MappedFileKeyValueStoreBenchmarks"
    ];

    /// <summary>
    /// Streams benchmarks: state stores, topology throughput, joins, serdes.
    /// </summary>
    public static readonly string[] StreamsBenchmarks =
    [
        "StateStoreBenchmarks",
        "TopologyThroughputBenchmarks",
        "WindowStoreBenchmarks",
        "JoinBenchmarks",
        "SerdeBenchmarks"
    ];
}

/// <summary>
/// Category name constants for use with [BenchmarkCategory] attribute.
/// Use with: dotnet run -- --allCategories=Latency
/// Or combine: dotnet run -- --anyCategories=Latency,P99
/// </summary>
public static class Categories
{
    // Primary categories (project-level)
    public const string Unit = "Unit";
    public const string Storage = "Storage";
    public const string Integration = "Integration";
    public const string Comparison = "Comparison";
    public const string Latency = "Latency";

    // Sub-categories (feature-level)
    public const string Serialization = "Serialization";
    public const string Compression = "Compression";
    public const string Protocol = "Protocol";
    public const string Simd = "Simd";
    public const string BufferPool = "BufferPool";
    public const string Throughput = "Throughput";
    public const string Transport = "Transport";

    // Latency percentile categories
    public const string P50 = "P50";
    public const string P90 = "P90";
    public const string P99 = "P99";
    public const string P999 = "P99.9";
    public const string P9999 = "P99.99";
    public const string EndToEnd = "EndToEnd";

    // Streams categories
    public const string Streams = "Streams";
    public const string StateStore = "StateStore";
    public const string Topology = "Topology";
    public const string Window = "Window";
    public const string Join = "Join";
    public const string Serde = "Serde";

    // Shared memory / transport categories
    public const string SharedMemory = "SharedMemory";
    public const string MappedFileStore = "MappedFileStore";
    public const string RingBuffer = "RingBuffer";

    // System comparison categories
    public const string Kafka = "Kafka";
    public const string Redpanda = "Redpanda";
    public const string Native = "Native";
    public const string Embedded = "Embedded";
}
