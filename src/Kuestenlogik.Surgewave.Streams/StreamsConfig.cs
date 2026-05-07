using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Streams.Dlq;
using Kuestenlogik.Surgewave.Streams.ExceptionHandling;
using Kuestenlogik.Surgewave.Streams.Resilience;
using Kuestenlogik.Surgewave.Streams.Runtime;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Configuration for a <see cref="StreamsApplication"/>.
/// Defines connection settings, processing guarantees, threading, and advanced behaviors.
/// </summary>
/// <example>
/// <code>
/// var config = new StreamsConfig
/// {
///     ApplicationId = "word-count",
///     BootstrapServers = "localhost:9092",
///     NumStreamThreads = 4,
///     ProcessingGuarantee = ProcessingGuarantee.ExactlyOnce
/// };
/// </code>
/// </example>
public sealed class StreamsConfig : IValidatableConfig
{
    /// <summary>Unique identifier for this streams application. Used for internal topic naming and consumer group IDs.</summary>
    [Required]
    [MinLength(1)]
    public required string ApplicationId { get; init; }

    /// <summary>Broker connection string in host:port format.</summary>
    [Required]
    [MinLength(1)]
    public required string BootstrapServers { get; init; }

    /// <summary>Directory for local state store data. If null, uses in-memory stores only.</summary>
    public string? StateDir { get; init; }

    /// <summary>Number of stream processing threads. Each thread processes a subset of partitions independently.</summary>
    [Range(1, 1024)]
    public int NumStreamThreads { get; init; } = 1;

    /// <summary>Interval in milliseconds between automatic offset commits.</summary>
    [Range(1, long.MaxValue)]
    public long CommitIntervalMs { get; init; } = 30_000;

    /// <summary>Maximum time to wait for new records during each poll cycle.</summary>
    public TimeSpan PollTimeout { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Where to start consuming if no committed offset exists ("earliest" or "latest").</summary>
    [RegularExpression("^(earliest|latest)$", ErrorMessage = "AutoOffsetReset must be 'earliest' or 'latest'.")]
    public string AutoOffsetReset { get; init; } = "earliest";

    /// <summary>Whether to enable idempotent production to prevent duplicate records.</summary>
    public bool EnableIdempotence { get; init; } = true;

    /// <summary>Strategy for extracting timestamps from records. Default uses the record's embedded timestamp.</summary>
    public ITimestampExtractor TimestampExtractor { get; init; } = RecordTimestampExtractor.Instance;

    /// <summary>Processing guarantee level. AtLeastOnce (default) or ExactlyOnce (uses transactions).</summary>
    public ProcessingGuarantee ProcessingGuarantee { get; init; } = ProcessingGuarantee.AtLeastOnce;

    /// <summary>
    /// Handler for deserialization exceptions. Default: LogAndFailDeserializationHandler.
    /// </summary>
    public IDeserializationExceptionHandler DeserializationExceptionHandler { get; init; }
        = new LogAndFailDeserializationHandler();

    /// <summary>
    /// Handler for production exceptions. Default: LogAndFailProductionHandler.
    /// </summary>
    public IProductionExceptionHandler ProductionExceptionHandler { get; init; }
        = new LogAndFailProductionHandler();

    /// <summary>
    /// Handler for processing exceptions. Default: LogAndFailProcessingHandler.
    /// </summary>
    public IProcessingExceptionHandler ProcessingExceptionHandler { get; init; }
        = new LogAndFailProcessingHandler();

    /// <summary>
    /// Handler for uncaught exceptions that kill a stream thread.
    /// Controls whether to replace the thread, shut down the client, or shut down the application.
    /// Default: ReplaceThread (most resilient).
    /// </summary>
    public IStreamsUncaughtExceptionHandler UncaughtExceptionHandler { get; init; }
        = LogAndReplaceUncaughtExceptionHandler.Instance;

    /// <summary>
    /// Prefix for transactional IDs when using ExactlyOnce processing guarantee.
    /// </summary>
    public string? TransactionalIdPrefix { get; init; }

    /// <summary>
    /// Transaction timeout in milliseconds for ExactlyOnce processing.
    /// </summary>
    [Range(1_000, int.MaxValue)]
    public int TransactionTimeoutMs { get; init; } = 60_000;

    /// <summary>Maximum records per second across all partitions. -1 disables rate limiting.</summary>
    public long MaxRecordsPerSecond { get; init; } = -1;

    /// <summary>Maximum bytes per second across all partitions. -1 disables rate limiting.</summary>
    public long MaxBytesPerSecond { get; init; } = -1;

    /// <summary>Maximum time in milliseconds to wait when rate-limited before dropping records.</summary>
    [Range(0, int.MaxValue)]
    public int MaxRateLimitWaitMs { get; init; } = 5000;

    /// <summary>Dead letter queue configuration for routing failed records.</summary>
    public DeadLetterQueueConfig DeadLetterQueue { get; init; } = DeadLetterQueueConfig.Disabled;

    /// <summary>Maximum time to wait for graceful shutdown to complete.</summary>
    public TimeSpan ShutdownTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Backpressure configuration for controlling flow when consumers fall behind.</summary>
    public BackpressureConfig Backpressure { get; init; } = new();

    /// <summary>Retry configuration for handling transient processing failures.</summary>
    public StreamsRetryConfig Retry { get; init; } = new();

    /// <summary>Record caching configuration. Caching reduces downstream updates for aggregations.</summary>
    public CachingConfig Caching { get; init; } = CachingConfig.Disabled;

    /// <summary>Listener for state store restoration progress events.</summary>
    public IStateRestoreListener StateRestoreListener { get; init; } = NoOpStateRestoreListener.Instance;

    /// <summary>Standby replica configuration for high-availability state stores.</summary>
    public StandbyConfig Standby { get; init; } = StandbyConfig.Disabled;

    /// <summary>Strategy for assigning partitions to stream threads. Default: round-robin.</summary>
    public IPartitionAssignor PartitionAssignor { get; init; } = RoundRobinAssignor.Instance;

    /// <summary>Whether to optimize the topology by merging repartition nodes.</summary>
    public bool OptimizeTopology { get; init; }

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        // Cross-property rules that the declarative attributes can't express
        if (ProcessingGuarantee == ProcessingGuarantee.ExactlyOnce && !EnableIdempotence)
        {
            errors.Add($"{nameof(EnableIdempotence)}: must be true when " +
                       $"{nameof(ProcessingGuarantee)} is ExactlyOnce.");
        }

        if (PollTimeout <= TimeSpan.Zero)
        {
            errors.Add($"{nameof(PollTimeout)}: must be positive.");
        }

        if (ShutdownTimeout <= TimeSpan.Zero)
        {
            errors.Add($"{nameof(ShutdownTimeout)}: must be positive.");
        }

        return errors;
    }
}

/// <summary>
/// Processing guarantee level.
/// </summary>
public enum ProcessingGuarantee
{
    /// <summary>
    /// At-least-once semantics. Records may be processed multiple times on failure.
    /// </summary>
    AtLeastOnce,

    /// <summary>
    /// Exactly-once semantics. Uses transactions for atomic processing.
    /// </summary>
    ExactlyOnce
}
