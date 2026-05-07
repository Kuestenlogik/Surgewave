using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kuestenlogik.Surgewave.Streams.Monitoring;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Metrics for streams processing.
/// Provides both atomic Interlocked counters (backward-compat) and OpenTelemetry Meter/ActivitySource.
/// </summary>
public sealed class StreamsMetrics : IDisposable
{
    private long _processedRecords;
    private long _processedBytes;
    private long _lastProcessedTimestamp;
    private long _deserializationErrors;
    private long _productionErrors;
    private long _processingErrors;
    private long _lateRecords;
    private long _rateLimitThrottles;
    private long _rateLimitWaitTimeMs;
    private long _currentLag;
    private long _dlqMessages;
    private bool _disposed;

    // OpenTelemetry instrumentation
    public Meter Meter { get; }
    public ActivitySource ActivitySource { get; }

    private readonly Counter<long> _otelRecordsProcessed;
    private readonly Counter<long> _otelBytesProcessed;
    private readonly Counter<long> _otelErrors;
    private readonly Counter<long> _otelLateRecords;
    private readonly Counter<long> _otelRateLimitThrottles;
    private readonly Counter<long> _otelDlqMessages;
    private readonly Histogram<double> _otelProcessingLatency;
    private readonly Counter<long> _otelBackpressureDropped;
    private readonly Counter<long> _otelRetries;
    private readonly Counter<long> _otelRetriesExhausted;

    // Per-node and per-store metrics registries
    private readonly ConcurrentDictionary<string, ProcessorNodeMetrics> _nodeMetrics = new();
    private readonly ConcurrentDictionary<string, StateStoreMetrics> _storeMetrics = new();

    private long _backpressureDropped;
    private long _backpressureBufferSize;
    private long _highWatermarkHits;
    private long _lowWatermarkResumes;
    private long _consumerPauses;
    private long _consumerResumes;
    private long _retries;
    private long _retriesExhausted;

    // Interlocked counters (backward-compat)
    public long ProcessedRecords => _processedRecords;
    public long ProcessedBytes => _processedBytes;
    public long LastProcessedTimestamp => _lastProcessedTimestamp;
    public long DeserializationErrors => _deserializationErrors;
    public long ProductionErrors => _productionErrors;
    public long ProcessingErrors => _processingErrors;
    public long LateRecords => _lateRecords;
    public long RateLimitThrottles => _rateLimitThrottles;
    public long RateLimitWaitTimeMs => _rateLimitWaitTimeMs;
    public long CurrentLag => _currentLag;
    public long DlqMessages => _dlqMessages;

    private long _storesRestored;
    private long _recordsRestored;

    private long _activeStreamThreads;
    private long _uncaughtExceptions;
    private long _threadReplacements;

    public long StoresRestored => _storesRestored;
    public long RecordsRestored => _recordsRestored;

    public long ActiveStreamThreads => _activeStreamThreads;
    public long UncaughtExceptions => _uncaughtExceptions;
    public long ThreadReplacements => _threadReplacements;

    public StreamsMetrics()
    {
        Meter = new Meter("Kuestenlogik.Surgewave.Streams");
        ActivitySource = new ActivitySource("Kuestenlogik.Surgewave.Streams");

        _otelRecordsProcessed = Meter.CreateCounter<long>(
            "surgewave_streams_records_processed_total",
            description: "Total number of records processed");

        _otelBytesProcessed = Meter.CreateCounter<long>(
            "surgewave_streams_bytes_processed_total",
            unit: "By",
            description: "Total bytes processed");

        _otelErrors = Meter.CreateCounter<long>(
            "surgewave_streams_errors_total",
            description: "Total number of processing errors");

        _otelLateRecords = Meter.CreateCounter<long>(
            "surgewave_streams_late_records_total",
            description: "Total number of late records dropped");

        _otelRateLimitThrottles = Meter.CreateCounter<long>(
            "surgewave_streams_rate_limit_throttles_total",
            description: "Total rate limit throttle events");

        _otelDlqMessages = Meter.CreateCounter<long>(
            "surgewave_streams_dlq_messages_total",
            description: "Total messages sent to dead letter queue");

        _otelProcessingLatency = Meter.CreateHistogram<double>(
            "surgewave_streams_processing_latency_ms",
            unit: "ms",
            description: "Record processing latency in milliseconds");

        _otelBackpressureDropped = Meter.CreateCounter<long>(
            "surgewave_streams_backpressure_dropped_total",
            description: "Total records dropped due to backpressure");

        Meter.CreateObservableGauge(
            "surgewave_streams_backpressure_buffer_size",
            () => Interlocked.Read(ref _backpressureBufferSize),
            description: "Current backpressure buffer depth");

        _otelRetries = Meter.CreateCounter<long>(
            "surgewave_streams_retries_total",
            description: "Total retry attempts");

        _otelRetriesExhausted = Meter.CreateCounter<long>(
            "surgewave_streams_retries_exhausted_total",
            description: "Total retry exhaustions");

        Meter.CreateObservableGauge(
            "surgewave_streams_active_threads",
            () => Interlocked.Read(ref _activeStreamThreads),
            description: "Number of active stream threads");

        Meter.CreateObservableGauge(
            "surgewave_streams_stores_restored_total",
            () => Interlocked.Read(ref _storesRestored),
            description: "Total number of state stores restored from changelog");

        Meter.CreateObservableGauge(
            "surgewave_streams_records_restored_total",
            () => Interlocked.Read(ref _recordsRestored),
            description: "Total number of records restored from changelog");
    }

    public void RecordProcessed(int bytes)
    {
        Interlocked.Increment(ref _processedRecords);
        Interlocked.Add(ref _processedBytes, bytes);
        // Use Environment.TickCount64 instead of DateTimeOffset.UtcNow (10x faster, monotonic)
        Interlocked.Exchange(ref _lastProcessedTimestamp, Environment.TickCount64);

        _otelRecordsProcessed.Add(1);
        _otelBytesProcessed.Add(bytes);
    }

    public void RecordDeserializationError()
    {
        Interlocked.Increment(ref _deserializationErrors);
        _otelErrors.Add(1, new KeyValuePair<string, object?>("error.type", "deserialization"));
    }

    public void RecordProductionError()
    {
        Interlocked.Increment(ref _productionErrors);
        _otelErrors.Add(1, new KeyValuePair<string, object?>("error.type", "production"));
    }

    public void RecordProcessingError()
    {
        Interlocked.Increment(ref _processingErrors);
        _otelErrors.Add(1, new KeyValuePair<string, object?>("error.type", "processing"));
    }

    public void RecordLateRecord()
    {
        Interlocked.Increment(ref _lateRecords);
        _otelLateRecords.Add(1);
    }

    public void RecordProcessingLatency(double latencyMs)
    {
        _otelProcessingLatency.Record(latencyMs);
    }

    public void RecordRateLimitThrottle(int waitMs)
    {
        Interlocked.Increment(ref _rateLimitThrottles);
        Interlocked.Add(ref _rateLimitWaitTimeMs, waitMs);
        _otelRateLimitThrottles.Add(1);
    }

    public void UpdateLag(long lag)
    {
        Interlocked.Exchange(ref _currentLag, lag);
    }

    public void RecordDlqMessage()
    {
        Interlocked.Increment(ref _dlqMessages);
        _otelDlqMessages.Add(1);
    }

    public ProcessorNodeMetrics GetOrCreateNodeMetrics(string nodeName)
    {
        return _nodeMetrics.GetOrAdd(nodeName, name => new ProcessorNodeMetrics(Meter, name));
    }

    public StateStoreMetrics GetOrCreateStoreMetrics(string storeName, Func<long>? entryCountProvider = null)
    {
        return _storeMetrics.GetOrAdd(storeName, name => new StateStoreMetrics(Meter, name, entryCountProvider));
    }

    public IReadOnlyDictionary<string, ProcessorNodeMetrics> NodeMetrics => _nodeMetrics;
    public IReadOnlyDictionary<string, StateStoreMetrics> StoreMetrics => _storeMetrics;

    public long BackpressureDropped => _backpressureDropped;
    public long BackpressureBufferSize => _backpressureBufferSize;
    public long HighWatermarkHits => _highWatermarkHits;
    public long LowWatermarkResumes => _lowWatermarkResumes;
    public long ConsumerPauses => _consumerPauses;
    public long ConsumerResumes => _consumerResumes;
    public long Retries => _retries;
    public long RetriesExhausted => _retriesExhausted;

    public void RecordBackpressureDrop()
    {
        Interlocked.Increment(ref _backpressureDropped);
        _otelBackpressureDropped.Add(1);
    }

    /// <summary>Records that the buffer high watermark was reached.</summary>
    public void RecordHighWatermarkHit() => Interlocked.Increment(ref _highWatermarkHits);

    /// <summary>Records that the buffer dropped below the low watermark and consumption can resume.</summary>
    public void RecordLowWatermarkResume() => Interlocked.Increment(ref _lowWatermarkResumes);

    /// <summary>Records that a consumer partition was paused due to backpressure.</summary>
    public void RecordConsumerPause() => Interlocked.Increment(ref _consumerPauses);

    /// <summary>Records that a consumer partition was resumed after backpressure cleared.</summary>
    public void RecordConsumerResume() => Interlocked.Increment(ref _consumerResumes);

    public void UpdateBackpressureBufferSize(long size)
    {
        Interlocked.Exchange(ref _backpressureBufferSize, size);
    }

    public void RecordRetry()
    {
        Interlocked.Increment(ref _retries);
        _otelRetries.Add(1);
    }

    public void RecordRetryExhausted()
    {
        Interlocked.Increment(ref _retriesExhausted);
        _otelRetriesExhausted.Add(1);
    }

    public void IncrementActiveThreads() => Interlocked.Increment(ref _activeStreamThreads);
    public void DecrementActiveThreads() => Interlocked.Decrement(ref _activeStreamThreads);

    public void RecordUncaughtException()
    {
        Interlocked.Increment(ref _uncaughtExceptions);
        _otelErrors.Add(1, new KeyValuePair<string, object?>("error.type", "uncaught"));
    }

    public void RecordThreadReplacement()
    {
        Interlocked.Increment(ref _threadReplacements);
    }

    public void RecordStoreRestored() => Interlocked.Increment(ref _storesRestored);
    public void RecordRestoredRecords(long count) => Interlocked.Add(ref _recordsRestored, count);

    /// <summary>
    /// Starts an activity for processing a single record.
    /// </summary>
    public Activity? StartProcessRecordActivity(string topic, int partition, long offset, string applicationId)
    {
        var activity = ActivitySource.StartActivity("surgewave.streams.process_record", ActivityKind.Consumer);
        if (activity != null)
        {
            activity.SetTag("messaging.system", "surgewave");
            activity.SetTag("messaging.destination.name", topic);
            activity.SetTag("messaging.destination.partition.id", partition);
            activity.SetTag("messaging.message.offset", offset);
            activity.SetTag("surgewave.streams.application.id", applicationId);
        }
        return activity;
    }

    /// <summary>
    /// Starts an activity for a processor node.
    /// </summary>
    public Activity? StartProcessorNodeActivity(string nodeName)
    {
        var activity = ActivitySource.StartActivity($"surgewave.streams.node.{nodeName}", ActivityKind.Internal);
        activity?.SetTag("surgewave.streams.node.name", nodeName);
        return activity;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        Meter.Dispose();
        ActivitySource.Dispose();
        _disposed = true;
    }
}
