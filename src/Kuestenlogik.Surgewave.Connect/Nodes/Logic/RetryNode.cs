namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Retries failed records with exponential backoff.
/// Records without error markers are passed through directly.
/// Records that exceed max retry attempts are emitted to the error topic.
/// </summary>
[ConnectorMetadata(
    Name = "Retry",
    Description = "Retries failed records with exponential backoff",
    Tags = "logic,retry,error,resilience")]
public sealed class RetryNode : ProcessorConnector
{
    public override Type TaskClass => typeof(RetryNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for successful records")
        .Define("retry.max.attempts", ConfigType.Int, "3", Importance.High,
            "Maximum number of retry attempts")
        .Define("retry.backoff.ms", ConfigType.Long, "1000", Importance.Medium,
            "Initial backoff in milliseconds")
        .Define("retry.backoff.multiplier", ConfigType.Double, "2.0", Importance.Medium,
            "Backoff multiplier for exponential increase")
        .Define("retry.max.backoff.ms", ConfigType.Long, "30000", Importance.Low,
            "Maximum backoff cap in milliseconds");
}

#pragma warning disable CA2213 // Timer is disposed in Stop()
internal sealed class RetryNodeTask : ProcessorTask
{
    private int _maxAttempts = 3;
    private long _backoffMs = 1000;
    private double _backoffMultiplier = 2.0;
    private long _maxBackoffMs = 30000;

    private readonly ConcurrentQueue<RetryEntry> _retryQueue = new();
    private Timer? _retryTimer;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        if (config.TryGetValue("retry.max.attempts", out var ma) && int.TryParse(ma, out var maxAttempts))
            _maxAttempts = maxAttempts;
        if (config.TryGetValue("retry.backoff.ms", out var bm) && long.TryParse(bm, out var backoffMs))
            _backoffMs = backoffMs;
        if (config.TryGetValue("retry.backoff.multiplier", out var mult) && double.TryParse(mult, CultureInfo.InvariantCulture, out var multiplier))
            _backoffMultiplier = multiplier;
        if (config.TryGetValue("retry.max.backoff.ms", out var mbm) && long.TryParse(mbm, out var maxBackoffMs))
            _maxBackoffMs = maxBackoffMs;

        _retryTimer = new Timer(OnRetryTimer, null, 100, 100);
    }

    public override void Stop()
    {
        _retryTimer?.Dispose();
        _retryTimer = null;
        while (_retryQueue.TryDequeue(out _)) { }
        base.Stop();
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (!HasErrorMarker(record))
            {
                // Pass-through: no error markers, emit directly
                var key = GetKeyString(record);
                var headers = ConvertHeaders(record.Headers);
                EmitRecord(key, record.Value, headers);
                continue;
            }

            var retryCount = GetRetryCount(record);

            if (retryCount >= _maxAttempts)
            {
                // Max retries exceeded — emit error
                EmitError(record, new InvalidOperationException(
                    $"Max retry attempts ({_maxAttempts}) exceeded after {retryCount} retries"));
                continue;
            }

            // Calculate backoff
            var backoff = (long)(_backoffMs * Math.Pow(_backoffMultiplier, retryCount));
            if (backoff > _maxBackoffMs)
                backoff = _maxBackoffMs;

            var nextRetryAt = DateTimeOffset.UtcNow.AddMilliseconds(backoff);

            _retryQueue.Enqueue(new RetryEntry(record, retryCount + 1, nextRetryAt));
        }

        return Task.CompletedTask;
    }

    internal void OnRetryTimer(object? state)
    {
        var now = DateTimeOffset.UtcNow;
        var requeue = new List<RetryEntry>();

        while (_retryQueue.TryDequeue(out var entry))
        {
            if (entry.NextRetryAt <= now)
            {
                // Re-emit with updated retry count header
                var key = GetKeyString(entry.Record);
                var headers = ConvertHeaders(entry.Record.Headers) ?? [];
                headers["_retry_count"] = entry.RetryCount.ToString(CultureInfo.InvariantCulture);
                headers["_retry_next_at"] = "";
                EmitRecord(key, entry.Record.Value, headers);
            }
            else
            {
                requeue.Add(entry);
            }
        }

        foreach (var entry in requeue)
        {
            _retryQueue.Enqueue(entry);
        }
    }

    private static bool HasErrorMarker(SinkRecord record)
    {
        if (record.Headers is null)
            return false;

        return record.Headers.ContainsKey("_error_message") ||
               record.Headers.ContainsKey("_error_type") ||
               record.Headers.ContainsKey("_retry_count");
    }

    private static int GetRetryCount(SinkRecord record)
    {
        if (record.Headers is null)
            return 0;

        if (record.Headers.TryGetValue("_retry_count", out var countBytes))
        {
            var countStr = Encoding.UTF8.GetString(countBytes);
            if (int.TryParse(countStr, out var count))
                return count;
        }

        return 0;
    }
}

internal sealed class RetryEntry(SinkRecord record, int retryCount, DateTimeOffset nextRetryAt)
{
    public SinkRecord Record { get; } = record;
    public int RetryCount { get; } = retryCount;
    public DateTimeOffset NextRetryAt { get; } = nextRetryAt;
}
