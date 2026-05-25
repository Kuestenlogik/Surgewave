namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Limits record throughput using a token bucket algorithm.
/// Overflow records can be silently dropped or emitted as errors.
/// </summary>
[ConnectorMetadata(
    Name = "Rate Limiter",
    Description = "Limits record throughput using token bucket algorithm",
    Tags = "logic,rate-limit,throttle,backpressure")]
public sealed class RateLimiterNode : ProcessorConnector
{
    public override Type TaskClass => typeof(RateLimiterNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for rate-limited records")
        .Define("rate.limit", ConfigType.Long, "1000", Importance.High,
            "Maximum records per interval")
        .Define("rate.interval.ms", ConfigType.Long, "1000", Importance.Medium,
            "Interval in milliseconds")
        .Define("rate.burst", ConfigType.Long, "0", Importance.Medium,
            "Additional burst capacity (0 = no burst)")
        .Define("rate.overflow", ConfigType.String, "drop", Importance.Medium,
            "Overflow strategy: 'drop' (silent) or 'error' (emit error)");
}

internal sealed class RateLimiterNodeTask : ProcessorTask
{
    private long _rateLimit = 1000;
    private long _intervalMs = 1000;
    private long _burst;
    private string _overflow = "drop";

    private double _tokens;
    private DateTimeOffset _lastRefill;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        if (config.TryGetValue("rate.limit", out var rl) && long.TryParse(rl, out var rateLimit))
            _rateLimit = rateLimit;
        if (config.TryGetValue("rate.interval.ms", out var im) && long.TryParse(im, out var intervalMs))
            _intervalMs = intervalMs;
        if (config.TryGetValue("rate.burst", out var b) && long.TryParse(b, out var burst))
            _burst = burst;
        _overflow = config.TryGetValue("rate.overflow", out var ov) ? ov.ToLowerInvariant() : "drop";

        _tokens = _rateLimit + _burst;
        _lastRefill = DateTimeOffset.UtcNow;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            Refill();

            if (TryConsume())
            {
                var key = GetKeyString(record);
                var headers = ConvertHeaders(record.Headers);
                EmitRecord(key, record.Value, headers);
            }
            else
            {
                HandleOverflow(record);
            }
        }

        return Task.CompletedTask;
    }

    internal void Refill()
    {
        var now = DateTimeOffset.UtcNow;
        var elapsed = (now - _lastRefill).TotalMilliseconds;

        if (elapsed <= 0)
            return;

        var tokensToAdd = elapsed / _intervalMs * _rateLimit;
        var maxTokens = _rateLimit + _burst;
        _tokens = Math.Min(_tokens + tokensToAdd, maxTokens);
        _lastRefill = now;
    }

    internal bool TryConsume()
    {
        if (_tokens >= 1.0)
        {
            _tokens -= 1.0;
            return true;
        }

        return false;
    }

    private void HandleOverflow(SinkRecord record)
    {
        if (_overflow == "error")
        {
            EmitError(record, new InvalidOperationException("Rate limit exceeded"));
        }
        // "drop" = silently discard
    }

    /// <summary>
    /// Expose token count for testing.
    /// </summary>
    internal double CurrentTokens => _tokens;

    /// <summary>
    /// Set tokens directly for testing.
    /// </summary>
    internal void SetTokens(double tokens) => _tokens = tokens;
}
