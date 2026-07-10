using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Quotas;

/// <summary>
/// Manages per-client and per-user bandwidth quotas.
/// Resolves the effective quota for a client (user override > client override > default),
/// tracks bandwidth via sliding window counters, and enforces throttling.
/// </summary>
public sealed partial class BandwidthQuotaManager : IDisposable, IBandwidthQuota
{
    private bool _disposed;
    private readonly BandwidthQuotaConfig _config;
    private readonly BandwidthTracker _tracker;
    private readonly ILogger<BandwidthQuotaManager> _logger;
    private readonly Timer _cleanupTimer;

    // Dynamic overrides (can be set at runtime via REST API)
    private readonly ConcurrentDictionary<string, ClientBandwidthQuota> _clientOverrides = new();
    private readonly ConcurrentDictionary<string, ClientBandwidthQuota> _userOverrides = new();

    // Metrics counters
    private long _totalBytesThrottled;
    private long _totalThrottleEvents;

    public BandwidthQuotaManager(BandwidthQuotaConfig config, ILogger<BandwidthQuotaManager> logger)
    {
        _config = config;
        _logger = logger;
        _tracker = new BandwidthTracker(config.EnforcementWindowMs);

        // Seed overrides from config
        foreach (var (clientId, quota) in config.ClientOverrides)
        {
            _clientOverrides[clientId] = quota;
        }

        foreach (var (user, quota) in config.UserOverrides)
        {
            _userOverrides[user] = quota;
        }

        // Cleanup inactive clients every 5 minutes
        _cleanupTimer = new Timer(CleanupInactive, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
    }

    /// <summary>
    /// Whether bandwidth quotas are enabled.
    /// </summary>
    public bool Enabled => _config.Enabled;

    /// <summary>
    /// Resolve the effective quota for a client, considering user overrides, client overrides, and defaults.
    /// User overrides take highest precedence, then client overrides, then defaults.
    /// </summary>
    public ClientBandwidthQuota GetQuota(string clientId, string? user = null)
    {
        // User override takes precedence
        if (user != null && _userOverrides.TryGetValue(user, out var userQuota))
            return userQuota;

        // Client override
        if (_clientOverrides.TryGetValue(clientId, out var clientQuota))
            return clientQuota;

        // Default
        return new ClientBandwidthQuota
        {
            ProduceBytesPerSec = _config.DefaultProduceBytesPerSec,
            ConsumeBytesPerSec = _config.DefaultConsumeBytesPerSec
        };
    }

    /// <summary>
    /// Set or update a bandwidth quota for a specific client ID.
    /// </summary>
    public void SetClientQuota(string clientId, ClientBandwidthQuota quota)
    {
        _clientOverrides[clientId] = quota;
        LogClientQuotaSet(clientId, quota.ProduceBytesPerSec, quota.ConsumeBytesPerSec);
    }

    /// <summary>
    /// Set or update a bandwidth quota for a specific user.
    /// </summary>
    public void SetUserQuota(string user, ClientBandwidthQuota quota)
    {
        _userOverrides[user] = quota;
        LogUserQuotaSet(user, quota.ProduceBytesPerSec, quota.ConsumeBytesPerSec);
    }

    /// <summary>
    /// Remove a client-specific bandwidth quota override.
    /// </summary>
    public bool RemoveClientQuota(string clientId)
    {
        var removed = _clientOverrides.TryRemove(clientId, out _);
        if (removed)
            LogClientQuotaRemoved(clientId);
        return removed;
    }

    /// <summary>
    /// Remove a user-specific bandwidth quota override.
    /// </summary>
    public bool RemoveUserQuota(string user)
    {
        var removed = _userOverrides.TryRemove(user, out _);
        if (removed)
            LogUserQuotaRemoved(user);
        return removed;
    }

    /// <summary>
    /// Check bandwidth and record bytes for a produce operation.
    /// Returns a throttle result indicating whether the request should be delayed.
    /// </summary>
    public ThrottleResult CheckAndRecordProduce(string clientId, string? user, long bytes)
    {
        if (!_config.Enabled)
            return new ThrottleResult(false, null, 0, 0);

        var quota = GetQuota(clientId, user);
        var result = _tracker.CheckProduce(clientId, bytes, quota.ProduceBytesPerSec, _config.ThrottleDelayFactor);

        // Always record the bytes (even if throttled, for accurate tracking)
        _tracker.RecordProduce(clientId, bytes);

        if (result.Throttled)
        {
            Interlocked.Add(ref _totalBytesThrottled, bytes);
            Interlocked.Increment(ref _totalThrottleEvents);
            LogProduceThrottled(clientId, result.CurrentBytesPerSec, result.LimitBytesPerSec, result.Delay?.TotalMilliseconds ?? 0);
        }

        return result;
    }

    /// <summary>
    /// Check bandwidth and record bytes for a consume (fetch) operation.
    /// Returns a throttle result indicating whether the request should be delayed.
    /// </summary>
    public ThrottleResult CheckAndRecordConsume(string clientId, string? user, long bytes)
    {
        if (!_config.Enabled)
            return new ThrottleResult(false, null, 0, 0);

        var quota = GetQuota(clientId, user);
        var result = _tracker.CheckConsume(clientId, bytes, quota.ConsumeBytesPerSec, _config.ThrottleDelayFactor);

        // Always record the bytes
        _tracker.RecordConsume(clientId, bytes);

        if (result.Throttled)
        {
            Interlocked.Add(ref _totalBytesThrottled, bytes);
            Interlocked.Increment(ref _totalThrottleEvents);
            LogConsumeThrottled(clientId, result.CurrentBytesPerSec, result.LimitBytesPerSec, result.Delay?.TotalMilliseconds ?? 0);
        }

        return result;
    }

    /// <summary>
    /// Check bandwidth for a consume (fetch) operation without recording bytes.
    /// Use this for pre-flight throttle checks where actual bytes are not yet known.
    /// </summary>
    public ThrottleResult CheckConsume(string clientId, string? user, long estimatedBytes)
    {
        if (!_config.Enabled)
            return new ThrottleResult(false, null, 0, 0);

        var quota = GetQuota(clientId, user);
        var result = _tracker.CheckConsume(clientId, estimatedBytes, quota.ConsumeBytesPerSec, _config.ThrottleDelayFactor);

        if (result.Throttled)
        {
            Interlocked.Increment(ref _totalThrottleEvents);
            LogConsumeThrottled(clientId, result.CurrentBytesPerSec, result.LimitBytesPerSec, result.Delay?.TotalMilliseconds ?? 0);
        }

        return result;
    }

    /// <summary>
    /// Record actual bytes consumed after a fetch operation completes.
    /// </summary>
    public void RecordConsume(string clientId, long actualBytes)
    {
        if (!_config.Enabled || actualBytes <= 0)
            return;

        _tracker.RecordConsume(clientId, actualBytes);
    }

    /// <summary>
    /// Get bandwidth usage for all tracked clients.
    /// </summary>
    public IReadOnlyList<BandwidthUsage> GetAllUsage()
    {
        return _tracker.GetAllUsage(clientId =>
        {
            var quota = GetQuota(clientId);
            return (quota.ProduceBytesPerSec, quota.ConsumeBytesPerSec);
        });
    }

    /// <summary>
    /// Get bandwidth usage for a specific client.
    /// </summary>
    public BandwidthUsage? GetClientUsage(string clientId)
    {
        var quota = GetQuota(clientId);
        return _tracker.GetUsage(clientId, quota.ProduceBytesPerSec, quota.ConsumeBytesPerSec);
    }

    /// <summary>
    /// Get aggregate bandwidth quota metrics.
    /// </summary>
    public BandwidthQuotaMetrics GetMetrics()
    {
        var allUsage = GetAllUsage();

        return new BandwidthQuotaMetrics
        {
            TotalClientsTracked = allUsage.Count,
            TotalClientsThrottled = allUsage.Count(u => u.IsThrottled),
            TotalBytesThrottled = Interlocked.Read(ref _totalBytesThrottled),
            TotalThrottleEvents = Interlocked.Read(ref _totalThrottleEvents)
        };
    }

    /// <summary>
    /// Get the current bandwidth quota configuration.
    /// </summary>
    public BandwidthQuotaConfig Config => _config;

    /// <summary>
    /// Get all client overrides.
    /// </summary>
    public IReadOnlyDictionary<string, ClientBandwidthQuota> ClientOverrides => _clientOverrides;

    /// <summary>
    /// Get all user overrides.
    /// </summary>
    public IReadOnlyDictionary<string, ClientBandwidthQuota> UserOverrides => _userOverrides;

    private void CleanupInactive(object? state)
    {
        var removed = _tracker.CleanupInactive(TimeSpan.FromMinutes(10));
        if (removed > 0)
            LogClientsCleanedUp(removed);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _cleanupTimer.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Bandwidth quota set for client '{ClientId}': produce={ProduceBps} B/s, consume={ConsumeBps} B/s")]
    private partial void LogClientQuotaSet(string clientId, long produceBps, long consumeBps);

    [LoggerMessage(Level = LogLevel.Information, Message = "Bandwidth quota set for user '{User}': produce={ProduceBps} B/s, consume={ConsumeBps} B/s")]
    private partial void LogUserQuotaSet(string user, long produceBps, long consumeBps);

    [LoggerMessage(Level = LogLevel.Information, Message = "Bandwidth quota removed for client '{ClientId}'")]
    private partial void LogClientQuotaRemoved(string clientId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Bandwidth quota removed for user '{User}'")]
    private partial void LogUserQuotaRemoved(string user);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Produce throttled for client '{ClientId}': current={CurrentBps} B/s, limit={LimitBps} B/s, delay={DelayMs}ms")]
    private partial void LogProduceThrottled(string clientId, long currentBps, long limitBps, double delayMs);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Consume throttled for client '{ClientId}': current={CurrentBps} B/s, limit={LimitBps} B/s, delay={DelayMs}ms")]
    private partial void LogConsumeThrottled(string clientId, long currentBps, long limitBps, double delayMs);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Cleaned up {Count} inactive bandwidth tracking states")]
    private partial void LogClientsCleanedUp(int count);
}
