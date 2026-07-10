using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Manages client quotas using token bucket algorithm.
/// Tracks produce and fetch byte rates per client and enforces limits.
/// Persists quota configuration to disk for restart survival.
/// </summary>
public sealed partial class QuotaManager : IDisposable, IQuotaManager
{
    private bool _disposed;
    private readonly QuotaConfig _config;
    private readonly ILogger<QuotaManager> _logger;
    private readonly ConcurrentDictionary<string, ClientQuotaState> _clientStates = new();
    private readonly Timer _cleanupTimer;
    private readonly string? _configFilePath;
    private readonly Lock _persistLock = new();

    public QuotaManager(QuotaConfig config, ILogger<QuotaManager> logger, string? dataDirectory = null)
    {
        _config = config;
        _logger = logger;

        // Setup persistence
        if (dataDirectory != null)
        {
            var configDir = Path.Combine(dataDirectory, ".metadata");
            Directory.CreateDirectory(configDir);
            _configFilePath = Path.Combine(configDir, "quotas.json");
            LoadPersistedConfig();
        }

        // Cleanup inactive clients every minute
        _cleanupTimer = new Timer(CleanupInactiveClients, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    /// <summary>
    /// Update quota configuration and persist to disk.
    /// </summary>
    public void UpdateConfig(
        bool? enabled = null,
        long? producerBytesPerSecond = null,
        long? producerBurstBytes = null,
        long? consumerBytesPerSecond = null,
        long? consumerBurstBytes = null,
        int? maxThrottleTimeMs = null,
        int? clientInactivityTimeoutMinutes = null)
    {
        if (enabled.HasValue) _config.Enabled = enabled.Value;
        if (producerBytesPerSecond.HasValue) _config.ProducerBytesPerSecond = producerBytesPerSecond.Value;
        if (producerBurstBytes.HasValue) _config.ProducerBurstBytes = producerBurstBytes.Value;
        if (consumerBytesPerSecond.HasValue) _config.ConsumerBytesPerSecond = consumerBytesPerSecond.Value;
        if (consumerBurstBytes.HasValue) _config.ConsumerBurstBytes = consumerBurstBytes.Value;
        if (maxThrottleTimeMs.HasValue) _config.MaxThrottleTimeMs = maxThrottleTimeMs.Value;
        if (clientInactivityTimeoutMinutes.HasValue) _config.ClientInactivityTimeoutMinutes = clientInactivityTimeoutMinutes.Value;

        PersistConfig();
    }

    /// <summary>
    /// Get a copy of the current configuration.
    /// </summary>
    public QuotaConfig Config => new()
    {
        Enabled = _config.Enabled,
        ProducerBytesPerSecond = _config.ProducerBytesPerSecond,
        ProducerBurstBytes = _config.ProducerBurstBytes,
        ConsumerBytesPerSecond = _config.ConsumerBytesPerSecond,
        ConsumerBurstBytes = _config.ConsumerBurstBytes,
        MaxThrottleTimeMs = _config.MaxThrottleTimeMs,
        ClientInactivityTimeoutMinutes = _config.ClientInactivityTimeoutMinutes
    };

    private void LoadPersistedConfig()
    {
        if (_configFilePath == null || !File.Exists(_configFilePath))
            return;

        try
        {
            var json = File.ReadAllText(_configFilePath);
            var persisted = JsonSerializer.Deserialize(json, BrokerJsonContext.Default.PersistedQuotaConfig);
            if (persisted != null)
            {
                _config.Enabled = persisted.Enabled;
                _config.ProducerBytesPerSecond = persisted.ProducerBytesPerSecond;
                _config.ProducerBurstBytes = persisted.ProducerBurstBytes;
                _config.ConsumerBytesPerSecond = persisted.ConsumerBytesPerSecond;
                _config.ConsumerBurstBytes = persisted.ConsumerBurstBytes;
                _config.MaxThrottleTimeMs = persisted.MaxThrottleTimeMs;
                _config.ClientInactivityTimeoutMinutes = persisted.ClientInactivityTimeoutMinutes;
                LogConfigLoaded();
            }
        }
        catch (Exception ex)
        {
            LogConfigLoadError(ex.Message);
        }
    }

    private void PersistConfig()
    {
        if (_configFilePath == null) return;

        lock (_persistLock)
        {
            try
            {
                var persisted = new PersistedQuotaConfig
                {
                    Enabled = _config.Enabled,
                    ProducerBytesPerSecond = _config.ProducerBytesPerSecond,
                    ProducerBurstBytes = _config.ProducerBurstBytes,
                    ConsumerBytesPerSecond = _config.ConsumerBytesPerSecond,
                    ConsumerBurstBytes = _config.ConsumerBurstBytes,
                    MaxThrottleTimeMs = _config.MaxThrottleTimeMs,
                    ClientInactivityTimeoutMinutes = _config.ClientInactivityTimeoutMinutes,
                    LastModified = DateTimeOffset.UtcNow
                };
                var json = JsonSerializer.Serialize(persisted, BrokerJsonContext.Default.PersistedQuotaConfig);
                File.WriteAllText(_configFilePath, json);
                LogConfigPersisted();
            }
            catch (Exception ex)
            {
                LogConfigPersistError(ex.Message);
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded persisted quota configuration")]
    private partial void LogConfigLoaded();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to load quota config: {Message}")]
    private partial void LogConfigLoadError(string message);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Persisted quota configuration")]
    private partial void LogConfigPersisted();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to persist quota config: {Message}")]
    private partial void LogConfigPersistError(string message);

    /// <summary>
    /// Check if a produce request should be throttled.
    /// Returns throttle time in milliseconds (0 if not throttled).
    /// </summary>
    public int CheckProduceQuota(string clientId, long bytesToProduce)
    {
        if (!_config.Enabled || _config.ProducerBytesPerSecond <= 0)
        {
            return 0;
        }

        var state = GetOrCreateClientState(clientId);
        return state.CheckAndConsumeProduceTokens(bytesToProduce, _config.ProducerBytesPerSecond);
    }

    /// <summary>
    /// Check if a fetch request should be throttled.
    /// Returns throttle time in milliseconds (0 if not throttled).
    /// </summary>
    public int CheckFetchQuota(string clientId, long bytesToFetch)
    {
        if (!_config.Enabled || _config.ConsumerBytesPerSecond <= 0)
        {
            return 0;
        }

        var state = GetOrCreateClientState(clientId);
        return state.CheckAndConsumeFetchTokens(bytesToFetch, _config.ConsumerBytesPerSecond);
    }

    /// <summary>
    /// Record actual bytes produced (for tracking after successful produce).
    /// </summary>
    public void RecordProducedBytes(string clientId, long bytes)
    {
        if (!_config.Enabled) return;

        var state = GetOrCreateClientState(clientId);
        state.RecordProducedBytes(bytes);
    }

    /// <summary>
    /// Record actual bytes fetched (for tracking after successful fetch).
    /// </summary>
    public void RecordFetchedBytes(string clientId, long bytes)
    {
        if (!_config.Enabled) return;

        var state = GetOrCreateClientState(clientId);
        state.RecordFetchedBytes(bytes);
    }

    /// <summary>
    /// Get quota statistics for a client.
    /// </summary>
    public ClientQuotaStats? GetClientStats(string clientId)
    {
        if (_clientStates.TryGetValue(clientId, out var state))
        {
            return state.GetStats();
        }
        return null;
    }

    /// <summary>
    /// Get all client quota statistics.
    /// </summary>
    public IEnumerable<(string ClientId, ClientQuotaStats Stats)> GetAllClientStats()
    {
        foreach (var (clientId, state) in _clientStates)
        {
            yield return (clientId, state.GetStats());
        }
    }

    private ClientQuotaState GetOrCreateClientState(string clientId)
    {
        return _clientStates.GetOrAdd(clientId, _ => new ClientQuotaState(_config));
    }

    private void CleanupInactiveClients(object? state)
    {
        var cutoff = DateTime.UtcNow.AddMinutes(-_config.ClientInactivityTimeoutMinutes);
        var removed = 0;

        foreach (var (clientId, clientState) in _clientStates)
        {
            if (clientState.LastActivity < cutoff)
            {
                if (_clientStates.TryRemove(clientId, out _))
                {
                    removed++;
                }
            }
        }

        if (removed > 0)
        {
            LogClientsCleanedUp(removed);
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Cleaned up {Count} inactive quota client states")]
    private partial void LogClientsCleanedUp(int count);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _cleanupTimer.Dispose();
    }
}

/// <summary>
/// Per-client quota state using token bucket algorithm.
/// </summary>
internal sealed class ClientQuotaState
{
    private readonly QuotaConfig _config;
    private readonly Lock _lock = new();

    // Token buckets
    private double _produceTokens;
    private double _fetchTokens;
    private DateTime _lastProduceRefill;
    private DateTime _lastFetchRefill;

    // Statistics
    private long _totalProducedBytes;
    private long _totalFetchedBytes;
    private int _produceThrottleCount;
    private int _fetchThrottleCount;

    public DateTime LastActivity { get; private set; }

    public ClientQuotaState(QuotaConfig config)
    {
        _config = config;
        _produceTokens = config.ProducerBurstBytes;
        _fetchTokens = config.ConsumerBurstBytes;
        _lastProduceRefill = DateTime.UtcNow;
        _lastFetchRefill = DateTime.UtcNow;
        LastActivity = DateTime.UtcNow;
    }

    public int CheckAndConsumeProduceTokens(long bytes, long bytesPerSecond)
    {
        lock (_lock)
        {
            LastActivity = DateTime.UtcNow;
            RefillProduceTokens(bytesPerSecond);

            if (_produceTokens >= bytes)
            {
                _produceTokens -= bytes;
                return 0; // No throttle
            }

            // Calculate throttle time
            var deficit = bytes - _produceTokens;
            var throttleMs = (int)Math.Ceiling(deficit * 1000.0 / bytesPerSecond);

            // Cap throttle time
            throttleMs = Math.Min(throttleMs, _config.MaxThrottleTimeMs);

            _produceThrottleCount++;
            return throttleMs;
        }
    }

    public int CheckAndConsumeFetchTokens(long bytes, long bytesPerSecond)
    {
        lock (_lock)
        {
            LastActivity = DateTime.UtcNow;
            RefillFetchTokens(bytesPerSecond);

            if (_fetchTokens >= bytes)
            {
                _fetchTokens -= bytes;
                return 0; // No throttle
            }

            // Calculate throttle time
            var deficit = bytes - _fetchTokens;
            var throttleMs = (int)Math.Ceiling(deficit * 1000.0 / bytesPerSecond);

            // Cap throttle time
            throttleMs = Math.Min(throttleMs, _config.MaxThrottleTimeMs);

            _fetchThrottleCount++;
            return throttleMs;
        }
    }

    public void RecordProducedBytes(long bytes)
    {
        lock (_lock)
        {
            _totalProducedBytes += bytes;
            LastActivity = DateTime.UtcNow;
        }
    }

    public void RecordFetchedBytes(long bytes)
    {
        lock (_lock)
        {
            _totalFetchedBytes += bytes;
            LastActivity = DateTime.UtcNow;
        }
    }

    public ClientQuotaStats GetStats()
    {
        lock (_lock)
        {
            return new ClientQuotaStats
            {
                TotalProducedBytes = _totalProducedBytes,
                TotalFetchedBytes = _totalFetchedBytes,
                ProduceThrottleCount = _produceThrottleCount,
                FetchThrottleCount = _fetchThrottleCount,
                AvailableProduceTokens = (long)_produceTokens,
                AvailableFetchTokens = (long)_fetchTokens,
                LastActivity = LastActivity
            };
        }
    }

    private void RefillProduceTokens(long bytesPerSecond)
    {
        var now = DateTime.UtcNow;
        var elapsed = (now - _lastProduceRefill).TotalSeconds;

        if (elapsed > 0)
        {
            var newTokens = elapsed * bytesPerSecond;
            _produceTokens = Math.Min(_produceTokens + newTokens, _config.ProducerBurstBytes);
            _lastProduceRefill = now;
        }
    }

    private void RefillFetchTokens(long bytesPerSecond)
    {
        var now = DateTime.UtcNow;
        var elapsed = (now - _lastFetchRefill).TotalSeconds;

        if (elapsed > 0)
        {
            var newTokens = elapsed * bytesPerSecond;
            _fetchTokens = Math.Min(_fetchTokens + newTokens, _config.ConsumerBurstBytes);
            _lastFetchRefill = now;
        }
    }
}

/// <summary>
/// Persisted quota configuration structure for JSON serialization.
/// </summary>
internal sealed class PersistedQuotaConfig
{
    public bool Enabled { get; set; }
    public long ProducerBytesPerSecond { get; set; }
    public long ProducerBurstBytes { get; set; }
    public long ConsumerBytesPerSecond { get; set; }
    public long ConsumerBurstBytes { get; set; }
    public int MaxThrottleTimeMs { get; set; }
    public int ClientInactivityTimeoutMinutes { get; set; }
    public DateTimeOffset LastModified { get; set; }
}
