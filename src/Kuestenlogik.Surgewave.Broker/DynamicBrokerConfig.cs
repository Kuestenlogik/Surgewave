using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Event args for config change events.
/// </summary>
public sealed class ConfigChangedEventArgs : EventArgs
{
    public required string Name { get; init; }
    public string? OldValue { get; init; }
    public string? NewValue { get; init; }
}

/// <summary>
/// Manages dynamic broker configuration that can be modified at runtime via AlterConfigs API.
/// Dynamic configs take precedence over static configs from appsettings.json.
/// </summary>
public sealed class DynamicBrokerConfig
{
    private readonly ConcurrentDictionary<string, string> _dynamicConfigs = new();
    private readonly BrokerConfig _staticConfig;
    private readonly ILogger<DynamicBrokerConfig> _logger;
    private readonly string _configFilePath;

    /// <summary>
    /// Set of broker configs that can be modified at runtime.
    /// These are considered "dynamic" configs - changes take effect immediately without restart.
    /// </summary>
    public static readonly HashSet<string> DynamicConfigKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        // Network tuning (safe to change at runtime)
        "socket.send.buffer.bytes",
        "socket.receive.buffer.bytes",
        "max.connections.per.ip",

        // Log retention (changes apply to new segments)
        "log.retention.hours",
        "log.retention.ms",
        "log.retention.bytes",
        "log.segment.bytes",

        // Topic defaults (affect new topics)
        "num.partitions",
        "default.replication.factor",
        "auto.create.topics.enable",
        "min.insync.replicas",

        // Message limits
        "message.max.bytes",
        "replica.fetch.max.bytes",

        // Replication tuning
        "replica.lag.time.max.ms",
        "replica.lag.max.messages",
        "replica.fetch.wait.max.ms",

        // Controller/leader settings
        "auto.leader.rebalance.enable",
        "leader.imbalance.check.interval.seconds",

        // Quota settings
        "quota.producer.default",
        "quota.consumer.default",

        // Background thread pool sizes
        "background.threads",

        // Request handling
        "queued.max.requests",
        "request.timeout.ms",

        // Metadata
        "metadata.max.age.ms",

        // Compression
        "compression.type"
    };

    /// <summary>
    /// Read-only broker configs that require restart to change.
    /// </summary>
    public static readonly HashSet<string> ReadOnlyConfigKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        "broker.id",
        "node.id",
        "listeners",
        "advertised.listeners",
        "log.dirs",
        "log.dir",
        "cluster.id"
    };

    /// <summary>
    /// Event raised when a config is changed.
    /// </summary>
    public event EventHandler<ConfigChangedEventArgs>? ConfigChanged;

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    public DynamicBrokerConfig(BrokerConfig staticConfig, ILogger<DynamicBrokerConfig> logger)
    {
        _staticConfig = staticConfig;
        _logger = logger;
        _configFilePath = Path.Combine(staticConfig.DataDirectory, "dynamic-config.json");

        // Load persisted dynamic configs on startup
        LoadPersistedConfigs();
    }

    /// <summary>
    /// Get the effective value for a config, checking dynamic overrides first.
    /// </summary>
    public string? GetConfig(string name)
    {
        if (_dynamicConfigs.TryGetValue(name, out var dynamicValue))
        {
            return dynamicValue;
        }

        return GetStaticConfigValue(name);
    }

    /// <summary>
    /// Set a dynamic config value. Returns error message if config is read-only or invalid.
    /// </summary>
    public string? SetConfig(string name, string? value)
    {
        if (ReadOnlyConfigKeys.Contains(name))
        {
            return $"Config '{name}' is read-only and requires broker restart to change";
        }

        if (!DynamicConfigKeys.Contains(name))
        {
            return $"Config '{name}' is not a recognized broker config";
        }

        var validationError = ValidateConfigValue(name, value);
        if (validationError != null)
        {
            return validationError;
        }

        var oldValue = GetConfig(name);

        if (value == null)
        {
            // Remove dynamic override, revert to static config
            _dynamicConfigs.TryRemove(name, out _);
        }
        else
        {
            _dynamicConfigs[name] = value;
        }

        // Persist changes
        PersistConfigs();

        // Apply the change to the static config object where applicable
        ApplyConfigToStaticConfig(name, value);

        _logger.LogInformation("Broker config '{Name}' changed from '{OldValue}' to '{NewValue}'",
            name, oldValue, value);

        ConfigChanged?.Invoke(this, new ConfigChangedEventArgs { Name = name, OldValue = oldValue, NewValue = value });

        return null;
    }

    /// <summary>
    /// Get all dynamic config overrides.
    /// </summary>
    public IReadOnlyDictionary<string, string> GetDynamicConfigs()
    {
        return _dynamicConfigs.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
    }

    /// <summary>
    /// Check if a config has been dynamically overridden.
    /// </summary>
    public bool IsDynamicallySet(string name)
    {
        return _dynamicConfigs.ContainsKey(name);
    }

    private string? GetStaticConfigValue(string name)
    {
        return name.ToLowerInvariant() switch
        {
            "broker.id" or "node.id" => _staticConfig.BrokerId.ToString(),
            "socket.send.buffer.bytes" => _staticConfig.SocketSendBufferBytes.ToString(),
            "socket.receive.buffer.bytes" => _staticConfig.SocketReceiveBufferBytes.ToString(),
            "max.connections.per.ip" => _staticConfig.MaxConnectionsPerIp.ToString(),
            "log.dirs" or "log.dir" => _staticConfig.DataDirectory,
            "log.segment.bytes" => _staticConfig.LogSegmentBytes.ToString(),
            "log.retention.hours" => _staticConfig.LogRetentionHours.ToString(),
            "log.retention.ms" => (_staticConfig.LogRetentionHours == -1 ? -1 : _staticConfig.LogRetentionHours * 3600000L).ToString(),
            "log.retention.bytes" => _staticConfig.LogRetentionBytes.ToString(),
            "num.partitions" => _staticConfig.DefaultNumPartitions.ToString(),
            "default.replication.factor" => _staticConfig.DefaultReplicationFactor.ToString(),
            "auto.create.topics.enable" => _staticConfig.AutoCreateTopics.ToString().ToLowerInvariant(),
            "min.insync.replicas" => _staticConfig.MinInSyncReplicas.ToString(),
            "message.max.bytes" or "socket.request.max.bytes" => _staticConfig.MaxRequestSize.ToString(),
            "replica.fetch.max.bytes" => _staticConfig.ReplicaFetchMaxBytes.ToString(),
            "replica.lag.time.max.ms" => _staticConfig.ReplicaLagTimeMaxMs.ToString(),
            "replica.lag.max.messages" => _staticConfig.ReplicaLagMaxMessages.ToString(),
            "replica.fetch.wait.max.ms" => _staticConfig.ReplicaFetchWaitMaxMs.ToString(),
            "auto.leader.rebalance.enable" => _staticConfig.AllowAutoLeaderRebalance.ToString().ToLowerInvariant(),
            "leader.imbalance.check.interval.seconds" => _staticConfig.LeaderImbalanceCheckIntervalSeconds.ToString(),
            "quota.producer.default" => _staticConfig.Quotas.ProducerBytesPerSecond.ToString(),
            "quota.consumer.default" => _staticConfig.Quotas.ConsumerBytesPerSecond.ToString(),
            "cluster.id" => _staticConfig.ClusterId ?? "surgewave-cluster",
            "broker.rack" => _staticConfig.Rack ?? "",
            _ => null
        };
    }

    private string? ValidateConfigValue(string name, string? value)
    {
        if (value == null)
        {
            return null; // Null means "revert to default"
        }

        var lowerName = name.ToLowerInvariant();

        // Numeric validations
        if (lowerName.EndsWith(".bytes") || lowerName.EndsWith(".ms") ||
            lowerName.EndsWith(".seconds") || lowerName.EndsWith(".hours") ||
            lowerName == "num.partitions" || lowerName == "default.replication.factor" ||
            lowerName == "min.insync.replicas" || lowerName == "max.connections.per.ip" ||
            lowerName.EndsWith(".messages") || lowerName.EndsWith(".threads"))
        {
            if (!long.TryParse(value, out var numValue))
            {
                return $"Config '{name}' must be a numeric value";
            }

            // Validate specific ranges
            if (lowerName == "num.partitions" && numValue < 1)
            {
                return $"Config '{name}' must be at least 1";
            }
            if (lowerName == "default.replication.factor" && numValue < 1)
            {
                return $"Config '{name}' must be at least 1";
            }
            if (lowerName == "min.insync.replicas" && numValue < 1)
            {
                return $"Config '{name}' must be at least 1";
            }
            if (lowerName.EndsWith(".bytes") && numValue < 0 && numValue != -1)
            {
                return $"Config '{name}' must be non-negative or -1 (unlimited)";
            }
        }

        // Boolean validations
        if (lowerName.EndsWith(".enable") || lowerName.StartsWith("auto."))
        {
            if (!bool.TryParse(value, out _))
            {
                return $"Config '{name}' must be 'true' or 'false'";
            }
        }

        // Compression type validation
        if (lowerName == "compression.type")
        {
            var validTypes = new[] { "none", "gzip", "snappy", "lz4", "zstd", "producer" };
            if (!validTypes.Contains(value.ToLowerInvariant()))
            {
                return $"Config '{name}' must be one of: {string.Join(", ", validTypes)}";
            }
        }

        return null;
    }

    private void ApplyConfigToStaticConfig(string name, string? value)
    {
        if (value == null) return;

        var lowerName = name.ToLowerInvariant();

        // Apply changes that can be immediately reflected in BrokerConfig
        switch (lowerName)
        {
            case "socket.send.buffer.bytes":
                if (int.TryParse(value, out var sendBuffer))
                    _staticConfig.SocketSendBufferBytes = sendBuffer;
                break;
            case "socket.receive.buffer.bytes":
                if (int.TryParse(value, out var recvBuffer))
                    _staticConfig.SocketReceiveBufferBytes = recvBuffer;
                break;
            case "max.connections.per.ip":
                if (int.TryParse(value, out var maxConn))
                    _staticConfig.MaxConnectionsPerIp = maxConn;
                break;
            case "log.retention.hours":
                if (int.TryParse(value, out var retHours))
                    _staticConfig.LogRetentionHours = retHours;
                break;
            case "log.retention.bytes":
                if (long.TryParse(value, out var retBytes))
                    _staticConfig.LogRetentionBytes = retBytes;
                break;
            case "log.segment.bytes":
                if (long.TryParse(value, out var segBytes))
                    _staticConfig.LogSegmentBytes = segBytes;
                break;
            case "num.partitions":
                if (int.TryParse(value, out var numParts))
                    _staticConfig.DefaultNumPartitions = numParts;
                break;
            case "default.replication.factor":
                if (short.TryParse(value, out var repFactor))
                    _staticConfig.DefaultReplicationFactor = repFactor;
                break;
            case "auto.create.topics.enable":
                if (bool.TryParse(value, out var autoCreate))
                    _staticConfig.AutoCreateTopics = autoCreate;
                break;
            case "min.insync.replicas":
                if (int.TryParse(value, out var minIsr))
                    _staticConfig.MinInSyncReplicas = minIsr;
                break;
            case "message.max.bytes":
                if (int.TryParse(value, out var maxMsg))
                    _staticConfig.MaxRequestSize = maxMsg;
                break;
            case "replica.fetch.max.bytes":
                if (int.TryParse(value, out var fetchMax))
                    _staticConfig.ReplicaFetchMaxBytes = fetchMax;
                break;
            case "replica.lag.time.max.ms":
                if (int.TryParse(value, out var lagTime))
                    _staticConfig.ReplicaLagTimeMaxMs = lagTime;
                break;
            case "replica.lag.max.messages":
                if (long.TryParse(value, out var lagMsgs))
                    _staticConfig.ReplicaLagMaxMessages = lagMsgs;
                break;
            case "replica.fetch.wait.max.ms":
                if (int.TryParse(value, out var fetchWait))
                    _staticConfig.ReplicaFetchWaitMaxMs = fetchWait;
                break;
            case "auto.leader.rebalance.enable":
                if (bool.TryParse(value, out var autoRebal))
                    _staticConfig.AllowAutoLeaderRebalance = autoRebal;
                break;
            case "leader.imbalance.check.interval.seconds":
                if (int.TryParse(value, out var imbalCheck))
                    _staticConfig.LeaderImbalanceCheckIntervalSeconds = imbalCheck;
                break;
            case "quota.producer.default":
                if (long.TryParse(value, out var prodQuota))
                    _staticConfig.Quotas.ProducerBytesPerSecond = prodQuota;
                break;
            case "quota.consumer.default":
                if (long.TryParse(value, out var consQuota))
                    _staticConfig.Quotas.ConsumerBytesPerSecond = consQuota;
                break;
        }
    }

    private void LoadPersistedConfigs()
    {
        if (!File.Exists(_configFilePath))
        {
            return;
        }

        try
        {
            var json = File.ReadAllText(_configFilePath);
            var configs = JsonSerializer.Deserialize<Dictionary<string, string>>(json);

            if (configs != null)
            {
                foreach (var kvp in configs)
                {
                    if (DynamicConfigKeys.Contains(kvp.Key))
                    {
                        _dynamicConfigs[kvp.Key] = kvp.Value;
                        ApplyConfigToStaticConfig(kvp.Key, kvp.Value);
                    }
                }

                _logger.LogInformation("Loaded {Count} dynamic broker configs from {Path}",
                    configs.Count, _configFilePath);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load dynamic broker configs from {Path}", _configFilePath);
        }
    }

    private void PersistConfigs()
    {
        try
        {
            var directory = Path.GetDirectoryName(_configFilePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var json = JsonSerializer.Serialize(
                _dynamicConfigs.ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                JsonOptions);

            File.WriteAllText(_configFilePath, json);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to persist dynamic broker configs to {Path}", _configFilePath);
        }
    }
}
