using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Manages the cluster ID with auto-generation and persistence.
/// The cluster ID is a unique identifier for the Surgewave cluster that:
/// - Is auto-generated on first startup if not configured
/// - Is persisted to disk so it survives restarts
/// - Must match across all brokers in the cluster
/// </summary>
public sealed class ClusterIdManager
{
    private const string ClusterIdFileName = "cluster.id";

    private readonly ClusteringConfig _config;
    private readonly ILogger<ClusterIdManager> _logger;
    private string? _clusterId;
    private readonly object _lock = new();

    public ClusterIdManager(ClusteringConfig config, ILogger<ClusterIdManager> logger)
    {
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Gets the cluster ID, initializing it if necessary.
    /// Thread-safe and idempotent.
    /// </summary>
    public string GetClusterId()
    {
        if (_clusterId != null)
            return _clusterId;

        lock (_lock)
        {
            if (_clusterId != null)
                return _clusterId;

            _clusterId = InitializeClusterId();
            return _clusterId;
        }
    }

    /// <summary>
    /// Validates that a given cluster ID matches this cluster.
    /// </summary>
    public bool ValidateClusterId(string? incomingClusterId)
    {
        if (string.IsNullOrEmpty(incomingClusterId))
            return true; // Empty cluster IDs are allowed (backwards compatibility)

        var myClusterId = GetClusterId();
        return string.Equals(myClusterId, incomingClusterId, StringComparison.Ordinal);
    }

    private string InitializeClusterId()
    {
        // Priority 1: Use configured cluster ID from appsettings
        if (!string.IsNullOrEmpty(_config.ClusterId))
        {
            _logger.LogInformation("Using configured cluster ID: {ClusterId}", _config.ClusterId);

            // Still persist it if not already persisted
            PersistClusterIdIfNeeded(_config.ClusterId);
            return _config.ClusterId;
        }

        // Priority 2: Load from persisted file
        var persistedId = LoadPersistedClusterId();
        if (!string.IsNullOrEmpty(persistedId))
        {
            _logger.LogInformation("Loaded persisted cluster ID: {ClusterId}", persistedId);
            return persistedId;
        }

        // Priority 3: Generate new cluster ID
        var newClusterId = GenerateClusterId();
        _logger.LogInformation("Generated new cluster ID: {ClusterId}", newClusterId);

        PersistClusterId(newClusterId);
        return newClusterId;
    }

    private string? LoadPersistedClusterId()
    {
        try
        {
            var clusterIdPath = GetClusterIdFilePath();
            if (!File.Exists(clusterIdPath))
                return null;

            var clusterId = File.ReadAllText(clusterIdPath).Trim();
            if (string.IsNullOrEmpty(clusterId))
                return null;

            // Validate format
            if (!IsValidClusterIdFormat(clusterId))
            {
                _logger.LogWarning("Invalid cluster ID format in persisted file, will generate new one");
                return null;
            }

            return clusterId;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load persisted cluster ID, will generate new one");
            return null;
        }
    }

    private void PersistClusterIdIfNeeded(string clusterId)
    {
        try
        {
            var clusterIdPath = GetClusterIdFilePath();
            if (File.Exists(clusterIdPath))
            {
                var existing = File.ReadAllText(clusterIdPath).Trim();
                if (existing == clusterId)
                    return;

                // Cluster ID mismatch - this is a serious error
                if (!string.IsNullOrEmpty(existing))
                {
                    _logger.LogError(
                        "Cluster ID mismatch! Configured: {Configured}, Persisted: {Persisted}. " +
                        "This may indicate data corruption or misconfiguration.",
                        clusterId, existing);
                    throw new InvalidOperationException(
                        $"Cluster ID mismatch. Configured: {clusterId}, Persisted: {existing}. " +
                        "Delete the cluster.id file if you intentionally changed the cluster ID.");
                }
            }

            PersistClusterId(clusterId);
        }
        catch (InvalidOperationException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to check/persist cluster ID");
        }
    }

    private void PersistClusterId(string clusterId)
    {
        try
        {
            var clusterIdPath = GetClusterIdFilePath();
            var directory = Path.GetDirectoryName(clusterIdPath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            File.WriteAllText(clusterIdPath, clusterId);
            _logger.LogDebug("Persisted cluster ID to {Path}", clusterIdPath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to persist cluster ID to disk");
            throw;
        }
    }

    private string GetClusterIdFilePath()
    {
        return Path.Combine(_config.DataDirectory, ClusterIdFileName);
    }

    /// <summary>
    /// Generates a new cluster ID in Kafka-compatible format.
    /// Format: Base64-encoded UUID without padding, similar to Kafka's cluster ID.
    /// </summary>
    private static string GenerateClusterId()
    {
        // Generate a v4 UUID and encode as Base64 without padding
        // This matches Kafka's cluster ID format
        var uuid = Guid.NewGuid();
        var bytes = uuid.ToByteArray();
        return Convert.ToBase64String(bytes).TrimEnd('=');
    }

    /// <summary>
    /// Validates that a cluster ID has a valid format.
    /// </summary>
    private static bool IsValidClusterIdFormat(string clusterId)
    {
        if (string.IsNullOrWhiteSpace(clusterId))
            return false;

        // Allow any non-empty string for flexibility
        // Kafka uses Base64-encoded UUIDs but we're lenient
        return clusterId.Length >= 1 && clusterId.Length <= 256;
    }
}
