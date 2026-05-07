using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Gateway;

/// <summary>
/// Registry for managing multiple Surgewave cluster connections.
/// Provides cluster lookup by ID with support for a default cluster.
/// </summary>
public sealed class ClusterRegistry : IAsyncDisposable
{
    private readonly Dictionary<string, SurgewaveNativeClient> _clients;
    private readonly Dictionary<string, ClusterConfig> _configs;
    private readonly string _defaultCluster;
    private readonly ILogger<ClusterRegistry> _logger;

    public ClusterRegistry(
        GatewayConfig config,
        ILogger<ClusterRegistry> logger)
    {
        _logger = logger;
        _clients = new Dictionary<string, SurgewaveNativeClient>(StringComparer.OrdinalIgnoreCase);
        _configs = new Dictionary<string, ClusterConfig>(StringComparer.OrdinalIgnoreCase);

        // Multi-cluster configuration
        foreach (var (clusterId, clusterConfig) in config.Clusters)
        {
            _configs[clusterId] = clusterConfig;
        }
        _defaultCluster = config.DefaultCluster ?? _configs.Keys.FirstOrDefault() ?? "surgewave-cluster";
        _logger.LogInformation("Configured {Count} cluster(s), default: {DefaultCluster}",
            _configs.Count, _defaultCluster);

        // Create clients for each cluster
        foreach (var (clusterId, clusterConfig) in _configs)
        {
            var client = new SurgewaveNativeClient(
                clusterConfig.BrokerHost,
                clusterConfig.BrokerPort,
                clusterConfig.EnablePipelining);
            _clients[clusterId] = client;
            _logger.LogDebug("Created client for cluster {ClusterId}: {Host}:{Port}",
                clusterId, clusterConfig.BrokerHost, clusterConfig.BrokerPort);
        }
    }

    /// <summary>
    /// Gets the default cluster ID.
    /// </summary>
    public string DefaultClusterId => _defaultCluster;

    /// <summary>
    /// Gets all configured cluster IDs.
    /// </summary>
    public IEnumerable<string> ClusterIds => _clients.Keys;

    /// <summary>
    /// Gets the client for the specified cluster, or the default cluster if not specified.
    /// </summary>
    /// <param name="clusterId">The cluster ID, or null/empty for the default cluster.</param>
    /// <returns>The Surgewave native client for the cluster.</returns>
    /// <exception cref="KeyNotFoundException">Thrown when the cluster ID is not found.</exception>
    public SurgewaveNativeClient GetClient(string? clusterId)
    {
        var id = string.IsNullOrEmpty(clusterId) ? _defaultCluster : clusterId;

        if (!_clients.TryGetValue(id, out var client))
        {
            throw new KeyNotFoundException($"Unknown cluster: '{id}'. Available clusters: {string.Join(", ", _clients.Keys)}");
        }

        return client;
    }

    /// <summary>
    /// Tries to get the client for the specified cluster.
    /// </summary>
    /// <param name="clusterId">The cluster ID, or null/empty for the default cluster.</param>
    /// <param name="client">The Surgewave native client if found.</param>
    /// <returns>True if the cluster was found, false otherwise.</returns>
    public bool TryGetClient(string? clusterId, out SurgewaveNativeClient? client)
    {
        var id = string.IsNullOrEmpty(clusterId) ? _defaultCluster : clusterId;
        return _clients.TryGetValue(id, out client);
    }

    /// <summary>
    /// Gets the configuration for the specified cluster.
    /// </summary>
    public ClusterConfig? GetConfig(string? clusterId)
    {
        var id = string.IsNullOrEmpty(clusterId) ? _defaultCluster : clusterId;
        return _configs.GetValueOrDefault(id);
    }

    /// <summary>
    /// Gets the number of registered clusters.
    /// </summary>
    public int ClusterCount => _clients.Count;

    /// <summary>
    /// Connects all cluster clients.
    /// </summary>
    public async Task ConnectAllAsync(CancellationToken cancellationToken = default)
    {
        foreach (var (clusterId, client) in _clients)
        {
            _logger.LogInformation("Connecting to cluster {ClusterId}...", clusterId);
            await client.ConnectAsync(cancellationToken);
            _logger.LogInformation("Connected to cluster {ClusterId}", clusterId);
        }
    }

    /// <summary>
    /// Disposes all cluster clients.
    /// </summary>
    public async Task DisposeAllAsync()
    {
        foreach (var (clusterId, client) in _clients)
        {
            _logger.LogInformation("Disposing cluster {ClusterId}...", clusterId);
            await client.DisposeAsync();
            _logger.LogInformation("Disposed cluster {ClusterId}", clusterId);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeAllAsync();
    }
}
