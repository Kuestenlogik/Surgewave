using Kuestenlogik.Surgewave.Plugins;

namespace Kuestenlogik.Surgewave.Connect.Distributed;

/// <summary>
/// Aggregates connector types from local plugins and remote worker capabilities.
/// Provides a unified view of all available connector types across the cluster,
/// deduplicating by class name and tracking which workers offer which types.
/// </summary>
public sealed class AggregatedConnectorRegistry
{
    private readonly object _lock = new();
    private readonly Dictionary<string, PluginInfo> _localPlugins = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Dictionary<string, ConnectorCapability>> _remoteCapabilities = new(StringComparer.Ordinal);
    private readonly Dictionary<string, WorkerMetadata> _workerMetadata = new(StringComparer.Ordinal);

    /// <summary>
    /// Gets all aggregated connector types (local + remote, deduplicated by class name).
    /// </summary>
    public IReadOnlyList<AggregatedConnectorType> GetAllTypes()
    {
        lock (_lock)
        {
            var result = new Dictionary<string, AggregatedConnectorType>(StringComparer.Ordinal);

            // Add local plugins first (they have richer metadata)
            foreach (var (className, plugin) in _localPlugins)
            {
                var workerIds = GetWorkerIdsForType(className);

                result[className] = new AggregatedConnectorType
                {
                    ClassName = className,
                    Type = plugin.Type,
                    DisplayName = plugin.DisplayName,
                    Icon = plugin.Icon,
                    Category = plugin.Category,
                    Description = plugin.Description,
                    Version = plugin.Version,
                    AvailableOnWorkers = workerIds,
                    IsLocal = true
                };
            }

            // Add remote-only types (not already present as local)
            foreach (var (workerId, capabilities) in _remoteCapabilities)
            {
                foreach (var (className, capability) in capabilities)
                {
                    if (result.ContainsKey(className))
                        continue;

                    var workerIds = GetWorkerIdsForType(className);

                    result[className] = new AggregatedConnectorType
                    {
                        ClassName = className,
                        Type = capability.Type,
                        DisplayName = capability.DisplayName,
                        Version = capability.Version,
                        AvailableOnWorkers = workerIds,
                        IsLocal = false
                    };
                }
            }

            return result.Values.ToList();
        }
    }

    /// <summary>
    /// Gets the IDs of workers that can instantiate the specified connector type.
    /// </summary>
    public IReadOnlyList<string> GetWorkersForType(string className)
    {
        lock (_lock)
        {
            return GetWorkerIdsForType(className);
        }
    }

    /// <summary>
    /// Updates remote capabilities from a worker heartbeat.
    /// </summary>
    public void UpdateFromHeartbeat(string workerId, IReadOnlyList<ConnectorCapability> capabilities,
        IReadOnlyList<string>? tags = null, bool allowAutoInstall = false)
    {
        lock (_lock)
        {
            var workerCaps = new Dictionary<string, ConnectorCapability>(StringComparer.Ordinal);
            foreach (var cap in capabilities)
            {
                workerCaps[cap.ClassName] = cap;
            }
            _remoteCapabilities[workerId] = workerCaps;
            _workerMetadata[workerId] = new WorkerMetadata(tags ?? [], allowAutoInstall);
        }
    }

    /// <summary>
    /// Updates the local plugin registry from discovered plugins.
    /// </summary>
    public void UpdateFromLocalPlugins(IReadOnlyList<PluginInfo> plugins)
    {
        lock (_lock)
        {
            _localPlugins.Clear();
            foreach (var plugin in plugins)
            {
                _localPlugins[plugin.Class] = plugin;
            }
        }
    }

    /// <summary>
    /// Removes a worker's capabilities (e.g., when a worker leaves or times out).
    /// </summary>
    public void RemoveWorker(string workerId)
    {
        lock (_lock)
        {
            _remoteCapabilities.Remove(workerId);
            _workerMetadata.Remove(workerId);
        }
    }

    /// <summary>
    /// Gets workers that have all the specified tags.
    /// </summary>
    public IReadOnlyList<string> GetWorkersWithTags(IReadOnlyList<string> requiredTags)
    {
        lock (_lock)
        {
            var result = new List<string>();
            foreach (var (workerId, metadata) in _workerMetadata)
            {
                if (requiredTags.All(tag =>
                    metadata.Tags.Contains(tag, StringComparer.OrdinalIgnoreCase)))
                {
                    result.Add(workerId);
                }
            }
            return result;
        }
    }

    /// <summary>
    /// Gets workers that can instantiate the specified connector type AND have all required tags.
    /// </summary>
    public IReadOnlyList<string> GetWorkersForTypeWithTags(string className, IReadOnlyList<string>? requiredTags)
    {
        lock (_lock)
        {
            var workers = GetWorkerIdsForType(className);
            if (requiredTags == null || requiredTags.Count == 0)
                return workers;

            return workers.Where(workerId =>
                _workerMetadata.TryGetValue(workerId, out var meta) &&
                requiredTags.All(tag => meta.Tags.Contains(tag, StringComparer.OrdinalIgnoreCase)))
                .ToList();
        }
    }

    /// <summary>
    /// Gets the metadata for a specific worker.
    /// </summary>
    public WorkerMetadata? GetWorkerMetadata(string workerId)
    {
        lock (_lock)
        {
            return _workerMetadata.GetValueOrDefault(workerId);
        }
    }

    private List<string> GetWorkerIdsForType(string className)
    {
        var workerIds = new List<string>();
        foreach (var (workerId, capabilities) in _remoteCapabilities)
        {
            if (capabilities.ContainsKey(className))
            {
                workerIds.Add(workerId);
            }
        }
        return workerIds;
    }
}

/// <summary>
/// Metadata about a worker beyond its connector capabilities.
/// </summary>
public sealed record WorkerMetadata(IReadOnlyList<string> Tags, bool AllowAutoInstall);
