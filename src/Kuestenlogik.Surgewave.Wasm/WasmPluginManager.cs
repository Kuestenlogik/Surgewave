using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Discovers, loads, manages lifecycle, and (optionally) hot-deploys WASM plugins.
/// Registered as a singleton in DI.
/// </summary>
public sealed class WasmPluginManager : IAsyncDisposable
{
    private static readonly JsonSerializerOptions ManifestJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly WasmPluginConfig _config;
    private readonly WasmRuntime _runtime;
    private readonly ILogger<WasmPluginManager> _logger;
    private readonly ConcurrentDictionary<string, WasmPluginInstance> _plugins = new();
    private FileSystemWatcher? _watcher;
    private Timer? _debounceTimer;
    private readonly ConcurrentQueue<string> _pendingReloads = new();
    private bool _disposed;

    public WasmPluginManager(
        WasmPluginConfig config,
        WasmRuntime runtime,
        ILogger<WasmPluginManager> logger)
    {
        _config = config;
        _runtime = runtime;
        _logger = logger;
    }

    /// <summary>
    /// Scans the configured <see cref="WasmPluginConfig.WasmDirectory"/> for plugin subdirectories,
    /// each containing a <c>wasm-plugin.json</c> manifest.
    /// </summary>
    public Task<IReadOnlyList<WasmPluginManifest>> DiscoverPluginsAsync()
    {
        var manifests = new List<WasmPluginManifest>();
        var dir = _config.WasmDirectory;

        if (!Directory.Exists(dir))
        {
            _logger.LogInformation("[WASM] Plugin directory does not exist: {Dir}", dir);
            return Task.FromResult<IReadOnlyList<WasmPluginManifest>>(manifests);
        }

        foreach (var subDir in Directory.GetDirectories(dir))
        {
            var manifestPath = Path.Combine(subDir, "wasm-plugin.json");
            if (!File.Exists(manifestPath)) continue;

            try
            {
                var json = File.ReadAllText(manifestPath);
                var manifest = JsonSerializer.Deserialize<WasmPluginManifest>(json, ManifestJsonOptions);
                if (manifest is not null)
                {
                    manifests.Add(manifest);
                    _logger.LogInformation("[WASM] Discovered plugin: {Id} ({Type}) v{Version}",
                        manifest.Id, manifest.Type, manifest.Version);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[WASM] Failed to read manifest: {Path}", manifestPath);
            }
        }

        _logger.LogInformation("[WASM] Discovered {Count} WASM plugins", manifests.Count);
        return Task.FromResult<IReadOnlyList<WasmPluginManifest>>(manifests);
    }

    /// <summary>
    /// Loads and starts a plugin by its directory name within the configured WASM directory.
    /// </summary>
    public async Task<WasmPluginInstance> LoadAndStartAsync(string pluginId)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // If already loaded, stop first
        if (_plugins.TryGetValue(pluginId, out var existing))
        {
            await StopAsync(pluginId).ConfigureAwait(false);
        }

        // Find the plugin directory
        var pluginDir = FindPluginDirectory(pluginId);
        if (pluginDir is null)
            throw new FileNotFoundException($"Plugin directory not found for '{pluginId}'");

        var instance = _runtime.LoadPluginFromDirectory(pluginDir);

        var initSuccess = await instance.InitializeAsync().ConfigureAwait(false);
        if (!initSuccess)
        {
            await instance.DisposeAsync().ConfigureAwait(false);
            throw new InvalidOperationException($"Plugin '{pluginId}' failed to initialise");
        }

        _plugins[pluginId] = instance;
        _logger.LogInformation("[WASM] Plugin '{PluginId}' loaded and started", pluginId);
        return instance;
    }

    /// <summary>
    /// Stops and removes a running plugin.
    /// </summary>
    public async Task StopAsync(string pluginId)
    {
        if (_plugins.TryRemove(pluginId, out var instance))
        {
            await instance.DisposeAsync().ConfigureAwait(false);
            _logger.LogInformation("[WASM] Plugin '{PluginId}' stopped", pluginId);
        }
    }

    /// <summary>
    /// Hot-reloads a plugin by stopping the current instance and loading the new version.
    /// </summary>
    public async Task ReloadAsync(string pluginId)
    {
        _logger.LogInformation("[WASM] Hot-reloading plugin '{PluginId}'", pluginId);
        await StopAsync(pluginId).ConfigureAwait(false);
        await LoadAndStartAsync(pluginId).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets a loaded plugin instance by ID.
    /// </summary>
    public WasmPluginInstance? GetPlugin(string pluginId)
    {
        _plugins.TryGetValue(pluginId, out var instance);
        return instance;
    }

    /// <summary>
    /// Returns the status of all loaded plugins.
    /// </summary>
    public IReadOnlyList<WasmPluginStatus> GetStatus()
    {
        return _plugins.Values
            .Select(p => p.GetStatus())
            .ToList();
    }

    /// <summary>
    /// Returns status of a single plugin.
    /// </summary>
    public WasmPluginStatus? GetPluginStatus(string pluginId)
    {
        return _plugins.TryGetValue(pluginId, out var instance)
            ? instance.GetStatus()
            : null;
    }

    /// <summary>
    /// Enables a <see cref="FileSystemWatcher"/> on the WASM plugins directory.
    /// When a <c>.wasm</c> file changes, the corresponding plugin is automatically reloaded
    /// after a debounce interval.
    /// </summary>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "FileSystemWatcher events must not crash the host")]
    public void EnableFileWatcher()
    {
        if (_watcher is not null) return;

        var dir = _config.WasmDirectory;
        if (!Directory.Exists(dir))
        {
            _logger.LogWarning("[WASM] Cannot enable file watcher — directory does not exist: {Dir}", dir);
            return;
        }

        _watcher = new FileSystemWatcher(dir)
        {
            IncludeSubdirectories = true,
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite | NotifyFilters.CreationTime,
            EnableRaisingEvents = true
        };

        _watcher.Changed += OnWasmFileChanged;
        _watcher.Created += OnWasmFileChanged;

        _logger.LogInformation("[WASM] File watcher enabled on {Dir}", dir);
    }

    private void OnWasmFileChanged(object sender, FileSystemEventArgs e)
    {
        if (!e.FullPath.EndsWith(".wasm", StringComparison.OrdinalIgnoreCase) &&
            !e.FullPath.EndsWith("wasm-plugin.json", StringComparison.OrdinalIgnoreCase))
            return;

        // Determine the plugin directory name
        var pluginDir = Path.GetDirectoryName(e.FullPath);
        if (pluginDir is null) return;

        var pluginDirName = Path.GetFileName(pluginDir);

        // Try to read the manifest to get the real plugin ID
        var manifestPath = Path.Combine(pluginDir, "wasm-plugin.json");
        string pluginId = pluginDirName;
        if (File.Exists(manifestPath))
        {
            try
            {
                var json = File.ReadAllText(manifestPath);
                var manifest = JsonSerializer.Deserialize<WasmPluginManifest>(json, ManifestJsonOptions);
                if (manifest?.Id is not null)
                    pluginId = manifest.Id;
            }
            catch
            {
                // Use directory name as fallback
            }
        }

        _pendingReloads.Enqueue(pluginId);

        // Debounce: reset the timer
        _debounceTimer?.Dispose();
        _debounceTimer = new Timer(
            _ => ProcessPendingReloads(),
            null,
            _config.HotDeployDebounce,
            Timeout.InfiniteTimeSpan);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Reload errors must not crash the host")]
    private void ProcessPendingReloads()
    {
        var processed = new HashSet<string>(StringComparer.Ordinal);
        while (_pendingReloads.TryDequeue(out var pluginId))
        {
            if (!processed.Add(pluginId)) continue;

            _logger.LogInformation("[WASM] Hot-deploy detected for '{PluginId}'", pluginId);
            try
            {
                // Fire-and-forget reload
                _ = ReloadAsync(pluginId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[WASM] Failed to hot-reload '{PluginId}'", pluginId);
            }
        }
    }

    private string? FindPluginDirectory(string pluginId)
    {
        var dir = _config.WasmDirectory;
        if (!Directory.Exists(dir)) return null;

        // First, try exact match by directory name
        var exact = Path.Combine(dir, pluginId);
        if (Directory.Exists(exact) && File.Exists(Path.Combine(exact, "wasm-plugin.json")))
            return exact;

        // Scan all subdirectories for a matching manifest ID
        foreach (var subDir in Directory.GetDirectories(dir))
        {
            var manifestPath = Path.Combine(subDir, "wasm-plugin.json");
            if (!File.Exists(manifestPath)) continue;

            try
            {
                var json = File.ReadAllText(manifestPath);
                var manifest = JsonSerializer.Deserialize<WasmPluginManifest>(json, ManifestJsonOptions);
                if (manifest?.Id == pluginId)
                    return subDir;
            }
            catch
            {
                // Skip unreadable manifests
            }
        }

        return null;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _watcher?.Dispose();
        _debounceTimer?.Dispose();

        foreach (var plugin in _plugins.Values)
        {
            await plugin.DisposeAsync().ConfigureAwait(false);
        }

        _plugins.Clear();
    }
}
