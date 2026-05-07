using System.Net.Http.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Plugins;

/// <summary>
/// Hosted service that synchronizes plugins from the broker.
/// On startup, compares local installed plugins with the broker's available packages
/// and pulls any missing ones via the broker's REST API.
/// </summary>
public sealed class PluginSyncService : BackgroundService
{
    private readonly PluginPackageManager _packageManager;
    private readonly string _pluginsDir;
    private readonly string _brokerBaseUrl;
    private readonly TimeSpan _syncInterval;
    private readonly bool _autoInstallOnDemand;
    private readonly ILogger<PluginSyncService> _logger;

    public PluginSyncService(
        PluginPackageManager packageManager,
        string pluginsDir,
        string brokerBaseUrl,
        PluginSyncConfig? config = null,
        ILogger<PluginSyncService>? logger = null)
    {
        _packageManager = packageManager;
        _pluginsDir = pluginsDir;
        _brokerBaseUrl = brokerBaseUrl.TrimEnd('/');
        _syncInterval = config?.SyncInterval ?? TimeSpan.FromMinutes(1);
        _autoInstallOnDemand = config?.AutoInstallOnDemand ?? false;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<PluginSyncService>.Instance;
    }

    /// <summary>
    /// Attempts to auto-install a missing plugin on demand when a pipeline requires it.
    /// </summary>
    public async Task<bool> EnsurePluginAvailableAsync(string pluginId, CancellationToken ct = default)
    {
        if (!_autoInstallOnDemand)
        {
            _logger.LogWarning(
                "Plugin {PluginId} is required but not installed. Auto-install is disabled (Surgewave:PluginSync:AutoInstallOnDemand=false)",
                pluginId);
            return false;
        }

        var installed = await GetInstalledPluginsAsync(ct);
        if (installed.ContainsKey(pluginId))
            return true;

        _logger.LogInformation("Auto-installing missing plugin {PluginId} from broker on demand", pluginId);

        try
        {
            using var httpClient = new HttpClient();
            var response = await httpClient.GetAsync($"{_brokerBaseUrl}/api/plugins", ct);
            if (!response.IsSuccessStatusCode) return false;

            var brokerPlugins = await response.Content.ReadFromJsonAsync<BrokerPluginInfo[]>(ct);
            var match = brokerPlugins?.FirstOrDefault(p =>
                string.Equals(p.Id, pluginId, StringComparison.OrdinalIgnoreCase));

            if (match?.Id == null || match.Version == null)
            {
                _logger.LogWarning("Plugin {PluginId} not found on broker", pluginId);
                return false;
            }

            return await PullPluginAsync(match.Id, match.Version, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to auto-install plugin {PluginId}", pluginId);
            return false;
        }
    }

    /// <summary>
    /// Gets the set of currently installed plugin IDs and versions.
    /// </summary>
    public async Task<Dictionary<string, string>> GetInstalledPluginsAsync(CancellationToken ct = default)
    {
        var installed = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        await foreach (var plugin in _packageManager.GetInstalledPluginsAsync(_pluginsDir, ct))
        {
            installed[plugin.Id] = plugin.Version;
        }
        return installed;
    }

    /// <summary>
    /// Pulls and installs a specific plugin from the broker's REST API.
    /// </summary>
    public async Task<bool> PullPluginAsync(string pluginId, string version, CancellationToken ct = default)
    {
        try
        {
            using var httpClient = new HttpClient();
            var url = $"{_brokerBaseUrl}/api/plugins/{pluginId}/{version}/download";

            _logger.LogInformation("Pulling plugin {PluginId} v{Version} from broker", pluginId, version);

            var response = await httpClient.GetAsync(url, ct);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Failed to pull plugin {PluginId} v{Version}: {Status}", pluginId, version, response.StatusCode);
                return false;
            }

            var tempPath = Path.Combine(Path.GetTempPath(), $"{pluginId}-{version}.swpkg");
            await using (var fileStream = new FileStream(tempPath, FileMode.Create))
            {
                await response.Content.CopyToAsync(fileStream, ct);
            }

            var result = await _packageManager.InstallAsync(
                tempPath, _pluginsDir, force: true, cancellationToken: ct);

            try { File.Delete(tempPath); } catch { /* ignore */ }

            if (result.Success)
            {
                _logger.LogInformation("Installed plugin {PluginId} v{Version} from broker", pluginId, version);
                return true;
            }

            _logger.LogWarning("Failed to install plugin {PluginId}: {Error}", pluginId, result.Error);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error pulling plugin {PluginId} v{Version}", pluginId, version);
            return false;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Plugin sync service started (on-demand auto-install: {AutoInstall})", _autoInstallOnDemand);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private sealed record BrokerPluginInfo(string? Id, string? Version);
}

/// <summary>
/// Configuration for plugin synchronization between worker and broker.
/// </summary>
public sealed class PluginSyncConfig
{
    public bool Enabled { get; set; }
    public TimeSpan SyncInterval { get; set; } = TimeSpan.FromMinutes(1);
    public bool AutoInstallOnDemand { get; set; }
}
