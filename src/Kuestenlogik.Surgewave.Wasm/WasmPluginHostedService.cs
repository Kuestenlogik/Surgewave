using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Background service that discovers and auto-loads WASM plugins on startup,
/// and optionally enables the file watcher for hot-deploy.
/// </summary>
public sealed class WasmPluginHostedService : IHostedService
{
    private readonly WasmPluginConfig _config;
    private readonly WasmPluginManager _manager;
    private readonly ILogger<WasmPluginHostedService> _logger;

    public WasmPluginHostedService(
        WasmPluginConfig config,
        WasmPluginManager manager,
        ILogger<WasmPluginHostedService> logger)
    {
        _config = config;
        _manager = manager;
        _logger = logger;
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Startup errors must not crash the host")]
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_config.Enabled)
        {
            _logger.LogInformation("[WASM] Plugin subsystem is disabled");
            return;
        }

        _logger.LogInformation("[WASM] Starting WASM plugin subsystem (directory={Dir})", _config.WasmDirectory);

        // Discover available plugins
        var manifests = await _manager.DiscoverPluginsAsync().ConfigureAwait(false);

        // Auto-load all discovered plugins
        foreach (var manifest in manifests)
        {
            try
            {
                await _manager.LoadAndStartAsync(manifest.Id).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[WASM] Failed to auto-load plugin '{PluginId}'", manifest.Id);
            }
        }

        // Enable file watcher for hot-deploy
        if (_config.EnableHotDeploy)
        {
            _manager.EnableFileWatcher();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("[WASM] Stopping WASM plugin subsystem");
        await _manager.DisposeAsync().ConfigureAwait(false);
    }
}
