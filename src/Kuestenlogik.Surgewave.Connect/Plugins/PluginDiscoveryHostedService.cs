using System.Diagnostics.CodeAnalysis;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Plugins;

/// <summary>
/// Hosted service that initializes plugin discovery on startup.
/// </summary>
[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Instantiated via DI")]
internal sealed class PluginDiscoveryHostedService : IHostedService
{
    private readonly PluginDiscovery _discovery;
    private readonly ConnectWorkerConfig _config;
    private readonly ILogger<PluginDiscoveryHostedService> _logger;

    public PluginDiscoveryHostedService(
        PluginDiscovery discovery,
        ConnectWorkerConfig config,
        ILogger<PluginDiscoveryHostedService> logger)
    {
        _discovery = discovery;
        _config = config;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_config.EnablePluginDiscovery)
        {
            _logger.LogInformation("Plugin discovery is disabled");
            return Task.CompletedTask;
        }

        var pluginPaths = new List<string>();

        // 1. Environment variable (highest priority)
        var envPath = Environment.GetEnvironmentVariable("Surgewave_PLUGIN_PATH");
        if (!string.IsNullOrEmpty(envPath))
        {
            var separator = Path.PathSeparator;
            pluginPaths.AddRange(envPath.Split(separator, StringSplitOptions.RemoveEmptyEntries));
            _logger.LogInformation("Surgewave_PLUGIN_PATH: {Paths}", envPath);
        }

        // 2. Config array (PluginsDirectories)
        if (_config.PluginsDirectories?.Length > 0)
        {
            pluginPaths.AddRange(_config.PluginsDirectories);
        }

        // 3. Legacy single path (PluginsDirectory) - only if no other paths configured
        if (pluginPaths.Count == 0 && !string.IsNullOrEmpty(_config.PluginsDirectory))
        {
            pluginPaths.Add(_config.PluginsDirectory);
        }

        foreach (var path in pluginPaths.Distinct())
        {
            var fullPath = Path.GetFullPath(path);
            _logger.LogInformation("Plugin discovery scanning: {Path}", fullPath);

            if (Directory.Exists(fullPath))
            {
                _discovery.DiscoverPlugins(fullPath, useDefaultContext: true);
            }
            else
            {
                _logger.LogWarning("Plugins directory does not exist: {Path}. " +
                    "Create this directory and copy connector DLLs to enable more connectors.", fullPath);
            }
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
