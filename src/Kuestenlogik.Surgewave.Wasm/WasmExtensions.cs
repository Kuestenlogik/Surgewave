using Kuestenlogik.Surgewave.Core.Pipeline;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// DI extension methods for registering the WASM plugin subsystem.
/// </summary>
public static class WasmExtensions
{
    /// <summary>
    /// Adds the Surgewave WASM plugin subsystem to the service collection.
    /// Registers <see cref="WasmPluginConfig"/>, <see cref="WasmRuntime"/>, and
    /// <see cref="WasmPluginManager"/> as singletons.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration callback.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSurgewaveWasm(
        this IServiceCollection services,
        Action<WasmPluginConfig>? configure = null)
    {
        var config = new WasmPluginConfig();
        configure?.Invoke(config);

        services.AddSingleton(config);

        services.AddSingleton(sp =>
        {
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            return new WasmRuntime(config, loggerFactory);
        });

        services.AddSingleton(sp =>
        {
            var runtime = sp.GetRequiredService<WasmRuntime>();
            var logger = sp.GetRequiredService<ILogger<WasmPluginManager>>();
            return new WasmPluginManager(config, runtime, logger);
        });

        services.AddHostedService<WasmPluginHostedService>();

        // G7 — broker-side hot-path record transforms backed by the WASM runtime
        // (Redpanda Data Transforms parity). Registered as both the
        // IRecordTransformPipeline the produce path looks for AND the concrete
        // type so tests / diagnostics can resolve it directly. Activated only
        // when a LogManager is also registered — otherwise the topic-config
        // lookup has nowhere to read from.
        services.AddSingleton(sp =>
        {
            var manager = sp.GetRequiredService<WasmPluginManager>();
            var logManager = sp.GetRequiredService<LogManager>();
            var logger = sp.GetRequiredService<ILogger<WasmRecordTransformPipeline>>();
            return new WasmRecordTransformPipeline(manager, logManager, logger);
        });
        services.AddSingleton<IRecordTransformPipeline>(sp => sp.GetRequiredService<WasmRecordTransformPipeline>());

        return services;
    }

    /// <summary>
    /// Adds the Surgewave WASM plugin subsystem from <see cref="IConfiguration"/>.
    /// Reads settings from the <c>Surgewave:Wasm</c> section.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSurgewaveWasm(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var config = new WasmPluginConfig();
        configuration.GetSection(WasmPluginConfig.SectionName).Bind(config);

        return services.AddSurgewaveWasm(c =>
        {
            c.Enabled = config.Enabled;
            c.WasmDirectory = config.WasmDirectory;
            c.MaxMemoryBytes = config.MaxMemoryBytes;
            c.ExecutionTimeout = config.ExecutionTimeout;
            c.AllowFileAccess = config.AllowFileAccess;
            c.AllowNetworkAccess = config.AllowNetworkAccess;
            c.EnableHotDeploy = config.EnableHotDeploy;
            c.HotDeployDebounce = config.HotDeployDebounce;
        });
    }
}
