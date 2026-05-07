using System.Text.Json;
using Microsoft.Extensions.Logging;
using Wasmtime;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Manages the Wasmtime <see cref="Engine"/> and creates <see cref="WasmPluginInstance"/>s
/// from <c>.wasm</c> files. One runtime is shared across all plugins; each plugin gets
/// its own <see cref="Store"/> and <see cref="Instance"/> for isolation.
/// </summary>
public sealed class WasmRuntime : IDisposable
{
    private static readonly JsonSerializerOptions ManifestJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly Engine _engine;
    private readonly WasmPluginConfig _config;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<WasmRuntime> _logger;
    private readonly Action<string, byte[]?, byte[]>? _produceCallback;
    private bool _disposed;

    /// <summary>
    /// Creates a new WASM runtime.
    /// </summary>
    /// <param name="config">Plugin configuration.</param>
    /// <param name="loggerFactory">Logger factory for per-plugin loggers.</param>
    /// <param name="produceCallback">
    /// Optional callback invoked when a WASM module calls <c>surgewave_produce</c>.
    /// </param>
    public WasmRuntime(
        WasmPluginConfig config,
        ILoggerFactory loggerFactory,
        Action<string, byte[]?, byte[]>? produceCallback = null)
    {
        _config = config;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<WasmRuntime>();
        _produceCallback = produceCallback;
        _engine = new Engine();

        _logger.LogInformation("Wasmtime engine created (maxMemory={MaxMB}MB, timeout={Timeout}s)",
            config.MaxMemoryBytes / (1024 * 1024), config.ExecutionTimeout.TotalSeconds);
    }

    /// <summary>
    /// Loads a WASM module from disk and creates a new <see cref="WasmPluginInstance"/>.
    /// </summary>
    /// <param name="wasmPath">Path to the <c>.wasm</c> file.</param>
    /// <param name="manifest">Plugin manifest describing the module.</param>
    /// <returns>A ready-to-initialise plugin instance.</returns>
    public WasmPluginInstance LoadPlugin(string wasmPath, WasmPluginManifest manifest)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(manifest);

        if (!File.Exists(wasmPath))
            throw new FileNotFoundException($"WASM module not found: {wasmPath}", wasmPath);

        _logger.LogInformation("[WASM] Loading module '{PluginId}' from {Path}", manifest.Id, wasmPath);

        var module = Module.FromFile(_engine, wasmPath);
        var store = new Store(_engine);

        // Configure memory limits
        store.SetLimits(
            memorySize: _config.MaxMemoryBytes,
            tableElements: 10_000,
            instances: 1,
            tables: 10,
            memories: 1);

        // Set up linker with host functions
        using var linker = new Linker(_engine);
        linker.DefineWasi();

        var pluginLogger = _loggerFactory.CreateLogger($"Wasm.{manifest.Id}");
        var hostFunctions = new WasmHostFunctions(pluginLogger, manifest.Config, _produceCallback);

        // We need a deferred memory reference because the memory is only available
        // after the instance is created.
        Memory? instanceMemory = null;
        hostFunctions.Register(linker, store, () => instanceMemory);

        var instance = linker.Instantiate(store, module);
        instanceMemory = instance.GetMemory("memory");

        var pluginInstance = new WasmPluginInstance(
            store, instance, manifest, _config.ExecutionTimeout, pluginLogger);

        _logger.LogInformation("[WASM] Module '{PluginId}' loaded ({Type})", manifest.Id, manifest.Type);
        return pluginInstance;
    }

    /// <summary>
    /// Loads both the manifest and the WASM module from a plugin directory.
    /// Expects <c>wasm-plugin.json</c> and <c>plugin.wasm</c> in the directory.
    /// </summary>
    /// <param name="pluginDirectory">Path to the plugin subdirectory.</param>
    /// <returns>A ready-to-initialise plugin instance.</returns>
    public WasmPluginInstance LoadPluginFromDirectory(string pluginDirectory)
    {
        var manifestPath = Path.Combine(pluginDirectory, "wasm-plugin.json");
        if (!File.Exists(manifestPath))
            throw new FileNotFoundException($"Manifest not found: {manifestPath}", manifestPath);

        var json = File.ReadAllText(manifestPath);
        var manifest = JsonSerializer.Deserialize<WasmPluginManifest>(json, ManifestJsonOptions)
            ?? throw new InvalidOperationException($"Failed to deserialise manifest: {manifestPath}");

        var wasmPath = Path.Combine(pluginDirectory, "plugin.wasm");
        if (!File.Exists(wasmPath))
        {
            // Fall back to any .wasm file in the directory
            var wasmFiles = Directory.GetFiles(pluginDirectory, "*.wasm");
            wasmPath = wasmFiles.Length > 0
                ? wasmFiles[0]
                : throw new FileNotFoundException($"No .wasm file found in {pluginDirectory}");
        }

        return LoadPlugin(wasmPath, manifest);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _engine.Dispose();
    }
}
