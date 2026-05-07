namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Configuration for the WASM plugin subsystem.
/// Bound from <c>Surgewave:Wasm</c> section in appsettings.json.
/// </summary>
public sealed class WasmPluginConfig
{
    /// <summary>Configuration section name.</summary>
    public const string SectionName = "Surgewave:Wasm";

    /// <summary>
    /// Enable the WASM plugin subsystem. Default: false.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Directory to scan for WASM plugin subdirectories.
    /// Each subdirectory should contain a <c>plugin.wasm</c> and <c>wasm-plugin.json</c> manifest.
    /// Default: <c>wasm-plugins</c>.
    /// </summary>
    public string WasmDirectory { get; set; } = "wasm-plugins";

    /// <summary>
    /// Maximum linear memory in bytes that a single WASM module may allocate.
    /// Default: 64 MB.
    /// </summary>
    public long MaxMemoryBytes { get; set; } = 64 * 1024 * 1024;

    /// <summary>
    /// Maximum wall-clock time for a single WASM function call.
    /// If exceeded, the call is cancelled and the plugin is marked <see cref="WasmPluginState.Failed"/>.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan ExecutionTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Allow the WASM module to access the host file system via WASI.
    /// Default: false (fully sandboxed).
    /// </summary>
    public bool AllowFileAccess { get; set; }

    /// <summary>
    /// Allow the WASM module to make outbound network calls via WASI.
    /// Default: false (fully sandboxed).
    /// </summary>
    public bool AllowNetworkAccess { get; set; }

    /// <summary>
    /// Enable FileSystemWatcher on <see cref="WasmDirectory"/> for hot-deploy.
    /// When a new or updated <c>.wasm</c> file is detected, the plugin is automatically (re)loaded.
    /// Default: true.
    /// </summary>
    public bool EnableHotDeploy { get; set; } = true;

    /// <summary>
    /// Debounce interval for file watcher events to avoid multiple reloads
    /// during a single file copy operation. Default: 2 seconds.
    /// </summary>
    public TimeSpan HotDeployDebounce { get; set; } = TimeSpan.FromSeconds(2);
}
