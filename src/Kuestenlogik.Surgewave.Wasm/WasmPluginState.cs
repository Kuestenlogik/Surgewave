namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Lifecycle state of a loaded WASM plugin instance.
/// </summary>
public enum WasmPluginState
{
    /// <summary>The WASM module is being loaded and validated.</summary>
    Loading,

    /// <summary>The module loaded successfully and is ready to be started.</summary>
    Ready,

    /// <summary>The plugin is actively processing messages.</summary>
    Running,

    /// <summary>The plugin encountered an unrecoverable error.</summary>
    Failed,

    /// <summary>The plugin has been stopped gracefully.</summary>
    Stopped
}
