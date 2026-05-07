namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Defines the WASM Application Binary Interface (ABI) contract between Surgewave and WASM modules.
/// These are the exported function names that Surgewave looks for in a WASM module,
/// and the host function names that Surgewave provides to the module.
/// </summary>
internal static class WasmAbi
{
    // ────────── Functions the WASM module MUST export ──────────

    /// <summary>Initialize the plugin. Returns 0 on success.</summary>
    internal const string PluginInit = "plugin_init";

    /// <summary>Return a pointer to a JSON metadata string in WASM memory.</summary>
    internal const string PluginInfo = "plugin_info";

    /// <summary>Process a message (Transform / Function). Takes (ptr, len), returns result ptr.</summary>
    internal const string PluginProcess = "plugin_process";

    /// <summary>Poll for new data (Source). Returns pointer to output buffer.</summary>
    internal const string PluginPoll = "plugin_poll";

    /// <summary>Push data to the plugin (Sink). Takes (ptr, len), returns 0 on success.</summary>
    internal const string PluginPush = "plugin_push";

    /// <summary>Cleanup and shutdown. Returns 0 on success.</summary>
    internal const string PluginClose = "plugin_close";

    /// <summary>Allocate memory inside the WASM module for the host to write into. Takes size, returns ptr.</summary>
    internal const string Alloc = "alloc";

    /// <summary>Free memory inside the WASM module. Takes (ptr, size).</summary>
    internal const string Dealloc = "dealloc";

    // ────────── Functions Surgewave exports TO the WASM module (host functions) ──────────

    /// <summary>
    /// Produce a message to a Surgewave topic.
    /// Signature: <c>surgewave_produce(topic_ptr, topic_len, key_ptr, key_len, value_ptr, value_len) -> i32</c>
    /// </summary>
    internal const string HostProduce = "surgewave_produce";

    /// <summary>
    /// Log a message from WASM to the Surgewave logger.
    /// Signature: <c>surgewave_log(level, msg_ptr, msg_len)</c>
    /// </summary>
    internal const string HostLog = "surgewave_log";

    /// <summary>
    /// Read a configuration value by key.
    /// Signature: <c>surgewave_get_config(key_ptr, key_len, out_ptr, out_len) -> i32</c>
    /// </summary>
    internal const string HostGetConfig = "surgewave_get_config";

    /// <summary>
    /// Get a value from the plugin state store.
    /// Signature: <c>surgewave_state_get(key_ptr, key_len, out_ptr, out_len) -> i32</c>
    /// </summary>
    internal const string HostStateGet = "surgewave_state_get";

    /// <summary>
    /// Put a value into the plugin state store.
    /// Signature: <c>surgewave_state_put(key_ptr, key_len, value_ptr, value_len) -> i32</c>
    /// </summary>
    internal const string HostStatePut = "surgewave_state_put";
}
