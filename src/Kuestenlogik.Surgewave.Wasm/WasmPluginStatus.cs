namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Runtime status snapshot of a loaded WASM plugin, suitable for REST API responses
/// and the control UI dashboard.
/// </summary>
/// <param name="PluginId">Plugin identifier from manifest.</param>
/// <param name="Name">Human-readable name from manifest.</param>
/// <param name="Type">Plugin type (Source, Sink, Transform, Function).</param>
/// <param name="State">Current lifecycle state.</param>
/// <param name="MemoryUsageBytes">Approximate linear memory usage of the WASM instance in bytes.</param>
/// <param name="MessagesProcessed">Total messages successfully processed since load.</param>
/// <param name="ErrorCount">Total errors since load.</param>
/// <param name="LoadedAt">Timestamp when this plugin instance was loaded.</param>
/// <param name="Version">Plugin version from manifest.</param>
/// <param name="LastError">Last error message, if any.</param>
public sealed record WasmPluginStatus(
    string PluginId,
    string Name,
    WasmPluginType Type,
    WasmPluginState State,
    long MemoryUsageBytes,
    long MessagesProcessed,
    long ErrorCount,
    DateTimeOffset LoadedAt,
    string Version,
    string? LastError = null);
