using System.Diagnostics.CodeAnalysis;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Sink task that delegates to a WASM plugin's <c>plugin_push()</c> export.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Plugin instance lifecycle is owned by WasmPluginManager")]
public sealed class WasmSinkTask : SinkTask
{
    private WasmPluginManager? _manager;
    private WasmPluginInstance? _instance;

    /// <inheritdoc />
    public override string Version => "1.0.0";

    /// <inheritdoc />
    public override void Start(IDictionary<string, string> config)
    {
        var pluginId = config.GetValueOrDefault("wasm.plugin.id")
            ?? throw new ArgumentException("wasm.plugin.id is required");

        if (_manager is null)
            throw new InvalidOperationException("WasmPluginManager not available. Ensure WASM subsystem is enabled.");

        _instance = _manager.GetPlugin(pluginId);
        if (_instance is null)
        {
            _instance = _manager.LoadAndStartAsync(pluginId).GetAwaiter().GetResult();
        }
    }

    /// <inheritdoc />
    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_instance is null) return;

        foreach (var record in records)
        {
            if (record.Value is null) continue;
            await _instance.PushAsync(record.Value, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public override void Stop()
    {
        // The plugin lifecycle is managed by WasmPluginManager, not the task.
        _instance = null;
    }

    /// <summary>
    /// Injects the plugin manager. Called by the connector framework integration.
    /// </summary>
    internal void SetPluginManager(WasmPluginManager manager)
    {
        _manager = manager;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            // We do not dispose _instance here — it is owned by WasmPluginManager.
            _instance = null;
        }

        base.Dispose(disposing);
    }
}
