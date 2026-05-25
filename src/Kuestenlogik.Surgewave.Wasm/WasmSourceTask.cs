using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Source task that delegates to a WASM plugin's <c>plugin_poll()</c> export.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Plugin instance lifecycle is owned by WasmPluginManager")]
public sealed class WasmSourceTask : SourceTask
{
    private WasmPluginManager? _manager;
    private WasmPluginInstance? _instance;
    private string _topic = "wasm-source";
    private int _pollIntervalMs = 1000;

    /// <inheritdoc />
    public override string Version => "1.0.0";

    /// <inheritdoc />
    public override void Start(IDictionary<string, string> config)
    {
        var pluginId = config.GetValueOrDefault("wasm.plugin.id")
            ?? throw new ArgumentException("wasm.plugin.id is required");

        _topic = config.GetValueOrDefault("topic") ?? "wasm-source";

        if (config.TryGetValue("poll.interval.ms", out var interval) &&
            int.TryParse(interval, CultureInfo.InvariantCulture, out var ms))
        {
            _pollIntervalMs = ms;
        }

        // Resolve the plugin manager from the task context
        // The manager is set via SetPluginManager during connector framework wiring
        if (_manager is null)
            throw new InvalidOperationException("WasmPluginManager not available. Ensure WASM subsystem is enabled.");

        _instance = _manager.GetPlugin(pluginId);
        if (_instance is null)
        {
            // Try to load it
            _instance = _manager.LoadAndStartAsync(pluginId).GetAwaiter().GetResult();
        }
    }

    /// <inheritdoc />
    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_instance is null) return [];

        await Task.Delay(_pollIntervalMs, cancellationToken).ConfigureAwait(false);

        var batches = await _instance.PollAsync(cancellationToken).ConfigureAwait(false);
        if (batches.Count == 0) return [];

        var records = new List<SourceRecord>(batches.Count);
        for (var i = 0; i < batches.Count; i++)
        {
            records.Add(new SourceRecord
            {
                SourcePartition = new Dictionary<string, object> { ["wasm"] = _instance.PluginId },
                SourceOffset = new Dictionary<string, object> { ["seq"] = i },
                Topic = _topic,
                Value = batches[i]
            });
        }

        return records;
    }

    /// <inheritdoc />
    public override void Stop()
    {
        // The plugin lifecycle is managed by WasmPluginManager, not the task.
        // Null out the reference so CA2213 is satisfied (we don't own the instance).
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
