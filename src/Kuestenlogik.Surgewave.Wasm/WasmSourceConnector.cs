using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Wraps a WASM Source plugin as a Surgewave <see cref="SourceConnector"/>,
/// allowing it to be managed through the Connect framework and pipeline editor.
/// </summary>
[ConnectorMetadata(
    Name = "WASM Source",
    Description = "Loads a WebAssembly module as a source connector (sandboxed, language-agnostic)",
    Icon = "Memory",
    Tags = "wasm,webassembly,plugin,source")]
public sealed class WasmSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    /// <inheritdoc />
    public override string Version => "1.0.0";

    /// <inheritdoc />
    public override Type TaskClass => typeof(WasmSourceTask);

    /// <inheritdoc />
    public override ConfigDef Config => new ConfigDef()
        .Define("wasm.plugin.id", ConfigType.String, Importance.High,
            "ID of the WASM plugin to load (must match manifest id)")
        .Define("wasm.plugin.path", ConfigType.String, "", Importance.Medium,
            "Path to the WASM plugin directory (overrides default discovery)")
        .Define("topic", ConfigType.String, Importance.High,
            "Output topic for produced records")
        .Define("poll.interval.ms", ConfigType.Int, "1000", Importance.Medium,
            "Interval in milliseconds between poll calls");

    /// <inheritdoc />
    public override void Start(IDictionary<string, string> config)
    {
        _config = config;
    }

    /// <inheritdoc />
    public override void Stop() { }

    /// <inheritdoc />
    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // WASM plugins are single-task by nature (one WASM instance)
        return [new Dictionary<string, string>(_config)];
    }
}
