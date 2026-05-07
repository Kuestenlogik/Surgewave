namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// The type of a WASM plugin, defining how it participates in the Surgewave data pipeline.
/// </summary>
public enum WasmPluginType
{
    /// <summary>
    /// A source plugin that produces records into Surgewave topics.
    /// Must export <c>plugin_poll</c>.
    /// </summary>
    Source,

    /// <summary>
    /// A sink plugin that consumes records from Surgewave topics.
    /// Must export <c>plugin_push</c>.
    /// </summary>
    Sink,

    /// <summary>
    /// A transform plugin that processes messages in-flight (1:1 mapping).
    /// Must export <c>plugin_process</c>.
    /// </summary>
    Transform,

    /// <summary>
    /// A function plugin that processes messages and may produce 0..N output messages.
    /// Must export <c>plugin_process</c>.
    /// </summary>
    Function
}
