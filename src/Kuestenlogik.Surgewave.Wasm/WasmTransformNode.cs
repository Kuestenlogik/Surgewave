namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Metadata descriptor for a WASM transform that can appear as a node in the
/// visual pipeline editor. This is not a full connector — it wraps a
/// <see cref="WasmPluginInstance"/> of type <see cref="WasmPluginType.Transform"/>
/// or <see cref="WasmPluginType.Function"/> for inline message processing.
/// </summary>
public sealed class WasmTransformNode
{
    /// <summary>Plugin identifier from the manifest.</summary>
    public required string PluginId { get; init; }

    /// <summary>Human-readable name for the pipeline editor.</summary>
    public required string Name { get; init; }

    /// <summary>Description shown in the node tooltip.</summary>
    public string? Description { get; init; }

    /// <summary>The underlying WASM plugin type (Transform or Function).</summary>
    public WasmPluginType Type { get; init; }

    /// <summary>Optional input topic binding.</summary>
    public string? InputTopic { get; init; }

    /// <summary>Optional output topic binding.</summary>
    public string? OutputTopic { get; init; }

    /// <summary>
    /// Processes a single message through the WASM transform.
    /// Returns <c>null</c> if the transform signals "drop".
    /// </summary>
    /// <param name="manager">The plugin manager holding the loaded instance.</param>
    /// <param name="input">Input message bytes.</param>
    /// <param name="ct">Cancellation token.</param>
    public async Task<byte[]?> ProcessAsync(
        WasmPluginManager manager,
        byte[] input,
        CancellationToken ct = default)
    {
        var instance = manager.GetPlugin(PluginId)
            ?? throw new InvalidOperationException($"WASM plugin '{PluginId}' is not loaded");

        return await instance.ProcessAsync(input, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a <see cref="WasmTransformNode"/> from a manifest.
    /// </summary>
    public static WasmTransformNode FromManifest(WasmPluginManifest manifest)
    {
        return new WasmTransformNode
        {
            PluginId = manifest.Id,
            Name = manifest.Name,
            Description = manifest.Description,
            Type = manifest.Type,
            InputTopic = manifest.InputTopic,
            OutputTopic = manifest.OutputTopic
        };
    }
}
