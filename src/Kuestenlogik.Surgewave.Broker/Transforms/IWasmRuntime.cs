namespace Kuestenlogik.Surgewave.Broker.Transforms;

/// <summary>
/// Pluggable WASM runtime interface. Implement with Wasmtime, wazero, or similar.
/// The runtime loads a WASM module and exposes a single transform function
/// that accepts serialized input and returns serialized output.
/// </summary>
public interface IWasmRuntime : IDisposable
{
    /// <summary>
    /// Loads a WASM module from the specified file path.
    /// </summary>
    void LoadModule(string wasmPath);

    /// <summary>
    /// Calls the exported transform function with serialized JSON input.
    /// Returns the serialized JSON output.
    /// ABI: input = JSON-serialized TransformContext, output = JSON-serialized TransformResult.
    /// </summary>
    byte[] CallTransform(byte[] input);
}
