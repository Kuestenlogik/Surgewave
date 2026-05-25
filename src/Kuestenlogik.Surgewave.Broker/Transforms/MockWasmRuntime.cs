using System.Text.Json;

namespace Kuestenlogik.Surgewave.Broker.Transforms;

/// <summary>
/// Simple in-process mock WASM runtime for testing. Executes a configurable
/// delegate as the transform function instead of actual WASM bytecode.
/// </summary>
public sealed class MockWasmRuntime : IWasmRuntime
{
    private Func<byte[], byte[]>? _transformFunc;
    private bool _moduleLoaded;
    private bool _disposed;

    /// <summary>
    /// Creates a mock runtime with a pass-through transform.
    /// </summary>
    public MockWasmRuntime()
    {
        _transformFunc = PassThrough;
    }

    /// <summary>
    /// Creates a mock runtime with a custom transform function.
    /// The function receives JSON-serialized input and should return JSON-serialized output.
    /// </summary>
    public MockWasmRuntime(Func<byte[], byte[]> transformFunc)
    {
        _transformFunc = transformFunc;
    }

    /// <summary>
    /// The path of the loaded module (for verification in tests).
    /// </summary>
    public string? LoadedModulePath { get; private set; }

    public void LoadModule(string wasmPath)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        LoadedModulePath = wasmPath;
        _moduleLoaded = true;
    }

    public byte[] CallTransform(byte[] input)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_moduleLoaded)
        {
            throw new InvalidOperationException("No WASM module loaded. Call LoadModule first.");
        }

        return _transformFunc!(input);
    }

    public void Dispose()
    {
        _disposed = true;
        _transformFunc = null;
    }

    /// <summary>
    /// Default pass-through: deserializes input and returns it unchanged.
    /// </summary>
    private static byte[] PassThrough(byte[] input)
    {
        using var doc = JsonDocument.Parse(input);
        var root = doc.RootElement;

        var output = new Dictionary<string, object?>
        {
            ["Dropped"] = false,
        };

        if (root.TryGetProperty("Key", out var key))
        {
            output["Key"] = key.Deserialize<byte[]>();
        }

        if (root.TryGetProperty("Value", out var value))
        {
            output["Value"] = value.Deserialize<byte[]>();
        }

        return JsonSerializer.SerializeToUtf8Bytes(output);
    }
}
