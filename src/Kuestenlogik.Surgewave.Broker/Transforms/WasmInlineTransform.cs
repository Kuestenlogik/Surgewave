using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Transforms;

namespace Kuestenlogik.Surgewave.Broker.Transforms;

/// <summary>
/// WASM-based inline transform. Delegates to an <see cref="IWasmRuntime"/> for actual execution.
/// The WASM module receives JSON-serialized TransformContext and returns JSON-serialized output.
/// </summary>
public sealed class WasmInlineTransform : IInlineTransform
{
    private readonly IWasmRuntime _runtime;
    private IReadOnlyDictionary<string, string> _config = new Dictionary<string, string>();
    private bool _disposed;

    public WasmInlineTransform(string name, IWasmRuntime runtime, string wasmPath)
    {
        Name = name;
        _runtime = runtime;
        _runtime.LoadModule(wasmPath);
    }

    public string Name { get; }

    public void Initialize(IReadOnlyDictionary<string, string> config)
    {
        _config = config;
    }

    public TransformResult Transform(TransformContext context)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var inputJson = JsonSerializer.SerializeToUtf8Bytes(new WasmTransformInput
        {
            Topic = context.Topic,
            Partition = context.Partition,
            Key = context.Key,
            Value = context.Value,
            Headers = context.Headers,
            Timestamp = context.Timestamp,
            Phase = context.Phase.ToString(),
            Config = _config
        });

        var outputBytes = _runtime.CallTransform(inputJson);
        var output = JsonSerializer.Deserialize<WasmTransformOutput>(outputBytes);

        if (output == null)
        {
            return TransformResult.Pass(context.Key, context.Value, context.Headers);
        }

        if (output.Dropped)
        {
            return TransformResult.Drop();
        }

        var key = output.Key ?? context.Key;
        var value = output.Value ?? context.Value;
        var headers = output.Headers;

        return output.RouteTopic != null
            ? TransformResult.Route(output.RouteTopic, key, value, headers)
            : TransformResult.Pass(key, value, headers);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _runtime.Dispose();
        }
    }

    /// <summary>
    /// JSON input structure sent to the WASM module.
    /// </summary>
    internal sealed class WasmTransformInput
    {
        public string Topic { get; set; } = string.Empty;
        public int Partition { get; set; }
        public byte[] Key { get; set; } = [];
        public byte[] Value { get; set; } = [];
        public Dictionary<string, byte[]> Headers { get; set; } = [];
        public long Timestamp { get; set; }
        public string Phase { get; set; } = string.Empty;
        public IReadOnlyDictionary<string, string> Config { get; set; } = new Dictionary<string, string>();
    }

    /// <summary>
    /// JSON output structure returned by the WASM module.
    /// </summary>
    internal sealed class WasmTransformOutput
    {
        public bool Dropped { get; set; }
        public byte[]? Key { get; set; }
        public byte[]? Value { get; set; }
        public Dictionary<string, byte[]>? Headers { get; set; }
        public string? RouteTopic { get; set; }
    }
}
