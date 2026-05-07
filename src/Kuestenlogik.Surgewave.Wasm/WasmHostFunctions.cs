using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using Wasmtime;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Defines the host functions that Surgewave exports into the WASM module's import namespace.
/// These allow the WASM module to produce messages, read configuration, log, and
/// interact with a key-value state store.
/// </summary>
public sealed class WasmHostFunctions
{
    private readonly ILogger _logger;
    private readonly Dictionary<string, string> _config;
    private readonly ConcurrentDictionary<string, byte[]> _stateStore = new();
    private readonly Action<string, byte[]?, byte[]>? _produceCallback;

    /// <summary>
    /// Creates a new host function set for a WASM plugin instance.
    /// </summary>
    /// <param name="logger">Logger for <c>surgewave_log</c> calls.</param>
    /// <param name="config">Configuration dictionary for <c>surgewave_get_config</c>.</param>
    /// <param name="produceCallback">
    /// Optional callback invoked when the WASM module calls <c>surgewave_produce</c>.
    /// Parameters are (topic, key, value).
    /// </param>
    public WasmHostFunctions(
        ILogger logger,
        Dictionary<string, string> config,
        Action<string, byte[]?, byte[]>? produceCallback = null)
    {
        _logger = logger;
        _config = config;
        _produceCallback = produceCallback;
    }

    /// <summary>
    /// Registers all host functions into a Wasmtime <see cref="Linker"/>.
    /// </summary>
    /// <param name="linker">The Wasmtime linker to define imports on.</param>
    /// <param name="store">The Wasmtime store associated with the instance.</param>
    /// <param name="getMemory">Deferred accessor for the module's exported memory.</param>
    public void Register(Linker linker, Store store, Func<Memory?> getMemory)
    {
        ArgumentNullException.ThrowIfNull(linker);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(getMemory);

        // surgewave_log(level: i32, msg_ptr: i32, msg_len: i32)
        linker.DefineFunction("env", WasmAbi.HostLog,
            (Caller caller, int level, int msgPtr, int msgLen) =>
            {
                var memory = getMemory();
                if (memory is null) return;
                var msg = ReadStringFromMemory(memory, msgPtr, msgLen);
                var logLevel = level switch
                {
                    0 => LogLevel.Trace,
                    1 => LogLevel.Debug,
                    2 => LogLevel.Information,
                    3 => LogLevel.Warning,
                    4 => LogLevel.Error,
                    _ => LogLevel.Critical
                };
                _logger.Log(logLevel, "[WASM] {Message}", msg);
            });

        // surgewave_get_config(key_ptr: i32, key_len: i32, out_ptr: i32, out_len: i32) -> i32
        linker.DefineFunction("env", WasmAbi.HostGetConfig,
            (Caller caller, int keyPtr, int keyLen, int outPtr, int outLen) =>
            {
                var memory = getMemory();
                if (memory is null) return -1;
                var key = ReadStringFromMemory(memory, keyPtr, keyLen);
                if (!_config.TryGetValue(key, out var value))
                    return -1;

                var bytes = Encoding.UTF8.GetBytes(value);
                var written = Math.Min(bytes.Length, outLen);
                WriteToMemory(memory, outPtr, bytes.AsSpan(0, written));
                return written;
            });

        // surgewave_produce(topic_ptr, topic_len, key_ptr, key_len, value_ptr, value_len) -> i32
        linker.DefineFunction("env", WasmAbi.HostProduce,
            (Caller caller, int topicPtr, int topicLen, int keyPtr, int keyLen, int valuePtr, int valueLen) =>
            {
                var memory = getMemory();
                if (memory is null) return -1;

                var topic = ReadStringFromMemory(memory, topicPtr, topicLen);
                byte[]? key = keyLen > 0 ? ReadBytesFromMemory(memory, keyPtr, keyLen) : null;
                var value = ReadBytesFromMemory(memory, valuePtr, valueLen);

                try
                {
                    _produceCallback?.Invoke(topic, key, value);
                    return 0;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[WASM] surgewave_produce failed for topic {Topic}", topic);
                    return -1;
                }
            });

        // surgewave_state_get(key_ptr, key_len, out_ptr, out_len) -> i32
        linker.DefineFunction("env", WasmAbi.HostStateGet,
            (Caller caller, int keyPtr, int keyLen, int outPtr, int outLen) =>
            {
                var memory = getMemory();
                if (memory is null) return -1;
                var key = ReadStringFromMemory(memory, keyPtr, keyLen);
                if (!_stateStore.TryGetValue(key, out var value))
                    return -1;

                var written = Math.Min(value.Length, outLen);
                WriteToMemory(memory, outPtr, value.AsSpan(0, written));
                return written;
            });

        // surgewave_state_put(key_ptr, key_len, value_ptr, value_len) -> i32
        linker.DefineFunction("env", WasmAbi.HostStatePut,
            (Caller caller, int keyPtr, int keyLen, int valuePtr, int valueLen) =>
            {
                var memory = getMemory();
                if (memory is null) return -1;
                var key = ReadStringFromMemory(memory, keyPtr, keyLen);
                var value = ReadBytesFromMemory(memory, valuePtr, valueLen);
                _stateStore[key] = value;
                return 0;
            });
    }

    private static string ReadStringFromMemory(Memory memory, int ptr, int len)
    {
        if (len <= 0) return string.Empty;
        var span = memory.GetSpan(ptr, len);
        return Encoding.UTF8.GetString(span);
    }

    private static byte[] ReadBytesFromMemory(Memory memory, int ptr, int len)
    {
        if (len <= 0) return [];
        var span = memory.GetSpan(ptr, len);
        return span.ToArray();
    }

    private static void WriteToMemory(Memory memory, int ptr, ReadOnlySpan<byte> data)
    {
        var dest = memory.GetSpan(ptr, data.Length);
        data.CopyTo(dest);
    }
}
