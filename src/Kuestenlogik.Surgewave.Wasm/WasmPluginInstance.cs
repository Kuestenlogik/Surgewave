using Microsoft.Extensions.Logging;
using Wasmtime;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// A loaded and running WASM plugin instance. Wraps a Wasmtime <see cref="Instance"/>
/// and exposes strongly-typed methods that map to the Surgewave WASM ABI.
/// Thread-safety: individual calls are serialised via <see cref="SemaphoreSlim"/>;
/// do not call concurrently from multiple threads without external synchronisation.
/// </summary>
public sealed class WasmPluginInstance : IAsyncDisposable
{
    private readonly Store _store;
    private readonly Memory? _memory;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);

    // Cached typed ABI function references
    private readonly Func<int>? _pluginInit;
    private readonly Func<int, int, int>? _pluginProcess;
    private readonly Func<int>? _pluginPoll;
    private readonly Func<int, int, int>? _pluginPush;
    private readonly Func<int>? _pluginClose;
    private readonly Func<int, int>? _alloc;
    private readonly Action<int, int>? _dealloc;

    private long _messagesProcessed;
    private long _errorCount;
    private string? _lastError;
    private bool _disposed;

    /// <summary>Unique plugin identifier from the manifest.</summary>
    public string PluginId { get; }

    /// <summary>Full manifest metadata.</summary>
    public WasmPluginManifest Manifest { get; }

    /// <summary>Current lifecycle state.</summary>
    public WasmPluginState State { get; internal set; }

    /// <summary>Timestamp when this instance was loaded.</summary>
    public DateTimeOffset LoadedAt { get; } = DateTimeOffset.UtcNow;

    /// <summary>Total messages successfully processed.</summary>
    public long MessagesProcessed => Interlocked.Read(ref _messagesProcessed);

    /// <summary>Total errors encountered.</summary>
    public long ErrorCount => Interlocked.Read(ref _errorCount);

    /// <summary>Last error message, if any.</summary>
    public string? LastError => _lastError;

    /// <summary>Approximate linear memory usage in bytes.</summary>
    public long MemoryUsageBytes => _memory is not null ? _memory.GetLength() * Memory.PageSize : 0;

    internal WasmPluginInstance(
        Store store,
        Instance instance,
        WasmPluginManifest manifest,
        TimeSpan executionTimeout,
        ILogger logger)
    {
        _store = store;
        _memory = instance.GetMemory("memory");
        _logger = logger;

        PluginId = manifest.Id;
        Manifest = manifest;
        State = WasmPluginState.Loading;

        // Cache exported functions using typed accessors
        _pluginInit = instance.GetFunction<int>(WasmAbi.PluginInit);
        _pluginProcess = instance.GetFunction<int, int, int>(WasmAbi.PluginProcess);
        _pluginPoll = instance.GetFunction<int>(WasmAbi.PluginPoll);
        _pluginPush = instance.GetFunction<int, int, int>(WasmAbi.PluginPush);
        _pluginClose = instance.GetFunction<int>(WasmAbi.PluginClose);
        _alloc = instance.GetFunction<int, int>(WasmAbi.Alloc);
        _dealloc = instance.GetAction<int, int>(WasmAbi.Dealloc);
    }

    /// <summary>
    /// Calls <c>plugin_init()</c> on the WASM module. Returns <c>true</c> if the module
    /// returned 0 (success).
    /// </summary>
    public async Task<bool> InitializeAsync()
    {
        if (_pluginInit is null)
        {
            _logger.LogWarning("[WASM:{PluginId}] Module does not export {Func}, skipping init", PluginId, WasmAbi.PluginInit);
            State = WasmPluginState.Ready;
            return true;
        }

        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            var result = _pluginInit();
            if (result == 0)
            {
                State = WasmPluginState.Ready;
                _logger.LogInformation("[WASM:{PluginId}] Initialised successfully", PluginId);
                return true;
            }

            State = WasmPluginState.Failed;
            _lastError = $"plugin_init returned {result}";
            _logger.LogError("[WASM:{PluginId}] {Error}", PluginId, _lastError);
            return false;
        }
        catch (Exception ex)
        {
            State = WasmPluginState.Failed;
            _lastError = ex.Message;
            Interlocked.Increment(ref _errorCount);
            _logger.LogError(ex, "[WASM:{PluginId}] Init failed", PluginId);
            return false;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// For Transform / Function plugins: process a single message.
    /// Writes <paramref name="input"/> into WASM memory, calls <c>plugin_process</c>,
    /// and reads the result back.
    /// Returns <c>null</c> if the module signals "drop this message" (returns 0).
    /// </summary>
    public async Task<byte[]?> ProcessAsync(byte[] input, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_pluginProcess is null)
            throw new InvalidOperationException($"Plugin '{PluginId}' does not export {WasmAbi.PluginProcess}");

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            State = WasmPluginState.Running;

            // Allocate buffer in WASM and copy input
            var ptr = AllocInWasm(input.Length);
            WriteToWasmMemory(ptr, input);

            // Call plugin_process(ptr, len) -> resultPtr
            var resultPtr = _pluginProcess(ptr, input.Length);

            // Free input buffer
            DeallocInWasm(ptr, input.Length);

            if (resultPtr == 0)
            {
                // Module signals "drop" / no output
                Interlocked.Increment(ref _messagesProcessed);
                return null;
            }

            // Read result: first 4 bytes at resultPtr are the length
            var resultLen = ReadInt32FromWasm(resultPtr);
            if (resultLen <= 0)
            {
                Interlocked.Increment(ref _messagesProcessed);
                return null;
            }

            var result = ReadBytesFromWasm(resultPtr + 4, resultLen);
            DeallocInWasm(resultPtr, resultLen + 4);

            Interlocked.Increment(ref _messagesProcessed);
            return result;
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _errorCount);
            _lastError = ex.Message;
            _logger.LogError(ex, "[WASM:{PluginId}] ProcessAsync failed", PluginId);
            throw;
        }
        finally
        {
            if (State == WasmPluginState.Running)
                State = WasmPluginState.Ready;
            _gate.Release();
        }
    }

    /// <summary>
    /// For Source plugins: poll for new data.
    /// Calls <c>plugin_poll()</c> and reads the output buffer.
    /// Returns an empty list if no data is available.
    /// </summary>
    public async Task<IReadOnlyList<byte[]>> PollAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_pluginPoll is null)
            throw new InvalidOperationException($"Plugin '{PluginId}' does not export {WasmAbi.PluginPoll}");

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            State = WasmPluginState.Running;

            var resultPtr = _pluginPoll();
            if (resultPtr == 0)
                return [];

            // Read result: [count: i32][len1: i32][data1...][len2: i32][data2...]...
            var count = ReadInt32FromWasm(resultPtr);
            if (count <= 0)
                return [];

            var results = new List<byte[]>(count);
            var offset = resultPtr + 4;
            for (var i = 0; i < count; i++)
            {
                var len = ReadInt32FromWasm(offset);
                offset += 4;
                if (len > 0)
                {
                    results.Add(ReadBytesFromWasm(offset, len));
                    offset += len;
                }
            }

            Interlocked.Add(ref _messagesProcessed, results.Count);
            return results;
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _errorCount);
            _lastError = ex.Message;
            _logger.LogError(ex, "[WASM:{PluginId}] PollAsync failed", PluginId);
            throw;
        }
        finally
        {
            if (State == WasmPluginState.Running)
                State = WasmPluginState.Ready;
            _gate.Release();
        }
    }

    /// <summary>
    /// For Sink plugins: push data to the module.
    /// Calls <c>plugin_push(ptr, len)</c>.
    /// </summary>
    public async Task PushAsync(byte[] data, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_pluginPush is null)
            throw new InvalidOperationException($"Plugin '{PluginId}' does not export {WasmAbi.PluginPush}");

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            State = WasmPluginState.Running;

            var ptr = AllocInWasm(data.Length);
            WriteToWasmMemory(ptr, data);

            var result = _pluginPush(ptr, data.Length);
            DeallocInWasm(ptr, data.Length);

            if (result != 0)
            {
                Interlocked.Increment(ref _errorCount);
                _lastError = $"plugin_push returned {result}";
                throw new InvalidOperationException(_lastError);
            }

            Interlocked.Increment(ref _messagesProcessed);
        }
        catch (Exception ex) when (ex is not InvalidOperationException)
        {
            Interlocked.Increment(ref _errorCount);
            _lastError = ex.Message;
            _logger.LogError(ex, "[WASM:{PluginId}] PushAsync failed", PluginId);
            throw;
        }
        finally
        {
            if (State == WasmPluginState.Running)
                State = WasmPluginState.Ready;
            _gate.Release();
        }
    }

    /// <summary>
    /// Calls <c>plugin_close()</c> for graceful shutdown.
    /// </summary>
    public async Task CloseAsync()
    {
        if (_disposed) return;

        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_pluginClose is not null)
            {
                _pluginClose();
            }

            State = WasmPluginState.Stopped;
            _logger.LogInformation("[WASM:{PluginId}] Closed", PluginId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[WASM:{PluginId}] Error during close", PluginId);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Returns a status snapshot suitable for the REST API.
    /// </summary>
    public WasmPluginStatus GetStatus()
    {
        return new WasmPluginStatus(
            PluginId,
            Manifest.Name,
            Manifest.Type,
            State,
            MemoryUsageBytes,
            MessagesProcessed,
            ErrorCount,
            LoadedAt,
            Manifest.Version,
            _lastError);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await CloseAsync().ConfigureAwait(false);
        _gate.Dispose();
        _store.Dispose();
    }

    // ────────── Private helpers ──────────

    private int AllocInWasm(int size)
    {
        if (_alloc is null)
            throw new InvalidOperationException($"Plugin '{PluginId}' does not export {WasmAbi.Alloc}");

        return _alloc(size);
    }

    private void DeallocInWasm(int ptr, int size)
    {
        _dealloc?.Invoke(ptr, size);
    }

    private void WriteToWasmMemory(int ptr, byte[] data)
    {
        if (_memory is null)
            throw new InvalidOperationException("WASM module has no exported memory");

        var span = _memory.GetSpan(ptr, data.Length);
        data.AsSpan().CopyTo(span);
    }

    private byte[] ReadBytesFromWasm(int ptr, int len)
    {
        if (_memory is null)
            throw new InvalidOperationException("WASM module has no exported memory");

        var span = _memory.GetSpan(ptr, len);
        return span.ToArray();
    }

    private int ReadInt32FromWasm(int ptr)
    {
        if (_memory is null)
            throw new InvalidOperationException("WASM module has no exported memory");

        var span = _memory.GetSpan(ptr, 4);
        return BitConverter.ToInt32(span);
    }
}
