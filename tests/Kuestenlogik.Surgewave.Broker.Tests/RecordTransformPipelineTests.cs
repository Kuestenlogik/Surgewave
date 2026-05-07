using Kuestenlogik.Surgewave.Core.Pipeline;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Contract tests for <see cref="IRecordTransformPipeline"/>. The Surgewave.Wasm
/// implementation lives in its own assembly and needs the WASM runtime; this
/// suite uses a tiny in-process stub to verify the broker-side wiring (config
/// key, topic-binding cache invalidation via <see cref="ITopicLifecycleHook"/>).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RecordTransformPipelineTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;

    public RecordTransformPipelineTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-rtp-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task NoBinding_TransformReturnsInputUnchanged()
    {
        await _logManager.CreateTopicAsync("rt-no-binding", partitionCount: 1);
        var pipeline = new InProcessTransformPipeline(_logManager, configKey: "wasm.transform.plugin.id",
            transform: _ => null);

        var input = new byte[] { 1, 2, 3 };
        Assert.False(pipeline.HasBinding("rt-no-binding"));
        var result = await pipeline.TransformAsync("rt-no-binding", input, CancellationToken.None);
        Assert.True(result.HasValue);
        Assert.Equal(input, result.Value.ToArray());
    }

    [Fact]
    public async Task WithBinding_TransformInvokesProcessAndReturnsNewBytes()
    {
        await _logManager.CreateTopicAsync("rt-bound", partitionCount: 1, config: new()
        {
            ["wasm.transform.plugin.id"] = "uppercase"
        });

        var pipeline = new InProcessTransformPipeline(_logManager, "wasm.transform.plugin.id",
            transform: bytes => bytes.Select(b => (byte)char.ToUpperInvariant((char)b)).ToArray());

        Assert.True(pipeline.HasBinding("rt-bound"));
        var result = await pipeline.TransformAsync("rt-bound", new byte[] { (byte)'a', (byte)'b' }, CancellationToken.None);
        Assert.True(result.HasValue);
        Assert.Equal(new byte[] { (byte)'A', (byte)'B' }, result.Value.ToArray());
    }

    [Fact]
    public async Task WithBinding_TransformReturnsNull_SignalsDrop()
    {
        await _logManager.CreateTopicAsync("rt-drop", partitionCount: 1, config: new()
        {
            ["wasm.transform.plugin.id"] = "drop-everything"
        });
        var pipeline = new InProcessTransformPipeline(_logManager, "wasm.transform.plugin.id",
            transform: _ => null);

        var result = await pipeline.TransformAsync("rt-drop", new byte[] { 1 }, CancellationToken.None);
        Assert.Null(result);
    }

    [Fact]
    public async Task ConfigChange_InvalidatesBindingCache()
    {
        await _logManager.CreateTopicAsync("rt-flip", partitionCount: 1);
        var pipeline = new InProcessTransformPipeline(_logManager, "wasm.transform.plugin.id",
            transform: _ => Array.Empty<byte>());

        Assert.False(pipeline.HasBinding("rt-flip")); // primes the cache → "no binding"

        // Add the binding via UpdateTopicConfig; the lifecycle hook flips the cache.
        Assert.True(_logManager.UpdateTopicConfig("rt-flip", new() { ["wasm.transform.plugin.id"] = "anything" }));

        Assert.True(pipeline.HasBinding("rt-flip"));
    }

    [Fact]
    public async Task TopicDeletion_RemovesCachedBinding()
    {
        await _logManager.CreateTopicAsync("rt-delete", partitionCount: 1, config: new()
        {
            ["wasm.transform.plugin.id"] = "p"
        });
        var pipeline = new InProcessTransformPipeline(_logManager, "wasm.transform.plugin.id",
            transform: _ => Array.Empty<byte>());
        Assert.True(pipeline.HasBinding("rt-delete"));

        await _logManager.DeleteTopicAsync("rt-delete");

        Assert.False(pipeline.HasBinding("rt-delete"));
    }

    /// <summary>
    /// Reusable in-memory pipeline. Behaves the same way as the WASM-backed
    /// <c>WasmRecordTransformPipeline</c> for the cache + binding logic, but
    /// invokes a delegate instead of a WASM module so this assembly doesn't
    /// need the wasmtime native dependency.
    /// </summary>
    private sealed class InProcessTransformPipeline : IRecordTransformPipeline, ITopicLifecycleHook
    {
        private readonly LogManager _logManager;
        private readonly string _configKey;
        private readonly Func<byte[], byte[]?> _transform;
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, string?> _cache = new();

        public InProcessTransformPipeline(LogManager logManager, string configKey, Func<byte[], byte[]?> transform)
        {
            _logManager = logManager;
            _configKey = configKey;
            _transform = transform;
            _logManager.RegisterTopicHook(this);
        }

        public bool HasBinding(string topic) => Resolve(topic) is not null;

        public ValueTask<ReadOnlyMemory<byte>?> TransformAsync(string topic, ReadOnlyMemory<byte> recordBatch, CancellationToken ct)
        {
            if (Resolve(topic) is null) return ValueTask.FromResult<ReadOnlyMemory<byte>?>(recordBatch);
            var output = _transform(recordBatch.ToArray());
            if (output is null || output.Length == 0)
            {
                return ValueTask.FromResult<ReadOnlyMemory<byte>?>(null);
            }
            return ValueTask.FromResult<ReadOnlyMemory<byte>?>(output);
        }

        public Task OnTopicCreatedAsync(TopicLifecycleContext c, CancellationToken ct) { _cache.TryRemove(c.TopicName, out _); return Task.CompletedTask; }
        public Task OnTopicConfigChangedAsync(TopicLifecycleContext c, CancellationToken ct) { _cache.TryRemove(c.TopicName, out _); return Task.CompletedTask; }
        public Task OnTopicDeletedAsync(TopicLifecycleContext c, CancellationToken ct) { _cache.TryRemove(c.TopicName, out _); return Task.CompletedTask; }

        private string? Resolve(string topic) => _cache.GetOrAdd(topic, t =>
        {
            var meta = _logManager.GetTopicMetadata(t);
            if (meta is null) return null;
            return meta.Config.TryGetValue(_configKey, out var v) && !string.IsNullOrEmpty(v) ? v : null;
        });
    }
}
