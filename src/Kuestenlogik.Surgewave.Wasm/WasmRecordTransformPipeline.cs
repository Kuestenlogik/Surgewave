using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Pipeline;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Hot-path implementation of <see cref="IRecordTransformPipeline"/> backed by the
/// Surgewave WASM runtime — Surgewave's analogue of Redpanda Data Transforms (G7 of the
/// competitive gap analysis). A topic opts in by setting the
/// <see cref="ConfigKey"/> entry in its config to the id of a loaded
/// <see cref="WasmPluginInstance"/>; on every produce-batch the broker invokes
/// the plugin's <c>plugin_process</c> entry point against the raw record-batch
/// bytes. The plugin can return modified bytes, the unchanged input, or an empty
/// buffer to signal "drop".
/// </summary>
/// <remarks>
/// The pipeline caches the topic-to-plugin binding and invalidates the cache via
/// the topic-lifecycle hook so a config change goes live without a broker
/// restart. Lookups on a topic with no binding cost a single dictionary read
/// followed by an <see cref="bool"/> flag check, so the no-binding hot-path stays
/// allocation-free.
/// </remarks>
public sealed class WasmRecordTransformPipeline : IRecordTransformPipeline, ITopicLifecycleHook
{
    /// <summary>
    /// Topic-config key that pins a topic to a WASM transform plugin id. When the
    /// value is empty / missing the broker bypasses the pipeline for that topic.
    /// </summary>
    public const string ConfigKey = "wasm.transform.plugin.id";

    private readonly WasmPluginManager _manager;
    private readonly ILogger<WasmRecordTransformPipeline> _logger;
    private readonly LogManager _logManager;

    // Per-topic plugin id, refreshed lazily on first lookup or when the topic
    // lifecycle hook fires. A null entry encodes "topic exists but has no
    // transform binding" — distinct from "topic was never looked up".
    private readonly ConcurrentDictionary<string, string?> _bindingCache = new(StringComparer.Ordinal);

    public WasmRecordTransformPipeline(
        WasmPluginManager manager,
        LogManager logManager,
        ILogger<WasmRecordTransformPipeline> logger)
    {
        _manager = manager;
        _logManager = logManager;
        _logger = logger;
        _logManager.RegisterTopicHook(this);
    }

    public bool HasBinding(string topic)
    {
        if (string.IsNullOrEmpty(topic)) return false;
        return ResolvePluginId(topic) is not null;
    }

    public async ValueTask<ReadOnlyMemory<byte>?> TransformAsync(
        string topic,
        ReadOnlyMemory<byte> recordBatch,
        CancellationToken cancellationToken)
    {
        var pluginId = ResolvePluginId(topic);
        if (pluginId is null) return recordBatch;

        var plugin = _manager.GetPlugin(pluginId);
        if (plugin is null)
        {
            _logger.LogWarning(
                "Topic {Topic} pins WASM plugin '{PluginId}' but no such plugin is loaded — passing the batch through unchanged",
                topic, pluginId);
            return recordBatch;
        }

        // The current WasmPluginInstance.ProcessAsync surface is byte[]. We pay an
        // allocation when copying out of the input slice; if/when WasmPluginInstance
        // grows a Memory-aware overload this can switch to zero-copy.
        var input = recordBatch.ToArray();
        var output = await plugin.ProcessAsync(input, cancellationToken).ConfigureAwait(false);

        // Plugin convention: null == drop, empty == drop (defensive — some
        // plugins return Array.Empty<byte>() instead of null for the same intent),
        // anything else == use the new bytes.
        if (output is null || output.Length == 0)
        {
            return null;
        }
        return output;
    }

    private string? ResolvePluginId(string topic)
    {
        return _bindingCache.GetOrAdd(topic, t =>
        {
            var metadata = _logManager.GetTopicMetadata(t);
            if (metadata is null) return null;
            return metadata.Config.TryGetValue(ConfigKey, out var pluginId) && !string.IsNullOrEmpty(pluginId)
                ? pluginId
                : null;
        });
    }

    // ─────────────────────────────────────────────────────────────────────
    // ITopicLifecycleHook — invalidate the per-topic cache when the binding
    // could have changed. We don't rebuild eagerly; the next produce will
    // populate the cache lazily.
    // ─────────────────────────────────────────────────────────────────────

    public Task OnTopicCreatedAsync(TopicLifecycleContext context, CancellationToken cancellationToken)
    {
        _bindingCache.TryRemove(context.TopicName, out _);
        return Task.CompletedTask;
    }

    public Task OnTopicConfigChangedAsync(TopicLifecycleContext context, CancellationToken cancellationToken)
    {
        _bindingCache.TryRemove(context.TopicName, out _);
        return Task.CompletedTask;
    }

    public Task OnTopicDeletedAsync(TopicLifecycleContext context, CancellationToken cancellationToken)
    {
        _bindingCache.TryRemove(context.TopicName, out _);
        return Task.CompletedTask;
    }
}
