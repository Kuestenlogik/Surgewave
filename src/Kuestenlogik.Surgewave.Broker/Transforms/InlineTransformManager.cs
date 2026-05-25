using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Transforms;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Transforms;

/// <summary>
/// Manages loaded inline transforms (both C# plugins and WASM modules)
/// and their per-topic bindings. Thread-safe via ConcurrentDictionary.
/// </summary>
public sealed partial class InlineTransformManager : IDisposable
{
    private readonly TransformPluginLoader _pluginLoader;
    private readonly ILogger<InlineTransformManager> _logger;

    /// <summary>
    /// All loaded transforms indexed by name.
    /// </summary>
    private readonly ConcurrentDictionary<string, IInlineTransform> _transforms = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Per-topic, per-phase ordered list of transform names.
    /// Key = "topicName:Produce" or "topicName:Fetch".
    /// </summary>
    private readonly ConcurrentDictionary<string, List<string>> _topicBindings = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Cached pipelines per topic/phase. Invalidated when bindings change.
    /// </summary>
    private readonly ConcurrentDictionary<string, InlineTransformPipeline> _pipelineCache = new(StringComparer.OrdinalIgnoreCase);

    public InlineTransformManager(TransformPluginLoader pluginLoader, ILogger<InlineTransformManager> logger)
    {
        _pluginLoader = pluginLoader;
        _logger = logger;
    }

    /// <summary>
    /// Number of loaded transforms.
    /// </summary>
    public int TransformCount => _transforms.Count;

    /// <summary>
    /// Loads a C# transform plugin by fully-qualified class name from already-loaded assemblies,
    /// or directly registers an existing transform instance.
    /// </summary>
    public void LoadTransform(IInlineTransform transform, IReadOnlyDictionary<string, string> config)
    {
        transform.Initialize(config);
        _transforms[transform.Name] = transform;
        LogTransformLoaded(transform.Name);
    }

    /// <summary>
    /// Loads a WASM-based transform from a .wasm file path.
    /// </summary>
    public void LoadWasmTransform(string name, string wasmPath, IWasmRuntime runtime, IReadOnlyDictionary<string, string> config)
    {
        var transform = new WasmInlineTransform(name, runtime, wasmPath);
        transform.Initialize(config);
        _transforms[name] = transform;
        LogWasmTransformLoaded(name, wasmPath);
    }

    /// <summary>
    /// Registers a transform name for a specific topic and phase.
    /// Order of registration determines execution order.
    /// </summary>
    public void RegisterTopicTransform(string topicName, TransformPhase phase, string transformName)
    {
        var key = BuildBindingKey(topicName, phase);

        _topicBindings.AddOrUpdate(
            key,
            _ => [transformName],
            (_, existing) =>
            {
                if (!existing.Contains(transformName, StringComparer.OrdinalIgnoreCase))
                {
                    existing.Add(transformName);
                }
                return existing;
            });

        // Invalidate cached pipeline for this topic/phase
        _pipelineCache.TryRemove(key, out _);

        LogTopicTransformRegistered(topicName, phase.ToString(), transformName);
    }

    /// <summary>
    /// Gets the transform pipeline for a topic and phase. Returns an empty pipeline
    /// if no transforms are configured.
    /// </summary>
    public InlineTransformPipeline GetPipelineForTopic(string topicName, TransformPhase phase)
    {
        var key = BuildBindingKey(topicName, phase);

        return _pipelineCache.GetOrAdd(key, k =>
        {
            if (!_topicBindings.TryGetValue(k, out var transformNames))
            {
                return InlineTransformPipeline.Empty;
            }

            var transforms = new List<IInlineTransform>();
            foreach (var name in transformNames)
            {
                if (_transforms.TryGetValue(name, out var transform))
                {
                    transforms.Add(transform);
                }
                else
                {
                    LogTransformNotFound(name, topicName);
                }
            }

            return transforms.Count > 0
                ? new InlineTransformPipeline(transforms)
                : InlineTransformPipeline.Empty;
        });
    }

    /// <summary>
    /// Gets the ordered list of transform names bound to a topic and phase.
    /// Returns an empty list if no transforms are configured.
    /// </summary>
    public IReadOnlyList<string> GetTransformNamesForTopic(string topicName, TransformPhase phase)
    {
        var key = BuildBindingKey(topicName, phase);
        return _topicBindings.TryGetValue(key, out var names) ? names : [];
    }

    /// <summary>
    /// Parses topic config keys "surgewave.transform.produce" and "surgewave.transform.fetch"
    /// for comma-separated transform names and registers them.
    /// </summary>
    public void ParseAndRegisterTopicConfig(string topicName, IReadOnlyDictionary<string, string> topicConfig)
    {
        if (topicConfig.TryGetValue("surgewave.transform.produce", out var produceTransforms))
        {
            foreach (var name in ParseTransformNames(produceTransforms))
            {
                RegisterTopicTransform(topicName, TransformPhase.Produce, name);
            }
        }

        if (topicConfig.TryGetValue("surgewave.transform.fetch", out var fetchTransforms))
        {
            foreach (var name in ParseTransformNames(fetchTransforms))
            {
                RegisterTopicTransform(topicName, TransformPhase.Fetch, name);
            }
        }
    }

    /// <summary>
    /// Parses a comma-separated list of transform names, trimming whitespace.
    /// </summary>
    internal static IReadOnlyList<string> ParseTransformNames(string commaSeparated)
    {
        if (string.IsNullOrWhiteSpace(commaSeparated))
        {
            return [];
        }

        var names = new List<string>();
        foreach (var segment in commaSeparated.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (segment.Length > 0)
            {
                names.Add(segment);
            }
        }

        return names;
    }

    public void Dispose()
    {
        foreach (var transform in _transforms.Values)
        {
            try
            {
                transform.Dispose();
            }
            catch (Exception ex)
            {
                LogTransformDisposeFailed(transform.Name, ex);
            }
        }

        _transforms.Clear();
        _topicBindings.Clear();
        _pipelineCache.Clear();
        _pluginLoader.Dispose();
    }

    private static string BuildBindingKey(string topicName, TransformPhase phase)
        => $"{topicName}:{phase}";

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded inline transform: {TransformName}")]
    private partial void LogTransformLoaded(string transformName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded WASM inline transform: {TransformName} from {WasmPath}")]
    private partial void LogWasmTransformLoaded(string transformName, string wasmPath);

    [LoggerMessage(Level = LogLevel.Information, Message = "Registered transform {TransformName} for topic {TopicName} phase {Phase}")]
    private partial void LogTopicTransformRegistered(string topicName, string phase, string transformName);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Transform {TransformName} not found when building pipeline for topic {TopicName}")]
    private partial void LogTransformNotFound(string transformName, string topicName);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error disposing transform: {TransformName}")]
    private partial void LogTransformDisposeFailed(string transformName, Exception ex);
}
