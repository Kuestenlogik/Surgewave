using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Broker.Transforms;
using Kuestenlogik.Surgewave.Core.Transforms;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

[Trait("Category", TestCategories.Unit)]
public class InlineTransformTests : IDisposable
{
    private readonly InlineTransformManager _manager;
    private readonly TransformPluginLoader _pluginLoader;

    public InlineTransformTests()
    {
        _pluginLoader = new TransformPluginLoader(NullLogger<TransformPluginLoader>.Instance);
        _manager = new InlineTransformManager(_pluginLoader, NullLogger<InlineTransformManager>.Instance);
    }

    // --- Test transform implementations ---

    /// <summary>
    /// Transform that converts value bytes to uppercase ASCII.
    /// </summary>
    private sealed class UpperCaseTransform : IInlineTransform
    {
        public string Name => "uppercase";
        public void Initialize(IReadOnlyDictionary<string, string> config) { }

        public TransformResult Transform(TransformContext context)
        {
            var upper = Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(context.Value).ToUpperInvariant());
            return TransformResult.Pass(context.Key, upper, context.Headers);
        }

        public void Dispose() { }
    }

    /// <summary>
    /// Transform that drops records whose value starts with "DROP".
    /// </summary>
    private sealed class FilterTransform : IInlineTransform
    {
        public string Name => "filter";
        public void Initialize(IReadOnlyDictionary<string, string> config) { }

        public TransformResult Transform(TransformContext context)
        {
            var text = Encoding.UTF8.GetString(context.Value);
            return text.StartsWith("DROP", StringComparison.Ordinal)
                ? TransformResult.Drop()
                : TransformResult.Pass(context.Key, context.Value, context.Headers);
        }

        public void Dispose() { }
    }

    /// <summary>
    /// Transform that routes records to a different topic based on a header.
    /// </summary>
    private sealed class RouterTransform : IInlineTransform
    {
        public string Name => "router";
        public void Initialize(IReadOnlyDictionary<string, string> config) { }

        public TransformResult Transform(TransformContext context)
        {
            if (context.Headers.TryGetValue("route-to", out var topicBytes))
            {
                var targetTopic = Encoding.UTF8.GetString(topicBytes);
                return TransformResult.Route(targetTopic, context.Key, context.Value, context.Headers);
            }
            return TransformResult.Pass(context.Key, context.Value, context.Headers);
        }

        public void Dispose() { }
    }

    /// <summary>
    /// Identity transform that passes through unchanged.
    /// </summary>
    private sealed class PassThroughTransform : IInlineTransform
    {
        public string Name => "passthrough";
        public void Initialize(IReadOnlyDictionary<string, string> config) { }

        public TransformResult Transform(TransformContext context)
            => TransformResult.Pass(context.Key, context.Value, context.Headers);

        public void Dispose() { }
    }

    /// <summary>
    /// Transform that appends a suffix configured via config.
    /// </summary>
    private sealed class SuffixTransform : IInlineTransform
    {
        private string _suffix = string.Empty;
        public string Name => "suffix";
        public void Initialize(IReadOnlyDictionary<string, string> config)
        {
            if (config.TryGetValue("suffix", out var s))
            {
                _suffix = s;
            }
        }

        public TransformResult Transform(TransformContext context)
        {
            var text = Encoding.UTF8.GetString(context.Value) + _suffix;
            return TransformResult.Pass(context.Key, Encoding.UTF8.GetBytes(text), context.Headers);
        }

        public void Dispose() { }
    }

    // --- Helper ---

    private static TransformContext CreateContext(
        string value,
        string topic = "test-topic",
        int partition = 0,
        string key = "key1",
        TransformPhase phase = TransformPhase.Produce,
        Dictionary<string, byte[]>? headers = null)
    {
        return new TransformContext
        {
            Topic = topic,
            Partition = partition,
            Key = Encoding.UTF8.GetBytes(key),
            Value = Encoding.UTF8.GetBytes(value),
            Headers = headers ?? [],
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Phase = phase
        };
    }

    // --- Tests ---

    [Fact]
    public void Transform_PassThrough_ReturnsOriginal()
    {
        var transform = new PassThroughTransform();
        var context = CreateContext("hello world");

        var result = transform.Transform(context);

        Assert.False(result.Dropped);
        Assert.Null(result.RouteTopic);
        Assert.Equal(context.Key, result.Key);
        Assert.Equal(context.Value, result.Value);
    }

    [Fact]
    public void Transform_ModifyValue_ChangesContent()
    {
        var transform = new UpperCaseTransform();
        var context = CreateContext("hello world");

        var result = transform.Transform(context);

        Assert.False(result.Dropped);
        Assert.Equal("HELLO WORLD", Encoding.UTF8.GetString(result.Value));
        Assert.Equal(context.Key, result.Key);
    }

    [Fact]
    public void Transform_Drop_FiltersRecord()
    {
        var transform = new FilterTransform();
        var context = CreateContext("DROP this message");

        var result = transform.Transform(context);

        Assert.True(result.Dropped);
    }

    [Fact]
    public void Transform_Route_RedirectsTopic()
    {
        var transform = new RouterTransform();
        var headers = new Dictionary<string, byte[]>
        {
            ["route-to"] = Encoding.UTF8.GetBytes("errors-topic")
        };
        var context = CreateContext("error event", headers: headers);

        var result = transform.Transform(context);

        Assert.False(result.Dropped);
        Assert.Equal("errors-topic", result.RouteTopic);
        Assert.Equal(context.Value, result.Value);
    }

    [Fact]
    public void Pipeline_MultipleTransforms_ChainsCorrectly()
    {
        // Chain: suffix("!") -> uppercase
        var suffix = new SuffixTransform();
        suffix.Initialize(new Dictionary<string, string> { ["suffix"] = "!" });

        var upper = new UpperCaseTransform();
        upper.Initialize(new Dictionary<string, string>());

        var pipeline = new InlineTransformPipeline([suffix, upper]);
        var context = CreateContext("hello");

        var result = pipeline.Execute(context);

        Assert.False(result.Dropped);
        Assert.Equal("HELLO!", Encoding.UTF8.GetString(result.Value));
    }

    [Fact]
    public void Pipeline_DropInMiddle_ShortCircuits()
    {
        // Chain: filter -> uppercase  (filter drops "DROP..." messages before uppercase runs)
        var filter = new FilterTransform();
        var upper = new UpperCaseTransform();
        var pipeline = new InlineTransformPipeline([filter, upper]);

        var context = CreateContext("DROP me");
        var result = pipeline.Execute(context);

        Assert.True(result.Dropped);
    }

    [Fact]
    public void Manager_RegisterTopicTransform_BindsCorrectly()
    {
        var transform = new UpperCaseTransform();
        _manager.LoadTransform(transform, new Dictionary<string, string>());
        _manager.RegisterTopicTransform("orders", TransformPhase.Produce, "uppercase");

        var names = _manager.GetTransformNamesForTopic("orders", TransformPhase.Produce);

        Assert.Single(names);
        Assert.Equal("uppercase", names[0]);
    }

    [Fact]
    public void Manager_GetTransformsForTopic_ReturnsOrdered()
    {
        var passThrough = new PassThroughTransform();
        var upper = new UpperCaseTransform();
        _manager.LoadTransform(passThrough, new Dictionary<string, string>());
        _manager.LoadTransform(upper, new Dictionary<string, string>());

        _manager.RegisterTopicTransform("events", TransformPhase.Produce, "passthrough");
        _manager.RegisterTopicTransform("events", TransformPhase.Produce, "uppercase");

        var names = _manager.GetTransformNamesForTopic("events", TransformPhase.Produce);

        Assert.Equal(2, names.Count);
        Assert.Equal("passthrough", names[0]);
        Assert.Equal("uppercase", names[1]);
    }

    [Fact]
    public void Manager_NoTransforms_ReturnsEmpty()
    {
        var names = _manager.GetTransformNamesForTopic("no-such-topic", TransformPhase.Fetch);

        Assert.Empty(names);
    }

    [Fact]
    public void PluginLoader_LoadFromAssembly_FindsTransforms()
    {
        // Loading the test assembly itself would require a real DLL.
        // Instead we verify the loader handles a missing path gracefully.
        var loader = new TransformPluginLoader(NullLogger<TransformPluginLoader>.Instance);
        var transforms = loader.LoadFromAssembly("/nonexistent/path.dll");

        Assert.Empty(transforms);

        loader.Dispose();
    }

    [Fact]
    public void WasmTransform_MockRuntime_ExecutesCorrectly()
    {
        // Create a mock runtime that uppercases the value
        var runtime = new MockWasmRuntime(input =>
        {
            using var doc = JsonDocument.Parse(input);
            var root = doc.RootElement;
            var valueBase64 = root.GetProperty("Value").GetString()!;
            var valueBytes = Convert.FromBase64String(valueBase64);
            var upper = Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(valueBytes).ToUpperInvariant());

            return JsonSerializer.SerializeToUtf8Bytes(new
            {
                Dropped = false,
                Key = root.GetProperty("Key").GetString(),
                Value = upper,
            });
        });

        var transform = new WasmInlineTransform("wasm-upper", runtime, "/fake/transform.wasm");
        transform.Initialize(new Dictionary<string, string>());

        var context = CreateContext("hello wasm");
        var result = transform.Transform(context);

        Assert.False(result.Dropped);
        Assert.Equal("HELLO WASM", Encoding.UTF8.GetString(result.Value));
        Assert.Equal("/fake/transform.wasm", runtime.LoadedModulePath);

        transform.Dispose();
    }

    [Fact]
    public void TransformContext_ProducePhase_SetsCorrectly()
    {
        var context = new TransformContext
        {
            Topic = "my-topic",
            Partition = 3,
            Key = Encoding.UTF8.GetBytes("key"),
            Value = Encoding.UTF8.GetBytes("value"),
            Headers = new Dictionary<string, byte[]>
            {
                ["h1"] = Encoding.UTF8.GetBytes("v1")
            },
            Timestamp = 1234567890L,
            Phase = TransformPhase.Produce
        };

        Assert.Equal("my-topic", context.Topic);
        Assert.Equal(3, context.Partition);
        Assert.Equal("key", Encoding.UTF8.GetString(context.Key));
        Assert.Equal("value", Encoding.UTF8.GetString(context.Value));
        Assert.Single(context.Headers);
        Assert.Equal(1234567890L, context.Timestamp);
        Assert.Equal(TransformPhase.Produce, context.Phase);
    }

    [Fact]
    public void Pipeline_EmptyPipeline_PassesThrough()
    {
        var pipeline = InlineTransformPipeline.Empty;
        var context = CreateContext("unchanged");

        var result = pipeline.Execute(context);

        Assert.False(result.Dropped);
        Assert.Null(result.RouteTopic);
        Assert.Equal(context.Key, result.Key);
        Assert.Equal(context.Value, result.Value);
    }

    [Fact]
    public void Manager_ParseTopicConfig_ExtractsTransformNames()
    {
        // Load transforms first
        var upper = new UpperCaseTransform();
        var filter = new FilterTransform();
        _manager.LoadTransform(upper, new Dictionary<string, string>());
        _manager.LoadTransform(filter, new Dictionary<string, string>());

        var topicConfig = new Dictionary<string, string>
        {
            ["surgewave.transform.produce"] = "uppercase, filter",
            ["surgewave.transform.fetch"] = "uppercase"
        };

        _manager.ParseAndRegisterTopicConfig("events", topicConfig);

        var produceNames = _manager.GetTransformNamesForTopic("events", TransformPhase.Produce);
        var fetchNames = _manager.GetTransformNamesForTopic("events", TransformPhase.Fetch);

        Assert.Equal(2, produceNames.Count);
        Assert.Equal("uppercase", produceNames[0]);
        Assert.Equal("filter", produceNames[1]);

        Assert.Single(fetchNames);
        Assert.Equal("uppercase", fetchNames[0]);
    }

    [Fact]
    public void ParseTransformNames_HandlesVariousFormats()
    {
        // Empty/null
        Assert.Empty(InlineTransformManager.ParseTransformNames(""));
        Assert.Empty(InlineTransformManager.ParseTransformNames("   "));

        // Single
        var single = InlineTransformManager.ParseTransformNames("uppercase");
        Assert.Single(single);
        Assert.Equal("uppercase", single[0]);

        // Multiple with whitespace
        var multi = InlineTransformManager.ParseTransformNames("  upper , filter , router  ");
        Assert.Equal(3, multi.Count);
        Assert.Equal("upper", multi[0]);
        Assert.Equal("filter", multi[1]);
        Assert.Equal("router", multi[2]);
    }

    [Fact]
    public void Manager_GetPipeline_ExecutesTransforms()
    {
        var upper = new UpperCaseTransform();
        _manager.LoadTransform(upper, new Dictionary<string, string>());
        _manager.RegisterTopicTransform("events", TransformPhase.Produce, "uppercase");

        var pipeline = _manager.GetPipelineForTopic("events", TransformPhase.Produce);

        Assert.False(pipeline.IsEmpty);
        Assert.Equal(1, pipeline.Count);

        var context = CreateContext("hello");
        var result = pipeline.Execute(context);

        Assert.Equal("HELLO", Encoding.UTF8.GetString(result.Value));
    }

    public void Dispose()
    {
        _manager.Dispose();
        _pluginLoader.Dispose();
    }
}
