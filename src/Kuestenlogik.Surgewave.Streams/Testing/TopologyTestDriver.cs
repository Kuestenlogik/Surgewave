using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Streams.Processors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Streams.Testing;

/// <summary>
/// Test driver for Streams topologies. Enables deterministic, broker-free unit testing.
/// Equivalent to Kafka Streams' TopologyTestDriver.
///
/// Usage:
/// <code>
/// var builder = new StreamsBuilder();
/// builder.Stream&lt;string, int&gt;("input").MapValues(v => v * 2).To("output");
///
/// using var driver = new TopologyTestDriver(builder.Build(), config);
/// var input = driver.CreateInputTopic&lt;string, int&gt;("input");
/// var output = driver.CreateOutputTopic&lt;string, int&gt;("output");
///
/// input.PipeInput("key", 21);
/// var result = output.ReadKeyValue();
/// Assert.Equal(42, result.Value);
/// </code>
/// </summary>
public sealed class TopologyTestDriver : IDisposable
{
    private readonly Topology _topology;
    private readonly ProcessorContext _context;
    private readonly StreamsMetrics _metrics;
    private readonly ConcurrentDictionary<string, ConcurrentQueue<RawOutputRecord>> _outputBuffers = new();
    private readonly Dictionary<string, ProcessorNode> _sourcesByTopic = new();
    private bool _disposed;

    /// <summary>
    /// Creates a TopologyTestDriver with default config.
    /// </summary>
    public TopologyTestDriver(Topology topology)
        : this(topology, new StreamsConfig
        {
            ApplicationId = "test-driver",
            BootstrapServers = "dummy:9092"
        })
    {
    }

    /// <summary>
    /// Creates a TopologyTestDriver with the given config.
    /// </summary>
    public TopologyTestDriver(Topology topology, StreamsConfig config, ILoggerFactory? loggerFactory = null)
    {
        _topology = topology;
        _metrics = new StreamsMetrics();
        var logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<TopologyTestDriver>();
        _context = new ProcessorContext(config, _metrics, logger);

        // Initialize state stores
        foreach (var supplier in topology.StateStoreSuppliers)
        {
            var getMethod = supplier.GetType().GetMethod("Get");
            if (getMethod != null)
            {
                var store = (IStateStore)getMethod.Invoke(supplier, null)!;
                _context.RegisterStateStore(store);
            }
        }

        // Initialize all nodes and cache sources
        foreach (var source in topology.Sources)
        {
            InitializeNode(source);

            var topicProp = source.GetType().GetProperty("TopicPattern");
            if (topicProp?.GetValue(source)?.ToString() is string topic)
            {
                _sourcesByTopic[topic] = source;
            }
        }

        // Wire all sink OutputHandlers to capture output
        foreach (var source in topology.Sources)
        {
            WireSinkHandlersRecursive(source);
        }
    }

    private void InitializeNode(ProcessorNode node)
    {
        node.Init(_context);
        foreach (var child in node.Children)
        {
            InitializeNode(child);
        }
    }

    private void WireSinkHandlersRecursive(ProcessorNode node)
    {
        foreach (var child in node.Children)
        {
            var handlerProp = child.GetType().GetProperty("OutputHandler");
            if (handlerProp != null)
            {
                handlerProp.SetValue(child, new Action<string, byte[], byte[], long>((topic, key, value, ts) =>
                {
                    var buffer = _outputBuffers.GetOrAdd(topic, _ => new ConcurrentQueue<RawOutputRecord>());
                    buffer.Enqueue(new RawOutputRecord(key, value, ts));
                }));
            }
            WireSinkHandlersRecursive(child);
        }
    }

    /// <summary>
    /// Creates a TestInputTopic for piping records into the topology.
    /// </summary>
    public TestInputTopic<TKey, TValue> CreateInputTopic<TKey, TValue>(string topic)
    {
        return CreateInputTopic<TKey, TValue>(topic, Serdes.Json<TKey>(), Serdes.Json<TValue>());
    }

    /// <summary>
    /// Creates a TestInputTopic with custom serdes.
    /// </summary>
    public TestInputTopic<TKey, TValue> CreateInputTopic<TKey, TValue>(
        string topic,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        long startTimestamp = 0,
        long autoAdvanceMs = 1)
    {
        return new TestInputTopic<TKey, TValue>(topic, keySerde, valueSerde, this, startTimestamp, autoAdvanceMs);
    }

    /// <summary>
    /// Creates a TestOutputTopic for reading output records.
    /// </summary>
    public TestOutputTopic<TKey, TValue> CreateOutputTopic<TKey, TValue>(string topic)
    {
        return CreateOutputTopic<TKey, TValue>(topic, Serdes.Json<TKey>(), Serdes.Json<TValue>());
    }

    /// <summary>
    /// Creates a TestOutputTopic with custom serdes.
    /// </summary>
    public TestOutputTopic<TKey, TValue> CreateOutputTopic<TKey, TValue>(
        string topic,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde)
    {
        var buffer = _outputBuffers.GetOrAdd(topic, _ => new ConcurrentQueue<RawOutputRecord>());
        return new TestOutputTopic<TKey, TValue>(topic, keySerde, valueSerde, buffer);
    }

    /// <summary>
    /// Gets a state store by name for assertions.
    /// </summary>
    public TStore? GetStateStore<TStore>(string name) where TStore : class, IStateStore
    {
        return _context.GetStateStore<TStore>(name);
    }

    /// <summary>
    /// Gets the metrics for assertions.
    /// </summary>
    public StreamsMetrics Metrics => _metrics;

    /// <summary>
    /// Gets the processor context for advanced test scenarios.
    /// </summary>
    public ProcessorContext Context => _context;

    /// <summary>
    /// Pipes raw bytes into the topology (called by TestInputTopic).
    /// </summary>
    internal void PipeInput(string topic, byte[] key, byte[] value, long timestamp)
    {
        if (!_sourcesByTopic.TryGetValue(topic, out var source))
            throw new ArgumentException(
                $"No source node found for topic '{topic}'. Available topics: [{string.Join(", ", _sourcesByTopic.Keys)}]");

        _context.Topic = topic;
        _context.Timestamp = timestamp;

        source.Process(key, value, timestamp);
        _metrics.RecordProcessed(key.Length + value.Length);
    }

    /// <summary>
    /// Advances wall clock time to trigger wall-clock punctuations.
    /// </summary>
    public void AdvanceWallClockTime(TimeSpan advance)
    {
        _context.MaybeFireWallClockTimePunctuations();
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        foreach (var source in _topology.Sources)
        {
            CloseNode(source);
        }

        _metrics.Dispose();
        _disposed = true;
    }

    private void CloseNode(ProcessorNode node)
    {
        node.Close();
        foreach (var child in node.Children)
        {
            CloseNode(child);
        }
    }
}
