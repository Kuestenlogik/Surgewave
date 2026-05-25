using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Testing;
using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Benchmarks.Streams;

/// <summary>
/// Benchmarks for topology throughput: records/second through different processor pipelines.
/// Uses TopologyTestDriver for deterministic, broker-free measurement.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Streams", "Topology")]
public class TopologyThroughputBenchmarks : IDisposable
{
    private TopologyTestDriver _passthroughDriver = null!;
    private TopologyTestDriver _filterDriver = null!;
    private TopologyTestDriver _mapDriver = null!;
    private TopologyTestDriver _flatMapDriver = null!;
    private TopologyTestDriver _aggregateDriver = null!;
    private TopologyTestDriver _complexDriver = null!;

    private TestInputTopic<string, string> _passthroughInput = null!;
    private TestInputTopic<string, string> _filterInput = null!;
    private TestInputTopic<string, string> _mapInput = null!;
    private TestInputTopic<string, string> _flatMapInput = null!;
    private TestInputTopic<string, int> _aggregateInput = null!;
    private TestInputTopic<string, string> _complexInput = null!;

    private string[] _keys = null!;
    private string[] _values = null!;

    [Params(1000, 10_000)]
    public int RecordCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _keys = new string[RecordCount];
        _values = new string[RecordCount];
        for (int i = 0; i < RecordCount; i++)
        {
            _keys[i] = $"key-{i % 100}";
            _values[i] = $"value-{i}-{new string('a', 50)}";
        }

        // Passthrough: Source → Sink
        var b1 = new StreamsBuilder();
        b1.Stream<string, string>("input").To("output");
        _passthroughDriver = new TopologyTestDriver(b1.Build());
        _passthroughInput = _passthroughDriver.CreateInputTopic<string, string>("input");

        // Filter: Source → Filter → Sink
        var b2 = new StreamsBuilder();
        b2.Stream<string, string>("input").Filter((k, v) => v.Length > 10).To("output");
        _filterDriver = new TopologyTestDriver(b2.Build());
        _filterInput = _filterDriver.CreateInputTopic<string, string>("input");

        // Map: Source → MapValues → Sink
        var b3 = new StreamsBuilder();
        b3.Stream<string, string>("input").MapValues(v => v.ToUpperInvariant()).To("output");
        _mapDriver = new TopologyTestDriver(b3.Build());
        _mapInput = _mapDriver.CreateInputTopic<string, string>("input");

        // FlatMap: Source → FlatMapValues → Sink
        var b4 = new StreamsBuilder();
        b4.Stream<string, string>("input")
            .FlatMapValues(v => (IEnumerable<string>)[v, v + "-copy"])
            .To("output");
        _flatMapDriver = new TopologyTestDriver(b4.Build());
        _flatMapInput = _flatMapDriver.CreateInputTopic<string, string>("input");

        // Aggregate: Source → GroupByKey → Count
        var b5 = new StreamsBuilder();
        b5.Stream<string, int>("input")
            .GroupByKey()
            .Count();
        _aggregateDriver = new TopologyTestDriver(b5.Build());
        _aggregateInput = _aggregateDriver.CreateInputTopic<string, int>("input");

        // Complex: Source → Filter → MapValues → Filter → Sink
        var b6 = new StreamsBuilder();
        b6.Stream<string, string>("input")
            .Filter((k, v) => v.Length > 10)
            .MapValues(v => v.ToUpperInvariant())
            .Filter((k, v) => !v.StartsWith("SKIP", StringComparison.Ordinal))
            .To("output");
        _complexDriver = new TopologyTestDriver(b6.Build());
        _complexInput = _complexDriver.CreateInputTopic<string, string>("input");
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    public void Dispose()
    {
        _passthroughDriver?.Dispose();
        _filterDriver?.Dispose();
        _mapDriver?.Dispose();
        _flatMapDriver?.Dispose();
        _aggregateDriver?.Dispose();
        _complexDriver?.Dispose();
        GC.SuppressFinalize(this);
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Passthrough")]
    public void Passthrough()
    {
        for (int i = 0; i < RecordCount; i++)
            _passthroughInput.PipeInput(_keys[i], _values[i]);
    }

    [Benchmark]
    [BenchmarkCategory("Filter")]
    public void Filter()
    {
        for (int i = 0; i < RecordCount; i++)
            _filterInput.PipeInput(_keys[i], _values[i]);
    }

    [Benchmark]
    [BenchmarkCategory("Map")]
    public void MapValues()
    {
        for (int i = 0; i < RecordCount; i++)
            _mapInput.PipeInput(_keys[i], _values[i]);
    }

    [Benchmark]
    [BenchmarkCategory("FlatMap")]
    public void FlatMap()
    {
        for (int i = 0; i < RecordCount; i++)
            _flatMapInput.PipeInput(_keys[i], _values[i]);
    }

    [Benchmark]
    [BenchmarkCategory("Aggregate")]
    public void Aggregate_Count()
    {
        for (int i = 0; i < RecordCount; i++)
            _aggregateInput.PipeInput(_keys[i], i);
    }

    [Benchmark]
    [BenchmarkCategory("Complex")]
    public void Complex_FilterMapFilter()
    {
        for (int i = 0; i < RecordCount; i++)
            _complexInput.PipeInput(_keys[i], _values[i]);
    }
}
