using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Testing;
using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Benchmarks.Streams;

/// <summary>
/// Benchmarks for join operations: Stream-Stream, Stream-Table, Table-Table.
/// Uses TopologyTestDriver for deterministic measurement.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Streams", "Join")]
public class JoinBenchmarks : IDisposable
{
    private TopologyTestDriver _streamStreamDriver = null!;
    private TopologyTestDriver _streamTableDriver = null!;
    private TopologyTestDriver _tableTableDriver = null!;

    private TestInputTopic<string, string> _leftStreamInput = null!;
    private TestInputTopic<string, string> _rightStreamInput = null!;
    private TestInputTopic<string, string> _streamInput = null!;
    private TestInputTopic<string, string> _tableInput = null!;
    private TestInputTopic<string, string> _leftTableInput = null!;
    private TestInputTopic<string, string> _rightTableInput = null!;

    private string[] _keys = null!;
    private string[] _values = null!;

    [Params(1000, 5000)]
    public int RecordCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _keys = new string[RecordCount];
        _values = new string[RecordCount];
        for (int i = 0; i < RecordCount; i++)
        {
            _keys[i] = $"key-{i:D6}";
            _values[i] = $"val-{i}";
        }

        // Stream-Stream Join
        var b1 = new StreamsBuilder();
        var left = b1.Stream<string, string>("left-stream");
        var right = b1.Stream<string, string>("right-stream");
        left.Join(right, (l, r) => $"{l}+{r}",
            JoinWindows.Of(TimeSpan.FromMinutes(5)))
            .To("ss-output");
        _streamStreamDriver = new TopologyTestDriver(b1.Build());
        _leftStreamInput = _streamStreamDriver.CreateInputTopic<string, string>("left-stream");
        _rightStreamInput = _streamStreamDriver.CreateInputTopic<string, string>("right-stream");

        // Stream-Table Join
        var b2 = new StreamsBuilder();
        var stream = b2.Stream<string, string>("stream-input");
        var table = b2.Table<string, string>("table-input");
        stream.Join(table, (s, t) => $"{s}+{t}")
            .To("st-output");
        _streamTableDriver = new TopologyTestDriver(b2.Build());
        _streamInput = _streamTableDriver.CreateInputTopic<string, string>("stream-input");
        _tableInput = _streamTableDriver.CreateInputTopic<string, string>("table-input");

        // Pre-populate table
        for (int i = 0; i < RecordCount; i++)
            _tableInput.PipeInput(_keys[i], $"table-{i}");

        // Table-Table Join
        var b3 = new StreamsBuilder();
        var leftT = b3.Table<string, string>("left-table");
        var rightT = b3.Table<string, string>("right-table");
        leftT.Join(rightT, (l, r) => $"{l}+{r}");
        _tableTableDriver = new TopologyTestDriver(b3.Build());
        _leftTableInput = _tableTableDriver.CreateInputTopic<string, string>("left-table");
        _rightTableInput = _tableTableDriver.CreateInputTopic<string, string>("right-table");

        // Pre-populate right table
        for (int i = 0; i < RecordCount; i++)
            _rightTableInput.PipeInput(_keys[i], $"right-{i}");
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    public void Dispose()
    {
        _streamStreamDriver?.Dispose();
        _streamTableDriver?.Dispose();
        _tableTableDriver?.Dispose();
        GC.SuppressFinalize(this);
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("StreamStream")]
    public void StreamStreamJoin()
    {
        for (int i = 0; i < RecordCount; i++)
        {
            _leftStreamInput.PipeInput(_keys[i], $"left-{i}");
            _rightStreamInput.PipeInput(_keys[i], $"right-{i}");
        }
    }

    [Benchmark]
    [BenchmarkCategory("StreamTable")]
    public void StreamTableJoin()
    {
        for (int i = 0; i < RecordCount; i++)
            _streamInput.PipeInput(_keys[i], $"stream-{i}");
    }

    [Benchmark]
    [BenchmarkCategory("TableTable")]
    public void TableTableJoin()
    {
        for (int i = 0; i < RecordCount; i++)
            _leftTableInput.PipeInput(_keys[i], $"left-{i}");
    }
}
