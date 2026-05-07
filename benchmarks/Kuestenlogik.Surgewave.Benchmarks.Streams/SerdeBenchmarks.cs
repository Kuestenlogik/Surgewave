using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Streams;

namespace Kuestenlogik.Surgewave.Benchmarks.Streams;

/// <summary>
/// Benchmarks for Streams serialization/deserialization (Serdes).
/// Compares String, Int32, Int64, Double, ByteArray, and JSON serdes.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Streams", "Serde")]
public class SerdeBenchmarks
{
    private ISerde<string> _stringSerde = null!;
    private ISerde<int> _int32Serde = null!;
    private ISerde<long> _int64Serde = null!;
    private ISerde<double> _doubleSerde = null!;
    private ISerde<byte[]> _byteArraySerde = null!;
    private ISerde<SampleRecord> _jsonSerde = null!;

    private string _sampleString = null!;
    private byte[] _stringBytes = null!;
    private byte[] _intBytes = null!;
    private byte[] _longBytes = null!;
    private byte[] _doubleBytes = null!;
    private byte[] _byteArrayBytes = null!;
    private SampleRecord _sampleRecord = null!;
    private byte[] _jsonBytes = null!;

    [GlobalSetup]
    public void Setup()
    {
        _stringSerde = Serdes.String();
        _int32Serde = Serdes.Int32();
        _int64Serde = Serdes.Int64();
        _doubleSerde = Serdes.Double();
        _byteArraySerde = Serdes.ByteArray();
        _jsonSerde = Serdes.Json<SampleRecord>();

        _sampleString = "benchmark-test-value-with-some-realistic-length-data-" + new string('x', 100);
        _sampleRecord = new SampleRecord
        {
            Id = 42,
            Name = "benchmark",
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Tags = ["perf", "test", "streams"]
        };

        _stringBytes = _stringSerde.Serialize(_sampleString);
        _intBytes = _int32Serde.Serialize(42);
        _longBytes = _int64Serde.Serialize(123456789L);
        _doubleBytes = _doubleSerde.Serialize(3.14159);
        _byteArrayBytes = new byte[256];
        Random.Shared.NextBytes(_byteArrayBytes);
        _jsonBytes = _jsonSerde.Serialize(_sampleRecord);
    }

    // === SERIALIZE ===

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Serialize")]
    public byte[] Serialize_String() => _stringSerde.Serialize(_sampleString);

    [Benchmark]
    [BenchmarkCategory("Serialize")]
    public byte[] Serialize_Int32() => _int32Serde.Serialize(42);

    [Benchmark]
    [BenchmarkCategory("Serialize")]
    public byte[] Serialize_Int64() => _int64Serde.Serialize(123456789L);

    [Benchmark]
    [BenchmarkCategory("Serialize")]
    public byte[] Serialize_Double() => _doubleSerde.Serialize(3.14159);

    [Benchmark]
    [BenchmarkCategory("Serialize")]
    public byte[] Serialize_ByteArray() => _byteArraySerde.Serialize(_byteArrayBytes);

    [Benchmark]
    [BenchmarkCategory("Serialize")]
    public byte[] Serialize_Json() => _jsonSerde.Serialize(_sampleRecord);

    // === DESERIALIZE ===

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public string Deserialize_String() => _stringSerde.Deserialize(_stringBytes);

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public int Deserialize_Int32() => _int32Serde.Deserialize(_intBytes);

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public long Deserialize_Int64() => _int64Serde.Deserialize(_longBytes);

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public double Deserialize_Double() => _doubleSerde.Deserialize(_doubleBytes);

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public byte[] Deserialize_ByteArray() => _byteArraySerde.Deserialize(_byteArrayBytes);

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public SampleRecord Deserialize_Json() => _jsonSerde.Deserialize(_jsonBytes);

    // === ROUNDTRIP ===

    [Benchmark]
    [BenchmarkCategory("Roundtrip")]
    public string Roundtrip_String() => _stringSerde.Deserialize(_stringSerde.Serialize(_sampleString));

    [Benchmark]
    [BenchmarkCategory("Roundtrip")]
    public SampleRecord Roundtrip_Json() => _jsonSerde.Deserialize(_jsonSerde.Serialize(_sampleRecord));

    public sealed class SampleRecord
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public long Timestamp { get; set; }
        public List<string> Tags { get; set; } = [];
    }
}
