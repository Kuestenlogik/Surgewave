using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Dahomey.Cbor;
using Dahomey.Cbor.Serialization;
using Dahomey.Cbor.Serialization.Converters;
using MessagePack;
using MemoryPack;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// Benchmarks comparing throughput of all supported schemaless serialization formats.
/// Measures serialize (object to bytes) and deserialize (bytes to object) for each format.
/// </summary>
/// <remarks>
/// Formats included: System.Text.Json (baseline), MessagePack, MemoryPack, Hyperion, CBOR.
/// Orleans is excluded because its Roslyn source generators (Microsoft.CodeAnalysis 5.x)
/// conflict with BenchmarkDotNet's transitive Roslyn 4.14.x dependency (NU1608).
/// Protobuf/Avro/FlatBuffers/Bond/Thrift/CapnProto are excluded because they require
/// code generation which adds complexity beyond the scope of a unit benchmark.
/// </remarks>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Unit", "SerializationFormat")]
public class SerializationFormatBenchmarks
{
    // Serializer instances (reused across iterations)
    private Hyperion.Serializer _hyperionSerializer = null!;
    private CborOptions _cborOptions = null!;

    // Test payload
    private TestMessage _message = null!;

    // Pre-serialized bytes for deserialization benchmarks
    private byte[] _jsonBytes = null!;
    private byte[] _msgpackBytes = null!;
    private byte[] _memoryPackBytes = null!;
    private byte[] _hyperionBytes = null!;
    private byte[] _cborBytes = null!;

    [GlobalSetup]
    public void Setup()
    {
        _message = new TestMessage
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTimeOffset.UtcNow,
            Key = "sensor-42",
            Value = 23.7,
            Tags = ["temperature", "warehouse", "zone-a"],
            Metadata = new Dictionary<string, string>
            {
                ["source"] = "iot-gateway",
                ["region"] = "eu-west-1"
            }
        };

        // Initialize Hyperion
        _hyperionSerializer = new Hyperion.Serializer();

        // Initialize CBOR with DateTimeOffset converter
        _cborOptions = new CborOptions();
        _cborOptions.Registry.ConverterRegistry.RegisterConverter(typeof(DateTimeOffset), new CborDateTimeOffsetConverter());

        // Pre-serialize for deserialization benchmarks
        _jsonBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(_message);
        _msgpackBytes = MessagePackSerializer.Serialize(_message);
        _memoryPackBytes = MemoryPackSerializer.Serialize(_message);
        _hyperionBytes = HyperionSerialize(_message);
        _cborBytes = CborSerialize(_message);
    }

    private byte[] HyperionSerialize<T>(T obj)
    {
        using var stream = new MemoryStream();
        _hyperionSerializer.Serialize(obj, stream);
        return stream.ToArray();
    }

    private T HyperionDeserialize<T>(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        return _hyperionSerializer.Deserialize<T>(stream);
    }

    private byte[] CborSerialize<T>(T obj)
    {
        using var stream = new MemoryStream();
        Cbor.SerializeAsync(obj, stream, _cborOptions).GetAwaiter().GetResult();
        return stream.ToArray();
    }

    private T CborDeserialize<T>(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        return Cbor.DeserializeAsync<T>(stream, _cborOptions).AsTask().GetAwaiter().GetResult()!;
    }

    // ── Serialization (object -> bytes) ──

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Serialize")]
    public byte[] Json_Serialize()
        => System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(_message);

    [Benchmark]
    [BenchmarkCategory("Serialize")]
    public byte[] MessagePack_Serialize()
        => MessagePackSerializer.Serialize(_message);

    [Benchmark]
    [BenchmarkCategory("Serialize")]
    public byte[] MemoryPack_Serialize()
        => MemoryPackSerializer.Serialize(_message);

    [Benchmark]
    [BenchmarkCategory("Serialize")]
    public byte[] Hyperion_Serialize()
        => HyperionSerialize(_message);

    [Benchmark]
    [BenchmarkCategory("Serialize")]
    public byte[] Cbor_Serialize()
        => CborSerialize(_message);

    // ── Deserialization (bytes -> object) ──

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public TestMessage? Json_Deserialize()
        => System.Text.Json.JsonSerializer.Deserialize<TestMessage>(_jsonBytes);

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public TestMessage? MessagePack_Deserialize()
        => MessagePackSerializer.Deserialize<TestMessage>(_msgpackBytes);

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public TestMessage? MemoryPack_Deserialize()
        => MemoryPackSerializer.Deserialize<TestMessage>(_memoryPackBytes);

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public TestMessage Hyperion_Deserialize()
        => HyperionDeserialize<TestMessage>(_hyperionBytes);

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public TestMessage Cbor_Deserialize()
        => CborDeserialize<TestMessage>(_cborBytes);
}

/// <summary>
/// Realistic test message representing a typical Surgewave event payload.
/// Annotated for MessagePack and MemoryPack serialization.
/// </summary>
[MessagePackObject]
[MemoryPackable]
public partial class TestMessage
{
    /// <summary>Unique message identifier.</summary>
    [Key(0)]
    public Guid Id { get; set; }

    /// <summary>Message timestamp.</summary>
    [Key(1)]
    public DateTimeOffset Timestamp { get; set; }

    /// <summary>Partition key.</summary>
    [Key(2)]
    public string Key { get; set; } = "";

    /// <summary>Sensor reading value.</summary>
    [Key(3)]
    public double Value { get; set; }

    /// <summary>Classification tags.</summary>
    [Key(4)]
    public string[] Tags { get; set; } = [];

    /// <summary>Additional metadata.</summary>
    [Key(5)]
    public Dictionary<string, string> Metadata { get; set; } = new();
}

/// <summary>
/// CBOR converter for DateTimeOffset — serializes as Unix milliseconds (CBOR integer).
/// </summary>
internal sealed class CborDateTimeOffsetConverter : CborConverterBase<DateTimeOffset>
{
    public override DateTimeOffset Read(ref CborReader reader)
    {
        var ms = reader.ReadInt64();
        return DateTimeOffset.FromUnixTimeMilliseconds(ms);
    }

    public override void Write(ref CborWriter writer, DateTimeOffset value)
    {
        writer.WriteInt64(value.ToUnixTimeMilliseconds());
    }
}
