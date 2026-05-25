using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// Benchmarks for RecordBatch serialization and deserialization
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Unit", "Serialization")]
public class SerializationBenchmarks
{
    private RecordBatchSerializer _serializer = null!;
    private List<Message> _smallBatch = null!;
    private List<Message> _mediumBatch = null!;
    private List<Message> _largeBatch = null!;
    private byte[] _serializedSmall = null!;
    private byte[] _serializedMedium = null!;
    private byte[] _serializedLarge = null!;

    [GlobalSetup]
    public void Setup()
    {
        var logger = NullLogger<RecordBatchSerializer>.Instance;
        _serializer = new RecordBatchSerializer(logger);

        // Create test batches of different sizes
        _smallBatch = CreateMessages(10, 100);      // 10 messages, 100 bytes each
        _mediumBatch = CreateMessages(100, 500);    // 100 messages, 500 bytes each
        _largeBatch = CreateMessages(1000, 1000);   // 1000 messages, 1KB each

        // Pre-serialize for parsing benchmarks
        _serializedSmall = _serializer.SerializeMessages(_smallBatch);
        _serializedMedium = _serializer.SerializeMessages(_mediumBatch);
        _serializedLarge = _serializer.SerializeMessages(_largeBatch);
    }

    private static List<Message> CreateMessages(int count, int valueSize)
    {
        var messages = new List<Message>(count);
        var baseTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var valueTemplate = new byte[valueSize];
        Random.Shared.NextBytes(valueTemplate);

        for (int i = 0; i < count; i++)
        {
            messages.Add(new Message
            {
                Offset = i,
                Timestamp = baseTimestamp + i,
                Key = System.Text.Encoding.UTF8.GetBytes($"key-{i}"),
                Value = valueTemplate,
                Headers = ReadOnlyMemory<byte>.Empty
            });
        }

        return messages;
    }

    // Serialization benchmarks
    [Benchmark(Baseline = true)]
    public byte[] Serialize_SmallBatch() => _serializer.SerializeMessages(_smallBatch);

    [Benchmark]
    public byte[] Serialize_MediumBatch() => _serializer.SerializeMessages(_mediumBatch);

    [Benchmark]
    public byte[] Serialize_LargeBatch() => _serializer.SerializeMessages(_largeBatch);

    // Parsing benchmarks
    [Benchmark]
    public List<Message> Parse_SmallBatch() => _serializer.ParseRecordBatch(_serializedSmall);

    [Benchmark]
    public List<Message> Parse_MediumBatch() => _serializer.ParseRecordBatch(_serializedMedium);

    [Benchmark]
    public List<Message> Parse_LargeBatch() => _serializer.ParseRecordBatch(_serializedLarge);
}
