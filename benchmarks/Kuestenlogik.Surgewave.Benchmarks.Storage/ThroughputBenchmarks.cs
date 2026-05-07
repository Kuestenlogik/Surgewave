using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Benchmarks.Storage;

/// <summary>
/// Throughput benchmarks for measuring messages per second
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Storage", "Throughput", "Serialization")]
public class ThroughputBenchmarks
{
    private RecordBatchSerializer _serializer = null!;
    private List<Message> _millionMessages = null!;
    private byte[] _serializedMillion = null!;

    [Params(100_000, 1_000_000)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var logger = NullLogger<RecordBatchSerializer>.Instance;
        _serializer = new RecordBatchSerializer(logger);

        // Create messages (1KB each - typical size)
        _millionMessages = CreateMessages(MessageCount, 1024);

        // Pre-serialize for parsing throughput benchmark
        _serializedMillion = _serializer.SerializeMessages(_millionMessages);
    }

    private static List<Message> CreateMessages(int count, int valueSize)
    {
        var messages = new List<Message>(count);
        var baseTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var valueData = new byte[valueSize];
        Random.Shared.NextBytes(valueData);

        for (int i = 0; i < count; i++)
        {
            messages.Add(new Message
            {
                Offset = i,
                Timestamp = baseTimestamp + i,
                Key = BitConverter.GetBytes(i),
                Value = valueData,
                Headers = ReadOnlyMemory<byte>.Empty
            });
        }

        return messages;
    }

    /// <summary>
    /// Benchmark serializing messages to measure throughput (msg/s)
    /// </summary>
    [Benchmark(Baseline = true, Description = "Serialize N messages (1KB each)")]
    public byte[] Serialize_Messages()
    {
        return _serializer.SerializeMessages(_millionMessages);
    }

    /// <summary>
    /// Benchmark parsing messages to measure throughput (msg/s)
    /// </summary>
    [Benchmark(Description = "Parse N messages (1KB each)")]
    public List<Message> Parse_Messages()
    {
        return _serializer.ParseRecordBatch(_serializedMillion);
    }
}
