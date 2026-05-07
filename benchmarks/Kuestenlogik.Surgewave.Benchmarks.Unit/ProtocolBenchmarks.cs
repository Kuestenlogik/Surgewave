using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Protocol.Kafka;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// Benchmarks for Kafka protocol parsing operations
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Unit", "Protocol")]
public class ProtocolBenchmarks
{
    private KafkaProtocolHandler _handler = null!;
    private byte[] _metadataRequest = null!;
    private byte[] _produceRequest = null!;
    private byte[] _fetchRequest = null!;

    [GlobalSetup]
    public void Setup()
    {
        _handler = new KafkaProtocolHandler();

        // Create sample requests for parsing benchmarks
        _metadataRequest = CreateMetadataRequest();
        _produceRequest = CreateProduceRequest();
        _fetchRequest = CreateFetchRequest();
    }

    private static byte[] CreateMetadataRequest()
    {
        // Simple Metadata request (v9) for topic "test-topic"
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // API Key: Metadata (3)
        WriteBigEndian(writer, (short)3);
        // API Version: 9
        WriteBigEndian(writer, (short)9);
        // Correlation ID: 1
        WriteBigEndian(writer, 1);
        // Client ID: "benchmark"
        WriteString(writer, "benchmark");
        // Tagged fields (empty)
        writer.Write((byte)0);

        // Topics array (compact): 1 topic
        writer.Write((byte)2); // length + 1
        // Topic name (compact string)
        WriteCompactString(writer, "test-topic");
        // Tagged fields for topic
        writer.Write((byte)0);

        // AllowAutoTopicCreation: true
        writer.Write((byte)1);
        // IncludeTopicAuthorizedOperations: false
        writer.Write((byte)0);
        // Tagged fields (empty)
        writer.Write((byte)0);

        return stream.ToArray();
    }

    private static byte[] CreateProduceRequest()
    {
        // Simple Produce request (v9) with minimal data
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // API Key: Produce (0)
        WriteBigEndian(writer, (short)0);
        // API Version: 9
        WriteBigEndian(writer, (short)9);
        // Correlation ID: 2
        WriteBigEndian(writer, 2);
        // Client ID: "benchmark"
        WriteString(writer, "benchmark");
        // Tagged fields (empty)
        writer.Write((byte)0);

        // Transactional ID (null compact string)
        writer.Write((byte)0);
        // Acks: -1 (all)
        WriteBigEndian(writer, (short)-1);
        // Timeout: 30000ms
        WriteBigEndian(writer, 30000);

        // Topics array (compact): 1 topic
        writer.Write((byte)2); // length + 1
        // Topic name
        WriteCompactString(writer, "test-topic");

        // Partitions array (compact): 1 partition
        writer.Write((byte)2); // length + 1
        // Partition index: 0
        WriteBigEndian(writer, 0);

        // Records (compact bytes) - minimal record batch
        var records = CreateMinimalRecordBatch();
        WriteCompactBytes(writer, records);

        // Tagged fields for partition
        writer.Write((byte)0);
        // Tagged fields for topic
        writer.Write((byte)0);
        // Tagged fields (empty)
        writer.Write((byte)0);

        return stream.ToArray();
    }

    private static byte[] CreateFetchRequest()
    {
        // Simple Fetch request (v12)
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // API Key: Fetch (1)
        WriteBigEndian(writer, (short)1);
        // API Version: 12
        WriteBigEndian(writer, (short)12);
        // Correlation ID: 3
        WriteBigEndian(writer, 3);
        // Client ID: "benchmark"
        WriteString(writer, "benchmark");
        // Tagged fields (empty)
        writer.Write((byte)0);

        // ReplicaId: -1 (consumer)
        WriteBigEndian(writer, -1);
        // MaxWaitMs: 500
        WriteBigEndian(writer, 500);
        // MinBytes: 1
        WriteBigEndian(writer, 1);
        // MaxBytes: 1MB
        WriteBigEndian(writer, 1024 * 1024);
        // IsolationLevel: 0 (read uncommitted)
        writer.Write((byte)0);
        // SessionId: 0
        WriteBigEndian(writer, 0);
        // SessionEpoch: -1
        WriteBigEndian(writer, -1);

        // Topics array (compact): 1 topic
        writer.Write((byte)2);
        WriteCompactString(writer, "test-topic");

        // Partitions array (compact): 1 partition
        writer.Write((byte)2);
        // Partition: 0
        WriteBigEndian(writer, 0);
        // CurrentLeaderEpoch: -1
        WriteBigEndian(writer, -1);
        // FetchOffset: 0
        WriteBigEndian(writer, 0L);
        // LastFetchedEpoch: -1
        WriteBigEndian(writer, -1);
        // LogStartOffset: -1
        WriteBigEndian(writer, -1L);
        // PartitionMaxBytes: 1MB
        WriteBigEndian(writer, 1024 * 1024);
        // Tagged fields for partition
        writer.Write((byte)0);

        // Tagged fields for topic
        writer.Write((byte)0);

        // ForgottenTopics (compact array): empty
        writer.Write((byte)1);
        // RackId (compact string): empty
        writer.Write((byte)1);
        // Tagged fields
        writer.Write((byte)0);

        return stream.ToArray();
    }

    private static byte[] CreateMinimalRecordBatch()
    {
        // Create a minimal valid record batch
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // Base Offset
        WriteBigEndian(writer, 0L);
        // Batch Length (will be calculated)
        var batchLengthPosition = stream.Position;
        WriteBigEndian(writer, 0); // Placeholder

        // Partition Leader Epoch
        WriteBigEndian(writer, 0);
        // Magic: 2
        writer.Write((byte)2);
        // CRC (placeholder)
        WriteBigEndian(writer, 0u);
        // Attributes: 0
        WriteBigEndian(writer, (short)0);
        // Last Offset Delta: 0
        WriteBigEndian(writer, 0);
        // Base Timestamp
        WriteBigEndian(writer, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        // Max Timestamp
        WriteBigEndian(writer, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        // Producer ID: -1
        WriteBigEndian(writer, -1L);
        // Producer Epoch: -1
        WriteBigEndian(writer, (short)-1);
        // Base Sequence: -1
        WriteBigEndian(writer, -1);
        // Record Count: 1
        WriteBigEndian(writer, 1);

        // Single record (minimal)
        WriteVarInt(writer, 10); // Record length
        writer.Write((byte)0);   // Attributes
        WriteVarInt(writer, 0);  // Timestamp delta
        WriteVarInt(writer, 0);  // Offset delta
        WriteVarInt(writer, -1); // Key length (null)
        WriteVarInt(writer, 4);  // Value length
        writer.Write("test"u8);  // Value
        WriteVarInt(writer, 0);  // Headers count

        return stream.ToArray();
    }

    private static void WriteBigEndian(BinaryWriter writer, short value)
    {
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, int value)
    {
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, long value)
    {
        writer.Write((byte)(value >> 56));
        writer.Write((byte)(value >> 48));
        writer.Write((byte)(value >> 40));
        writer.Write((byte)(value >> 32));
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, uint value)
    {
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteString(BinaryWriter writer, string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        WriteBigEndian(writer, (short)bytes.Length);
        writer.Write(bytes);
    }

    private static void WriteCompactString(BinaryWriter writer, string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        WriteVarInt(writer, bytes.Length + 1);
        writer.Write(bytes);
    }

    private static void WriteCompactBytes(BinaryWriter writer, byte[] value)
    {
        WriteVarInt(writer, value.Length + 1);
        writer.Write(value);
    }

    private static void WriteVarInt(BinaryWriter writer, int value)
    {
        var v = (uint)((value << 1) ^ (value >> 31));
        while ((v & ~0x7F) != 0)
        {
            writer.Write((byte)((v & 0x7F) | 0x80));
            v >>= 7;
        }
        writer.Write((byte)v);
    }

    // Benchmarks
    [Benchmark(Baseline = true)]
    public object Parse_MetadataRequest() => _handler.ParseRequest(_metadataRequest);

    [Benchmark]
    public object Parse_ProduceRequest() => _handler.ParseRequest(_produceRequest);

    [Benchmark]
    public object Parse_FetchRequest() => _handler.ParseRequest(_fetchRequest);
}
