using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Mqtt;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Benchmarks.Transport;

/// <summary>
/// Cross-protocol serialization/deserialization benchmarks comparing the overhead of
/// MQTT topic mapping, WebSocket JSON framing, Surgewave Native binary encoding, and Kafka
/// wire protocol encoding — all without requiring a running broker.
///
/// Categories:
///   TopicMapping    — MQTT topic-filter matching and MQTT→Surgewave topic translation
///   Serialization   — Encode an outbound message to bytes (per protocol)
///   Deserialization — Decode an inbound message from bytes (per protocol)
///   MessageSize     — Per-protocol framing overhead across payload sizes
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Transport", "Protocol", "ProtocolComparison")]
public class ProtocolComparisonBenchmarks
{
    // -------------------------------------------------------------------------
    // Pre-built byte buffers (set up once, reused across all iterations)
    // -------------------------------------------------------------------------

    private KafkaProtocolHandler _kafkaHandler = null!;

    // Kafka wire-format requests
    private byte[] _kafkaProduceRequest = null!;
    private byte[] _kafkaFetchRequest = null!;
    private byte[] _kafkaMetadataRequest = null!;

    // Surgewave native produce frame (header + payload)
    private byte[] _nativeProduceFrame = null!;
    private byte[] _nativeBuffer = null!;            // scratch buffer for write benchmarks

    // WebSocket JSON messages (pre-serialized)
    private byte[] _wsProduceJson = null!;
    private byte[] _wsConsumeJson = null!;
    private byte[] _wsSubscribeJson = null!;

    // MQTT topic strings for matching benchmarks
    private static readonly string[] MqttTopics =
    [
        "sensors/temperature/room1",
        "fleet/truck/42/location",
        "alerts/critical/system",
        "home/lights/bedroom/dimmer",
        "metrics/cpu/node-1/usage"
    ];

    private static readonly string[] MqttFilters =
    [
        "sensors/+/room1",      // single-level wildcard match
        "fleet/#",              // multi-level wildcard match
        "alerts/critical/+",    // single-level wildcard
        "home/+/+/dimmer",      // two single-level wildcards
        "metrics/cpu/+/usage"   // single-level wildcard
    ];

    [Params(64, 256, 1024, 4096)]
    public int PayloadSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _kafkaHandler = new KafkaProtocolHandler();

        // Build Kafka wire requests
        _kafkaProduceRequest = BuildKafkaProduceRequest(PayloadSize);
        _kafkaFetchRequest = BuildKafkaFetchRequest();
        _kafkaMetadataRequest = BuildKafkaMetadataRequest();

        // Build Native produce frame
        _nativeProduceFrame = BuildNativeProduceFrame(PayloadSize);
        _nativeBuffer = new byte[4096 + 64]; // enough for largest payload + header

        // Build WebSocket JSON messages
        _wsProduceJson = BuildWebSocketProduceJson(PayloadSize);
        _wsConsumeJson = BuildWebSocketConsumeJson(PayloadSize);
        _wsSubscribeJson = BuildWebSocketSubscribeJson();
    }

    // =========================================================================
    // MQTT Topic Matching
    // =========================================================================

    /// <summary>
    /// Matches an MQTT topic against a filter containing '+' wildcards.
    /// Baseline: shows cost of parsing and level-by-level comparison.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("TopicMapping")]
    public bool Mqtt_TopicFilter_Match_Wildcard()
    {
        // Rotate through all filter/topic pairs to avoid branch prediction warmup
        var result = false;
        for (var i = 0; i < MqttFilters.Length; i++)
            result ^= MqttTopicMatcher.Matches(MqttFilters[i], MqttTopics[i]);
        return result;
    }

    /// <summary>
    /// Matches an MQTT topic against an exact filter (no wildcards).
    /// Shows the fast-path cost when filters are fully qualified.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("TopicMapping")]
    public bool Mqtt_TopicFilter_Match_Exact()
    {
        var result = false;
        for (var i = 0; i < MqttTopics.Length; i++)
            result ^= MqttTopicMatcher.Matches(MqttTopics[i], MqttTopics[i]);
        return result;
    }

    /// <summary>
    /// Maps an MQTT topic string to a Surgewave topic name (slash → dot, prefix prepend).
    /// Represents the per-message cost in the MQTT protocol adapter hot path.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("TopicMapping")]
    public string Mqtt_TopicToSurgewaveTopic_Map()
    {
        // Inline the same logic as MqttProtocolAdapter.MapMqttToSurgewaveTopic
        // (method is internal; this mirrors it faithfully)
        const string prefix = "mqtt.";
        var topic = MqttTopics[2]; // "alerts/critical/system"
        return string.Concat(prefix, topic.Replace('/', '.'));
    }

    // =========================================================================
    // WebSocket JSON Serialization / Deserialization
    // =========================================================================

    /// <summary>
    /// Serializes a WebSocket produce message to UTF-8 JSON bytes.
    /// Measures the overhead of System.Text.Json source-generated serialization.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Serialization")]
    public byte[] WebSocket_Serialize_ProduceMessage()
    {
        var msg = new WsBenchProduceMessage
        {
            Key = "bench-key",
            Value = "bench-value-" + PayloadSize
        };
        return JsonSerializer.SerializeToUtf8Bytes(msg, WsBenchJsonContext.Default.WsBenchProduceMessage);
    }

    /// <summary>
    /// Deserializes a pre-built WebSocket produce message from UTF-8 JSON bytes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Deserialization")]
    public WsBenchProduceMessage? WebSocket_Deserialize_ProduceMessage()
        => JsonSerializer.Deserialize(_wsProduceJson, WsBenchJsonContext.Default.WsBenchProduceMessage);

    /// <summary>
    /// Deserializes a pre-built WebSocket consume message from UTF-8 JSON bytes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Deserialization")]
    public WsBenchConsumeMessage? WebSocket_Deserialize_ConsumeMessage()
        => JsonSerializer.Deserialize(_wsConsumeJson, WsBenchJsonContext.Default.WsBenchConsumeMessage);

    // =========================================================================
    // Surgewave Native Binary Serialization / Deserialization
    // =========================================================================

    /// <summary>
    /// Writes a Surgewave native produce frame into a pre-allocated buffer.
    /// Uses <see cref="SurgewavePayloadWriter"/> — zero-allocation, Span-based.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Serialization")]
    public int Native_Serialize_ProduceFrame()
    {
        var writer = new SurgewavePayloadWriter(_nativeBuffer.AsSpan());
        // OpCode (2 bytes)
        writer.WriteUInt16((ushort)SurgewaveOpCode.Produce);
        // Correlation ID (4 bytes)
        writer.WriteInt32(42);
        // Topic (length-prefixed string)
        writer.WriteString("bench-topic");
        // Partition (4 bytes)
        writer.WriteInt32(0);
        // Payload size (4 bytes) + raw bytes
        writer.WriteBytes(_nativeProduceFrame.AsSpan(0, Math.Min(PayloadSize, _nativeProduceFrame.Length)));
        return writer.Position;
    }

    /// <summary>
    /// Reads a pre-built Surgewave native produce frame using <see cref="SurgewavePayloadReader"/>.
    /// Measures binary decode cost (big-endian field reads).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Deserialization")]
    public int Native_Deserialize_ProduceFrame()
    {
        var reader = new SurgewavePayloadReader(_nativeProduceFrame.AsSpan());
        var opCode = reader.ReadUInt16();          // OpCode
        var correlationId = reader.ReadInt32();    // Correlation ID
        var topic = reader.ReadString();           // Topic
        var partition = reader.ReadInt32();        // Partition
        var payload = reader.ReadBytes();          // Payload (with length prefix)
        return reader.Position;
    }

    // =========================================================================
    // Kafka Wire Protocol Deserialization
    // =========================================================================

    /// <summary>
    /// Parses a pre-built Kafka Produce request (v9 wire format).
    /// Baseline for Kafka protocol parsing cost.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Deserialization")]
    public object Kafka_Deserialize_ProduceRequest()
        => _kafkaHandler.ParseRequest(_kafkaProduceRequest);

    /// <summary>
    /// Parses a pre-built Kafka Fetch request (v12 wire format).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Deserialization")]
    public object Kafka_Deserialize_FetchRequest()
        => _kafkaHandler.ParseRequest(_kafkaFetchRequest);

    /// <summary>
    /// Parses a pre-built Kafka Metadata request (v9 wire format).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Deserialization")]
    public object Kafka_Deserialize_MetadataRequest()
        => _kafkaHandler.ParseRequest(_kafkaMetadataRequest);

    // =========================================================================
    // Message Size Overhead (framing cost vs raw payload)
    // =========================================================================

    /// <summary>
    /// Returns the total serialized byte count for each protocol's framing of
    /// a <see cref="PayloadSize"/>-byte message, so callers can compare overhead.
    /// Not a performance benchmark — used to characterize wire efficiency.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("MessageSize")]
    public int Native_FrameSize()
    {
        // OpCode(2) + CorrelationId(4) + TopicLen(2) + Topic("bench-topic"=11 UTF-8) + Partition(4) + PayloadLen(4) + Payload
        const int fixedHeader = 2 + 4 + 2 + 11 + 4 + 4;
        return fixedHeader + PayloadSize;
    }

    [Benchmark]
    [BenchmarkCategory("MessageSize")]
    public int WebSocket_FrameSize() => _wsProduceJson.Length;

    [Benchmark]
    [BenchmarkCategory("MessageSize")]
    public int Kafka_FrameSize() => _kafkaProduceRequest.Length;

    // =========================================================================
    // Private helpers — build binary test data
    // =========================================================================

    private static byte[] BuildNativeProduceFrame(int payloadSize)
    {
        var buf = new byte[2 + 4 + 2 + 11 + 4 + 4 + payloadSize];
        var writer = new SurgewavePayloadWriter(buf.AsSpan());
        writer.WriteUInt16((ushort)SurgewaveOpCode.Produce);
        writer.WriteInt32(1);
        writer.WriteString("bench-topic");
        writer.WriteInt32(0);
        var payload = new byte[payloadSize];
        Random.Shared.NextBytes(payload);
        writer.WriteBytes(payload.AsSpan());
        return buf[..writer.Position];
    }

    private static byte[] BuildKafkaProduceRequest(int payloadSize)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);

        WriteBE(w, (short)0);      // API Key: Produce
        WriteBE(w, (short)9);      // API Version: 9
        WriteBE(w, 1);             // Correlation ID
        WriteKafkaString(w, "benchmark");
        w.Write((byte)0);          // tagged fields (header)

        w.Write((byte)0);          // transactional ID (null compact string)
        WriteBE(w, (short)-1);     // acks = all
        WriteBE(w, 30_000);        // timeout ms

        w.Write((byte)2);          // topics array length + 1
        WriteKafkaCompactString(w, "bench-topic");
        w.Write((byte)2);          // partitions array length + 1
        WriteBE(w, 0);             // partition index
        var records = BuildMinimalRecordBatch(payloadSize);
        WriteKafkaCompactBytes(w, records);
        w.Write((byte)0);          // tagged fields (partition)
        w.Write((byte)0);          // tagged fields (topic)
        w.Write((byte)0);          // tagged fields (root)

        return ms.ToArray();
    }

    private static byte[] BuildKafkaFetchRequest()
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);

        WriteBE(w, (short)1);      // API Key: Fetch
        WriteBE(w, (short)12);     // API Version: 12
        WriteBE(w, 2);             // Correlation ID
        WriteKafkaString(w, "benchmark");
        w.Write((byte)0);          // tagged fields (header)

        WriteBE(w, -1);            // ReplicaId
        WriteBE(w, 500);           // MaxWaitMs
        WriteBE(w, 1);             // MinBytes
        WriteBE(w, 1024 * 1024);   // MaxBytes
        w.Write((byte)0);          // IsolationLevel
        WriteBE(w, 0);             // SessionId
        WriteBE(w, -1);            // SessionEpoch

        w.Write((byte)2);          // topics array
        WriteKafkaCompactString(w, "bench-topic");
        w.Write((byte)2);          // partitions array
        WriteBE(w, 0);             // partition
        WriteBE(w, -1);            // CurrentLeaderEpoch
        WriteBE(w, 0L);            // FetchOffset
        WriteBE(w, -1);            // LastFetchedEpoch
        WriteBE(w, -1L);           // LogStartOffset
        WriteBE(w, 1024 * 1024);   // PartitionMaxBytes
        w.Write((byte)0);          // tagged fields (partition)
        w.Write((byte)0);          // tagged fields (topic)

        w.Write((byte)1);          // ForgottenTopics (empty compact array)
        w.Write((byte)1);          // RackId (empty compact string)
        w.Write((byte)0);          // tagged fields (root)

        return ms.ToArray();
    }

    private static byte[] BuildKafkaMetadataRequest()
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);

        WriteBE(w, (short)3);      // API Key: Metadata
        WriteBE(w, (short)9);      // API Version: 9
        WriteBE(w, 3);             // Correlation ID
        WriteKafkaString(w, "benchmark");
        w.Write((byte)0);          // tagged fields (header)

        w.Write((byte)2);          // topics array length + 1
        WriteKafkaCompactString(w, "bench-topic");
        w.Write((byte)0);          // tagged fields (topic)
        w.Write((byte)1);          // AllowAutoTopicCreation
        w.Write((byte)0);          // IncludeTopicAuthorizedOperations
        w.Write((byte)0);          // tagged fields (root)

        return ms.ToArray();
    }

    private static byte[] BuildMinimalRecordBatch(int payloadSize)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);

        WriteBE(w, 0L);            // Base Offset
        var batchLenPos = ms.Position;
        WriteBE(w, 0);             // Batch Length placeholder
        WriteBE(w, 0);             // Partition Leader Epoch
        w.Write((byte)2);          // Magic
        WriteBE(w, 0u);            // CRC (placeholder)
        WriteBE(w, (short)0);      // Attributes
        WriteBE(w, 0);             // Last Offset Delta
        WriteBE(w, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()); // Base Timestamp
        WriteBE(w, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()); // Max Timestamp
        WriteBE(w, -1L);           // Producer ID
        WriteBE(w, (short)-1);     // Producer Epoch
        WriteBE(w, -1);            // Base Sequence
        WriteBE(w, 1);             // Record Count

        // Single record
        var valueBytes = new byte[payloadSize];
        Random.Shared.NextBytes(valueBytes);
        var recordLen = 1 + 1 + 1 + 1 + 1 + 1 + payloadSize + 1; // approx. for varint fields
        WriteVarInt(w, recordLen);
        w.Write((byte)0);          // Attributes
        WriteVarInt(w, 0);         // Timestamp delta
        WriteVarInt(w, 0);         // Offset delta
        WriteVarInt(w, -1);        // Key (null)
        WriteVarInt(w, payloadSize);
        w.Write(valueBytes);
        WriteVarInt(w, 0);         // Headers count

        return ms.ToArray();
    }

    private static byte[] BuildWebSocketProduceJson(int payloadSize)
    {
        var msg = new WsBenchProduceMessage
        {
            Key = "bench-key",
            Value = new string('x', payloadSize)
        };
        return JsonSerializer.SerializeToUtf8Bytes(msg, WsBenchJsonContext.Default.WsBenchProduceMessage);
    }

    private static byte[] BuildWebSocketConsumeJson(int payloadSize)
    {
        var msg = new WsBenchConsumeMessage
        {
            Topic = "bench-topic",
            Partition = 0,
            Offset = 42L,
            Key = "bench-key",
            Value = new string('x', payloadSize),
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
        return JsonSerializer.SerializeToUtf8Bytes(msg, WsBenchJsonContext.Default.WsBenchConsumeMessage);
    }

    private static byte[] BuildWebSocketSubscribeJson()
    {
        var msg = new WsBenchSubscribeMessage
        {
            Action = "subscribe",
            Topics = ["bench-topic", "other-topic"]
        };
        return JsonSerializer.SerializeToUtf8Bytes(msg, WsBenchJsonContext.Default.WsBenchSubscribeMessage);
    }

    // -------------------------------------------------------------------------
    // Kafka binary encoding helpers (mirrors ProtocolBenchmarks helpers)
    // -------------------------------------------------------------------------

    private static void WriteBE(BinaryWriter w, short v)
    {
        w.Write((byte)(v >> 8));
        w.Write((byte)v);
    }

    private static void WriteBE(BinaryWriter w, int v)
    {
        w.Write((byte)(v >> 24));
        w.Write((byte)(v >> 16));
        w.Write((byte)(v >> 8));
        w.Write((byte)v);
    }

    private static void WriteBE(BinaryWriter w, long v)
    {
        w.Write((byte)(v >> 56));
        w.Write((byte)(v >> 48));
        w.Write((byte)(v >> 40));
        w.Write((byte)(v >> 32));
        w.Write((byte)(v >> 24));
        w.Write((byte)(v >> 16));
        w.Write((byte)(v >> 8));
        w.Write((byte)v);
    }

    private static void WriteBE(BinaryWriter w, uint v)
    {
        w.Write((byte)(v >> 24));
        w.Write((byte)(v >> 16));
        w.Write((byte)(v >> 8));
        w.Write((byte)v);
    }

    private static void WriteKafkaString(BinaryWriter w, string s)
    {
        var bytes = Encoding.UTF8.GetBytes(s);
        WriteBE(w, (short)bytes.Length);
        w.Write(bytes);
    }

    private static void WriteKafkaCompactString(BinaryWriter w, string s)
    {
        var bytes = Encoding.UTF8.GetBytes(s);
        WriteVarInt(w, bytes.Length + 1);
        w.Write(bytes);
    }

    private static void WriteKafkaCompactBytes(BinaryWriter w, byte[] data)
    {
        WriteVarInt(w, data.Length + 1);
        w.Write(data);
    }

    private static void WriteVarInt(BinaryWriter w, int value)
    {
        var v = (uint)((value << 1) ^ (value >> 31));
        while ((v & ~0x7Fu) != 0)
        {
            w.Write((byte)((v & 0x7F) | 0x80));
            v >>= 7;
        }
        w.Write((byte)v);
    }
}

// =========================================================================
// Minimal WebSocket message DTOs + source-generated JSON context
// (mirrors Kuestenlogik.Surgewave.Protocol.WebSocket without the ASP.NET Core dependency)
// =========================================================================

/// <summary>Produce message shape used in WebSocket protocol benchmarks.</summary>
public sealed class WsBenchProduceMessage
{
    [JsonPropertyName("key")]   public string? Key { get; set; }
    [JsonPropertyName("value")] public string? Value { get; set; }
}

/// <summary>Consume message shape used in WebSocket protocol benchmarks.</summary>
public sealed class WsBenchConsumeMessage
{
    [JsonPropertyName("topic")]     public required string Topic { get; set; }
    [JsonPropertyName("partition")] public required int Partition { get; set; }
    [JsonPropertyName("offset")]    public required long Offset { get; set; }
    [JsonPropertyName("key")]       public string? Key { get; set; }
    [JsonPropertyName("value")]     public string? Value { get; set; }
    [JsonPropertyName("timestamp")] public long Timestamp { get; set; }
}

/// <summary>Subscribe message shape used in WebSocket protocol benchmarks.</summary>
public sealed class WsBenchSubscribeMessage
{
    [JsonPropertyName("action")] public required string Action { get; set; }
    [JsonPropertyName("topics")] public required List<string> Topics { get; set; }
}

/// <summary>
/// Source-generated JSON serialization context for WebSocket benchmark DTOs.
/// Avoids reflection at runtime and enables AOT compatibility.
/// </summary>
[JsonSerializable(typeof(WsBenchProduceMessage))]
[JsonSerializable(typeof(WsBenchConsumeMessage))]
[JsonSerializable(typeof(WsBenchSubscribeMessage))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
internal sealed partial class WsBenchJsonContext : JsonSerializerContext { }
