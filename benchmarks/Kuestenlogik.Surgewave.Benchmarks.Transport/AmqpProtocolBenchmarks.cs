using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Protocol.Amqp;

namespace Kuestenlogik.Surgewave.Benchmarks.Transport;

/// <summary>
/// AMQP 0.9.1 frame serialization/deserialization micro-benchmarks.
/// No broker is required — all benchmarks operate entirely in-process.
///
/// Frame writing is replicated inline (AmqpFrameWriter is internal) using the same
/// ArrayPool + BinaryPrimitives approach the production writer uses, so the benchmark
/// faithfully measures the same byte-level work.
///
/// Categories:
///   AmqpWrite         — Frame serialization (write to MemoryStream / byte array)
///   AmqpRead          — Frame deserialization (parse from byte array via MemoryStream)
///   AmqpTopicMapping  — AmqpTopicMapper.MapToSurgewaveTopic for Direct/Fanout/Topic exchange types
///   AmqpPatternMatch  — AmqpTopicPatternMatcher.Matches for various wildcard patterns
///   AmqpFrameSize     — Framing overhead vs raw payload across payload sizes
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("AmqpProtocol", "Protocol", "Transport")]
public class AmqpProtocolBenchmarks
{
    // ── Pre-built wire frames (built once in GlobalSetup) ────────────────────

    private byte[] _methodFrameBytes = null!;   // Connection.Start
    private byte[] _headerFrameBytes = null!;   // content-header for PayloadSize body
    private byte[] _bodyFrameBytes   = null!;   // content-body of PayloadSize bytes
    private byte[] _heartbeatBytes   = null!;   // heartbeat frame (8 bytes)

    // ── Reusable write buffer (avoids allocation in write benchmarks) ─────────

    private byte[] _writeBuffer = null!;

    // ── Topic-mapping fixtures ────────────────────────────────────────────────

    private static readonly (string exchange, string routingKey, AmqpExchangeType type)[] TopicMappingCases =
    [
        ("",           "orders.created",   AmqpExchangeType.Direct),
        ("my-exchange","",                 AmqpExchangeType.Fanout),
        ("",           "orders.*.created", AmqpExchangeType.Topic),
        ("",           "alerts",           AmqpExchangeType.Headers),
    ];

    // ── Pattern-matching fixtures ─────────────────────────────────────────────

    private static readonly (string pattern, string routingKey)[] PatternMatchCases =
    [
        ("#",               "any.key.here"),           // wildcard-all fast path
        ("orders.created",  "orders.created"),         // exact match
        ("orders.*",        "orders.created"),         // single-word wildcard match
        ("orders.#",        "orders.created.eu"),      // multi-word wildcard match
        ("*.created",       "orders.created"),         // leading wildcard
        ("orders.#.eu",     "orders.shipped.bulk.eu"), // interior # wildcard
        ("alerts.critical", "alerts.warning"),         // no match
        ("a.b.c.d.e",       "a.b.c.d.x"),             // deep no-match
    ];

    [Params(64, 256, 1024, 4096)]
    public int PayloadSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _methodFrameBytes = BuildMethodFrame();
        _headerFrameBytes = BuildHeaderFrame(PayloadSize);
        _bodyFrameBytes   = BuildBodyFrame(PayloadSize);
        _heartbeatBytes   = BuildHeartbeatFrame();

        // Large enough for any frame we write in benchmarks
        _writeBuffer = new byte[PayloadSize + 256];
    }

    // =========================================================================
    // AmqpWrite — frame serialization (mirrors AmqpFrameWriter internals)
    // =========================================================================

    /// <summary>
    /// Measures the cost of framing and writing a Connection.Start method payload.
    /// Baseline: fixed-size method frame, no payload variation.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("AmqpWrite", "AmqpProtocol")]
    public int Write_MethodFrame_ConnectionStart()
    {
        // Build Connection.Start payload inline
        using var ms = new MemoryStream(64);
        WriteShort(ms, 10);   // class Connection
        WriteShort(ms, 10);   // method Start
        ms.WriteByte(0);      // version-major
        ms.WriteByte(9);      // version-minor
        WriteLong(ms, 0);     // server-properties table (empty)
        WriteLongString(ms, "PLAIN AMQPLAIN"u8.ToArray());
        WriteLongString(ms, "en_US"u8.ToArray());

        var payload = ms.ToArray();
        return SerializeFrame(AmqpFrameTypeConst.Method, 0, payload, _writeBuffer);
    }

    /// <summary>
    /// Measures the cost of framing a Connection.Tune method frame.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("AmqpWrite", "AmqpProtocol")]
    public int Write_MethodFrame_ConnectionTune()
    {
        using var ms = new MemoryStream(14);
        WriteShort(ms, 10); WriteShort(ms, 30);  // Connection.Tune
        WriteShort(ms, 2047);
        WriteLong(ms, 131072);
        WriteShort(ms, 60);

        var payload = ms.ToArray();
        return SerializeFrame(AmqpFrameTypeConst.Method, 0, payload, _writeBuffer);
    }

    /// <summary>
    /// Measures the cost of framing a Queue.DeclareOk method frame.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("AmqpWrite", "AmqpProtocol")]
    public int Write_MethodFrame_QueueDeclareOk()
    {
        using var ms = new MemoryStream(32);
        WriteShort(ms, 50); WriteShort(ms, 11); // Queue.DeclareOk
        WriteShortString(ms, "bench-queue-amqp");
        WriteLong(ms, 0); WriteLong(ms, 0);

        var payload = ms.ToArray();
        return SerializeFrame(AmqpFrameTypeConst.Method, 1, payload, _writeBuffer);
    }

    /// <summary>
    /// Measures the cost of framing a heartbeat frame (empty payload, 8 bytes total).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("AmqpWrite", "AmqpProtocol")]
    public int Write_HeartbeatFrame()
        => SerializeFrame(AmqpFrameTypeConst.Heartbeat, 0, [], _writeBuffer);

    /// <summary>
    /// Measures the cost of framing a content-header frame (fixed 16-byte payload).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("AmqpWrite", "AmqpProtocol")]
    public int Write_ContentHeaderFrame()
    {
        using var ms = new MemoryStream(16);
        WriteShort(ms, 60); WriteShort(ms, 0);        // class-id=Basic, weight=0
        WriteLongLong(ms, (ulong)PayloadSize);         // body-size
        WriteShort(ms, 0);                             // property flags

        var payload = ms.ToArray();
        return SerializeFrame(AmqpFrameTypeConst.Header, 1, payload, _writeBuffer);
    }

    /// <summary>
    /// Measures the cost of framing a content-body frame of <see cref="PayloadSize"/> bytes.
    /// This is the hot path for every published message body in AMQP 0.9.1.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("AmqpWrite", "AmqpProtocol")]
    public int Write_ContentBodyFrame()
    {
        // Payload is the raw body — slice from pre-built frame (skip 7-byte header)
        var payload = _bodyFrameBytes.AsSpan(7, PayloadSize);
        return SerializeFrameSpan(AmqpFrameTypeConst.Body, 1, payload, _writeBuffer);
    }

    // =========================================================================
    // AmqpRead — frame deserialization speed (AmqpFrameReader is internal;
    //            we exercise it indirectly via MemoryStream round-trips)
    // =========================================================================

    /// <summary>
    /// Measures the cost of reading and parsing a Connection.Start method frame.
    /// Baseline: fixed-size method frame, no payload variation.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("AmqpRead", "AmqpProtocol")]
    public (byte type, ushort channel, int payloadLen) Read_MethodFrame()
        => ParseFrame(_methodFrameBytes);

    /// <summary>
    /// Measures the cost of reading a content-header frame.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("AmqpRead", "AmqpProtocol")]
    public (byte type, ushort channel, int payloadLen) Read_HeaderFrame()
        => ParseFrame(_headerFrameBytes);

    /// <summary>
    /// Measures the cost of reading a content-body frame of <see cref="PayloadSize"/> bytes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("AmqpRead", "AmqpProtocol")]
    public (byte type, ushort channel, int payloadLen) Read_BodyFrame()
        => ParseFrame(_bodyFrameBytes);

    /// <summary>
    /// Measures the cost of reading a heartbeat frame.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("AmqpRead", "AmqpProtocol")]
    public (byte type, ushort channel, int payloadLen) Read_HeartbeatFrame()
        => ParseFrame(_heartbeatBytes);

    // =========================================================================
    // AmqpTopicMapping — AmqpTopicMapper speed
    // =========================================================================

    /// <summary>
    /// Baseline: Direct exchange mapping — routing key becomes Surgewave topic directly.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("AmqpTopicMapping", "AmqpProtocol")]
    public string TopicMapper_Direct()
        => AmqpTopicMapper.MapToSurgewaveTopic("", "orders.created", AmqpExchangeType.Direct);

    /// <summary>Fanout exchange mapping — exchange name becomes Surgewave topic.</summary>
    [Benchmark]
    [BenchmarkCategory("AmqpTopicMapping", "AmqpProtocol")]
    public string TopicMapper_Fanout()
        => AmqpTopicMapper.MapToSurgewaveTopic("my-exchange", "", AmqpExchangeType.Fanout);

    /// <summary>Topic exchange mapping — routing key becomes Surgewave topic.</summary>
    [Benchmark]
    [BenchmarkCategory("AmqpTopicMapping", "AmqpProtocol")]
    public string TopicMapper_Topic()
        => AmqpTopicMapper.MapToSurgewaveTopic("", "orders.*.created", AmqpExchangeType.Topic);

    /// <summary>Headers exchange mapping (falls back to direct-style routing).</summary>
    [Benchmark]
    [BenchmarkCategory("AmqpTopicMapping", "AmqpProtocol")]
    public string TopicMapper_Headers()
        => AmqpTopicMapper.MapToSurgewaveTopic("", "alerts", AmqpExchangeType.Headers);

    /// <summary>Queue-to-consumer-group mapping.</summary>
    [Benchmark]
    [BenchmarkCategory("AmqpTopicMapping", "AmqpProtocol")]
    public string TopicMapper_QueueToConsumerGroup()
        => AmqpTopicMapper.MapQueueToConsumerGroup("my-queue-name");

    /// <summary>
    /// Full mapping loop — all exchange types + queue mapping in one pass.
    /// Represents the per-message cost across a mixed workload.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("AmqpTopicMapping", "AmqpProtocol")]
    public string TopicMapper_AllTypes()
    {
        var result = string.Empty;
        foreach (var (exchange, routingKey, type) in TopicMappingCases)
            result = AmqpTopicMapper.MapToSurgewaveTopic(exchange, routingKey, type);
        return result;
    }

    // =========================================================================
    // AmqpPatternMatch — AmqpTopicPatternMatcher speed
    // =========================================================================

    /// <summary>
    /// Baseline: '#' wildcard fast path (matches everything without further parsing).
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("AmqpPatternMatch", "AmqpProtocol")]
    public bool PatternMatcher_HashAll()
        => AmqpTopicMapper.MatchesTopicPattern("#", "any.key.here");

    /// <summary>Exact routing key match (no wildcards, string equality fast path).</summary>
    [Benchmark]
    [BenchmarkCategory("AmqpPatternMatch", "AmqpProtocol")]
    public bool PatternMatcher_Exact_Match()
        => AmqpTopicMapper.MatchesTopicPattern("orders.created", "orders.created");

    /// <summary>Single-word wildcard ('*') match — one word substitution.</summary>
    [Benchmark]
    [BenchmarkCategory("AmqpPatternMatch", "AmqpProtocol")]
    public bool PatternMatcher_Star_Match()
        => AmqpTopicMapper.MatchesTopicPattern("orders.*", "orders.created");

    /// <summary>Multi-word wildcard ('#') match — zero-or-more words.</summary>
    [Benchmark]
    [BenchmarkCategory("AmqpPatternMatch", "AmqpProtocol")]
    public bool PatternMatcher_Hash_Match()
        => AmqpTopicMapper.MatchesTopicPattern("orders.#", "orders.created.eu");

    /// <summary>No-match case — pattern and routing key diverge at the last word.</summary>
    [Benchmark]
    [BenchmarkCategory("AmqpPatternMatch", "AmqpProtocol")]
    public bool PatternMatcher_NoMatch()
        => AmqpTopicMapper.MatchesTopicPattern("alerts.critical", "alerts.warning");

    /// <summary>
    /// Deep no-match — 5-word pattern, mismatch only at the last word.
    /// Shows worst-case word-by-word scanning cost.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("AmqpPatternMatch", "AmqpProtocol")]
    public bool PatternMatcher_DeepNoMatch()
        => AmqpTopicMapper.MatchesTopicPattern("a.b.c.d.e", "a.b.c.d.x");

    /// <summary>
    /// Full pattern-match loop — all 8 pattern/key pairs in one pass.
    /// Represents mixed-workload cost for a subscriber with multiple bindings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("AmqpPatternMatch", "AmqpProtocol")]
    public bool PatternMatcher_AllCases()
    {
        var result = false;
        foreach (var (pattern, routingKey) in PatternMatchCases)
            result ^= AmqpTopicMapper.MatchesTopicPattern(pattern, routingKey);
        return result;
    }

    // =========================================================================
    // AmqpFrameSize — framing overhead characterisation
    // =========================================================================

    /// <summary>
    /// Total wire byte count of an AMQP body frame for <see cref="PayloadSize"/>.
    /// 8 bytes fixed overhead: 7-byte header + 1-byte frame-end.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("AmqpFrameSize", "AmqpProtocol")]
    public int FrameSize_Body()
        => 7 + PayloadSize + 1;

    /// <summary>Wire byte count of a content-header frame (fixed 16 bytes payload).</summary>
    [Benchmark]
    [BenchmarkCategory("AmqpFrameSize", "AmqpProtocol")]
    public int FrameSize_Header()
        => 7 + 16 + 1;

    /// <summary>Wire byte count of a heartbeat frame (empty payload = 8 bytes total).</summary>
    [Benchmark]
    [BenchmarkCategory("AmqpFrameSize", "AmqpProtocol")]
    public int FrameSize_Heartbeat()
        => 7 + 0 + 1;

    // =========================================================================
    // Private helpers — AMQP wire format utilities
    // =========================================================================

    /// <summary>
    /// Serializes an AMQP frame into <paramref name="dest"/> and returns total byte count.
    /// Mirrors AmqpFrameWriter.WriteFrameAsync without the async/stream overhead,
    /// measuring pure frame-construction cost.
    /// </summary>
    private static int SerializeFrame(byte type, ushort channel, byte[] payload, byte[] dest)
    {
        var size = payload.Length;
        dest[0] = type;
        BinaryPrimitives.WriteUInt16BigEndian(dest.AsSpan(1), channel);
        BinaryPrimitives.WriteUInt32BigEndian(dest.AsSpan(3), (uint)size);
        payload.CopyTo(dest.AsSpan(7));
        dest[7 + size] = AmqpFrameTypeConst.FrameEnd;
        return 7 + size + 1;
    }

    private static int SerializeFrameSpan(byte type, ushort channel, ReadOnlySpan<byte> payload, byte[] dest)
    {
        var size = payload.Length;
        dest[0] = type;
        BinaryPrimitives.WriteUInt16BigEndian(dest.AsSpan(1), channel);
        BinaryPrimitives.WriteUInt32BigEndian(dest.AsSpan(3), (uint)size);
        payload.CopyTo(dest.AsSpan(7));
        dest[7 + size] = AmqpFrameTypeConst.FrameEnd;
        return 7 + size + 1;
    }

    /// <summary>
    /// Parses the 7-byte frame header from a raw AMQP frame byte array.
    /// Mirrors what AmqpFrameReader.ReadFrameAsync does for the header decode step.
    /// </summary>
    private static (byte type, ushort channel, int payloadLen) ParseFrame(byte[] frameBytes)
    {
        // Wire format: type(1) + channel(2) + size(4) + payload(N) + frame-end(1)
        var span    = frameBytes.AsSpan();
        var type    = span[0];
        var channel = BinaryPrimitives.ReadUInt16BigEndian(span[1..3]);
        var size    = (int)BinaryPrimitives.ReadUInt32BigEndian(span[3..7]);
        // Validate frame-end (mirrors AmqpFrameReader validation)
        _ = span[7 + size]; // frame-end byte at expected position
        return (type, channel, size);
    }

    // ── Wire-format build helpers ─────────────────────────────────────────────

    private static byte[] BuildMethodFrame()
    {
        using var ms = new MemoryStream(64);
        WriteShort(ms, 10); WriteShort(ms, 10);
        ms.WriteByte(0); ms.WriteByte(9);
        WriteLong(ms, 0);
        WriteLongString(ms, "PLAIN AMQPLAIN"u8.ToArray());
        WriteLongString(ms, "en_US"u8.ToArray());
        return WrapFrame(AmqpFrameTypeConst.Method, 0, ms.ToArray());
    }

    private static byte[] BuildHeaderFrame(int bodySize)
    {
        using var ms = new MemoryStream(16);
        WriteShort(ms, 60); WriteShort(ms, 0);
        WriteLongLong(ms, (ulong)bodySize);
        WriteShort(ms, 0);
        return WrapFrame(AmqpFrameTypeConst.Header, 1, ms.ToArray());
    }

    private static byte[] BuildBodyFrame(int bodySize)
    {
        var payload = new byte[bodySize];
        Random.Shared.NextBytes(payload);
        return WrapFrame(AmqpFrameTypeConst.Body, 1, payload);
    }

    private static byte[] BuildHeartbeatFrame()
        => WrapFrame(AmqpFrameTypeConst.Heartbeat, 0, []);

    private static byte[] WrapFrame(byte type, ushort channel, byte[] payload)
    {
        var buf = new byte[7 + payload.Length + 1];
        buf[0] = type;
        BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(1), channel);
        BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(3), (uint)payload.Length);
        payload.CopyTo(buf.AsSpan(7));
        buf[7 + payload.Length] = AmqpFrameTypeConst.FrameEnd;
        return buf;
    }

    private static void WriteShort(Stream s, ushort v)
    {
        s.WriteByte((byte)(v >> 8));
        s.WriteByte((byte)v);
    }

    private static void WriteLong(Stream s, uint v)
    {
        s.WriteByte((byte)(v >> 24));
        s.WriteByte((byte)(v >> 16));
        s.WriteByte((byte)(v >> 8));
        s.WriteByte((byte)v);
    }

    private static void WriteLongLong(Stream s, ulong v)
    {
        for (int i = 7; i >= 0; i--)
            s.WriteByte((byte)(v >> (i * 8)));
    }

    private static void WriteShortString(Stream s, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        s.WriteByte((byte)bytes.Length);
        s.Write(bytes);
    }

    private static void WriteLongString(Stream s, byte[] value)
    {
        WriteLong(s, (uint)value.Length);
        s.Write(value);
    }
}

/// <summary>
/// AMQP 0.9.1 frame type byte constants — mirrors AmqpFrameType (which is internal).
/// </summary>
internal static class AmqpFrameTypeConst
{
    public const byte Method    = 1;
    public const byte Header    = 2;
    public const byte Body      = 3;
    public const byte Heartbeat = 8;
    public const byte FrameEnd  = 0xCE;
}
