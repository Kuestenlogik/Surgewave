using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// Writes AMQP 0.9.1 frames to a <see cref="Stream"/>.
/// Provides helper methods for all framing operations required by the server side.
/// </summary>
internal sealed class AmqpFrameWriter
{
    private readonly Stream _stream;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public AmqpFrameWriter(Stream stream)
    {
        _stream = stream;
    }

    // -------------------------------------------------------------------------
    // Low-level frame writing
    // -------------------------------------------------------------------------

    /// <summary>
    /// Writes a raw AMQP frame to the stream.
    /// </summary>
    public async ValueTask WriteFrameAsync(byte type, ushort channel, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        // Frame: type(1) + channel(2) + size(4) + payload(N) + frame-end(1)
        var size = payload.Length;
        var buf = ArrayPool<byte>.Shared.Rent(7 + size + 1);
        try
        {
            buf[0] = type;
            BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(1), channel);
            BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(3), (uint)size);
            payload.Span.CopyTo(buf.AsSpan(7));
            buf[7 + size] = AmqpFrameType.FrameEnd;

            await _writeLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await _stream.WriteAsync(buf.AsMemory(0, 7 + size + 1), ct).ConfigureAwait(false);
                await _stream.FlushAsync(ct).ConfigureAwait(false);
            }
            finally
            {
                _writeLock.Release();
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }

    // -------------------------------------------------------------------------
    // Method frame helpers
    // -------------------------------------------------------------------------

    /// <summary>Writes Connection.Start (class 10, method 10).</summary>
    public ValueTask WriteConnectionStartAsync(ushort channel, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(64);
        // class-id=10, method-id=10
        WriteShort(ms, 10);
        WriteShort(ms, 10);
        // version-major, version-minor
        ms.WriteByte(0);
        ms.WriteByte(9);
        // server-properties (empty table)
        WriteTable(ms, new Dictionary<string, string>());
        // mechanisms
        WriteLongString(ms, "PLAIN AMQPLAIN"u8.ToArray());
        // locales
        WriteLongString(ms, "en_US"u8.ToArray());
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes Connection.Tune (class 10, method 30).</summary>
    public ValueTask WriteConnectionTuneAsync(ushort channel, ushort maxChannels, uint maxFrameSize, ushort heartbeat, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(14);
        WriteShort(ms, 10);
        WriteShort(ms, 30);
        WriteShort(ms, maxChannels);
        WriteLong(ms, maxFrameSize);
        WriteShort(ms, heartbeat);
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes Connection.OpenOk (class 10, method 41).</summary>
    public ValueTask WriteConnectionOpenOkAsync(ushort channel, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(8);
        WriteShort(ms, 10);
        WriteShort(ms, 41);
        WriteShortString(ms, ""); // known-hosts (empty)
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes Connection.Close (class 10, method 50).</summary>
    public ValueTask WriteConnectionCloseAsync(ushort channel, ushort replyCode, string replyText, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(32);
        WriteShort(ms, 10);
        WriteShort(ms, 50);
        WriteShort(ms, replyCode);
        WriteShortString(ms, replyText);
        WriteShort(ms, 0); // class-id
        WriteShort(ms, 0); // method-id
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes Connection.CloseOk (class 10, method 51).</summary>
    public ValueTask WriteConnectionCloseOkAsync(ushort channel, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(4);
        WriteShort(ms, 10);
        WriteShort(ms, 51);
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes Channel.OpenOk (class 20, method 11).</summary>
    public ValueTask WriteChannelOpenOkAsync(ushort channel, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(8);
        WriteShort(ms, 20);
        WriteShort(ms, 11);
        WriteLongString(ms, []); // channel-id (empty for 0.9.1)
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes Channel.CloseOk (class 20, method 41).</summary>
    public ValueTask WriteChannelCloseOkAsync(ushort channel, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(4);
        WriteShort(ms, 20);
        WriteShort(ms, 41);
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes Exchange.DeclareOk (class 40, method 11).</summary>
    public ValueTask WriteExchangeDeclareOkAsync(ushort channel, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(4);
        WriteShort(ms, 40);
        WriteShort(ms, 11);
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes Queue.DeclareOk (class 50, method 11).</summary>
    public ValueTask WriteQueueDeclareOkAsync(ushort channel, string queueName, uint messageCount = 0, uint consumerCount = 0, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(32);
        WriteShort(ms, 50);
        WriteShort(ms, 11);
        WriteShortString(ms, queueName);
        WriteLong(ms, messageCount);
        WriteLong(ms, consumerCount);
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes Queue.BindOk (class 50, method 21).</summary>
    public ValueTask WriteQueueBindOkAsync(ushort channel, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(4);
        WriteShort(ms, 50);
        WriteShort(ms, 21);
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes Basic.ConsumeOk (class 60, method 21).</summary>
    public ValueTask WriteBasicConsumeOkAsync(ushort channel, string consumerTag, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(16);
        WriteShort(ms, 60);
        WriteShort(ms, 21);
        WriteShortString(ms, consumerTag);
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes Basic.Deliver (class 60, method 60) + content header + body.</summary>
    public async ValueTask WriteBasicDeliverAsync(
        ushort channel,
        string consumerTag,
        ulong deliveryTag,
        bool redelivered,
        string exchange,
        string routingKey,
        ReadOnlyMemory<byte> body,
        CancellationToken ct = default)
    {
        // Method frame
        using (var ms = new MemoryStream(64))
        {
            WriteShort(ms, 60);
            WriteShort(ms, 60);
            WriteShortString(ms, consumerTag);
            WriteLongLong(ms, deliveryTag);
            ms.WriteByte(redelivered ? (byte)1 : (byte)0);
            WriteShortString(ms, exchange);
            WriteShortString(ms, routingKey);
            await WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct).ConfigureAwait(false);
        }

        // Content header frame: class-id=60, weight=0, body-size, property-flags=0
        using (var ms = new MemoryStream(16))
        {
            WriteShort(ms, 60);   // class-id
            WriteShort(ms, 0);    // weight (always 0)
            WriteLongLong(ms, (ulong)body.Length);
            WriteShort(ms, 0);    // property flags (none)
            await WriteFrameAsync(AmqpFrameType.Header, channel, ms.ToArray(), ct).ConfigureAwait(false);
        }

        // Body frame
        if (body.Length > 0)
            await WriteFrameAsync(AmqpFrameType.Body, channel, body, ct).ConfigureAwait(false);
    }

    /// <summary>Writes Basic.AckOk (class 60, method 80).</summary>
    public ValueTask WriteBasicAckAsync(ushort channel, ulong deliveryTag, bool multiple = false, CancellationToken ct = default)
    {
        using var ms = new MemoryStream(12);
        WriteShort(ms, 60);
        WriteShort(ms, 80);
        WriteLongLong(ms, deliveryTag);
        ms.WriteByte(multiple ? (byte)1 : (byte)0);
        return WriteFrameAsync(AmqpFrameType.Method, channel, ms.ToArray(), ct);
    }

    /// <summary>Writes a heartbeat frame.</summary>
    public ValueTask WriteHeartbeatAsync(CancellationToken ct = default)
        => WriteFrameAsync(AmqpFrameType.Heartbeat, 0, ReadOnlyMemory<byte>.Empty, ct);

    // -------------------------------------------------------------------------
    // Primitive encoding helpers
    // -------------------------------------------------------------------------

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

    private static void WriteTable(Stream s, IReadOnlyDictionary<string, string> table)
    {
        // Write an empty table (4-byte length = 0)
        WriteLong(s, 0);
        _ = table; // future: write actual fields
    }
}
