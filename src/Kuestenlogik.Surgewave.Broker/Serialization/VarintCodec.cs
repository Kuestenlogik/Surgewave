namespace Kuestenlogik.Surgewave.Broker.Serialization;

/// <summary>
/// Inline varint/varlong encoding and decoding for high-performance serialization.
/// Zero-allocation methods using Span-based API.
/// </summary>
public static class VarintCodec
{
    /// <summary>
    /// Write varint inline to span, returns bytes written.
    /// </summary>
    public static int WriteVarInt(Span<byte> span, int value)
    {
        var pos = 0;
        var uval = (uint)value;
        while (uval >= 0x80)
        {
            span[pos++] = (byte)(uval | 0x80);
            uval >>= 7;
        }
        span[pos++] = (byte)uval;
        return pos;
    }

    /// <summary>
    /// Write varlong inline to span, returns bytes written.
    /// </summary>
    public static int WriteVarLong(Span<byte> span, long value)
    {
        var pos = 0;
        var uval = (ulong)value;
        while (uval >= 0x80)
        {
            span[pos++] = (byte)(uval | 0x80);
            uval >>= 7;
        }
        span[pos++] = (byte)uval;
        return pos;
    }

    /// <summary>
    /// Read varint inline from span (avoids KafkaProtocolReader allocation).
    /// </summary>
    public static int ReadVarInt(ReadOnlySpan<byte> span, ref int pos)
    {
        int result = 0;
        int shift = 0;
        byte b;
        do
        {
            b = span[pos++];
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0 && shift < 35);
        return result;
    }

    /// <summary>
    /// Read varlong inline from span (avoids KafkaProtocolReader allocation).
    /// </summary>
    public static long ReadVarLong(ReadOnlySpan<byte> span, ref int pos)
    {
        long result = 0;
        int shift = 0;
        byte b;
        do
        {
            b = span[pos++];
            result |= (long)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0 && shift < 70);
        return result;
    }
}
