using Kuestenlogik.Surgewave.Protocol.Native.Serialization;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// GetBlockLength must consume exactly the bytes Decode consumes (#83): the broker's produce path
/// uses it to find where the next message starts, so any divergence would silently mis-frame the
/// rest of the batch instead of failing.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class NativeMessageHeaderCodecTests
{
    public static TheoryData<Dictionary<string, byte[]>?> HeaderCases() => new()
    {
        (Dictionary<string, byte[]>?)null,
        new Dictionary<string, byte[]>(),
        new Dictionary<string, byte[]> { ["k"] = "v"u8.ToArray() },
        new Dictionary<string, byte[]> { ["key"] = [] },                       // empty value
        new Dictionary<string, byte[]> { ["key"] = null! },                    // null value
        new Dictionary<string, byte[]> { [""] = "v"u8.ToArray() },             // empty key
        new Dictionary<string, byte[]>
        {
            ["trace-id"] = "abc-123"u8.ToArray(),
            ["empty"] = [],
            ["null"] = null!,
            ["binary"] = [0, 1, 2, 255],
        },
    };

    [Theory]
    [MemberData(nameof(HeaderCases))]
    public void GetBlockLength_MatchesDecodeAndEncode(Dictionary<string, byte[]>? headers)
    {
        var buffer = new byte[NativeMessageHeaderCodec.EncodedSize(headers)];
        var written = NativeMessageHeaderCodec.Encode(headers, buffer);

        NativeMessageHeaderCodec.Decode(buffer, out var consumed);

        Assert.Equal(written, consumed);
        Assert.Equal(consumed, NativeMessageHeaderCodec.GetBlockLength(buffer));
    }

    [Fact]
    public void GetBlockLength_TrailingBytes_StopsAtBlockEnd()
    {
        // The produce path hands in the rest of the payload, not just the block: the length must
        // describe the block only, so the next message starts in the right place.
        var headers = new Dictionary<string, byte[]> { ["a"] = "1"u8.ToArray() };
        var block = new byte[NativeMessageHeaderCodec.EncodedSize(headers)];
        var written = NativeMessageHeaderCodec.Encode(headers, block);

        var withTrailer = new byte[block.Length + 16];
        block.CopyTo(withTrailer, 0);

        Assert.Equal(written, NativeMessageHeaderCodec.GetBlockLength(withTrailer));
    }

    [Fact]
    public void GetBlockLength_NegativeKeyLength_Throws()
    {
        // Would otherwise walk the position backwards and return a plausible but wrong length.
        var source = new byte[12];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(source, 1);            // one header
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(source.AsSpan(4), -4); // negative key length

        Assert.Throws<ArgumentOutOfRangeException>(() => NativeMessageHeaderCodec.GetBlockLength(source));
    }

    [Fact]
    public void GetBlockLength_KeyLengthBeyondBuffer_Throws()
    {
        var source = new byte[12];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(source, 1);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(source.AsSpan(4), 1000);

        Assert.Throws<ArgumentOutOfRangeException>(() => NativeMessageHeaderCodec.GetBlockLength(source));
    }

    [Fact]
    public void GetBlockLength_ValueLengthBeyondBuffer_Throws()
    {
        var source = new byte[16];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(source, 1);              // one header
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(source.AsSpan(4), 1);    // key length 1
        source[8] = (byte)'k';
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(source.AsSpan(9), 1000); // value length lies

        Assert.Throws<ArgumentOutOfRangeException>(() => NativeMessageHeaderCodec.GetBlockLength(source));
    }
}
