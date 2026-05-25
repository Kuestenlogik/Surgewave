using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Unit tests for VarInt/VarLong encoding/decoding.
/// Verifies correctness and round-trip behavior for various value ranges.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class VarIntTests
{
    private readonly ITestOutputHelper _output;

    public VarIntTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(127, 1)]
    [InlineData(128, 2)]
    [InlineData(16383, 2)]
    [InlineData(16384, 3)]
    [InlineData(2097151, 3)]
    [InlineData(2097152, 4)]
    [InlineData(268435455, 4)]
    [InlineData(268435456, 5)]
    [InlineData(int.MaxValue, 5)]
    public void WriteVarInt_ProducesCorrectLength(int value, int expectedLength)
    {
        var buffer = new byte[5];
        var bytesWritten = KafkaProtocolPrimitives.WriteVarInt(buffer, value);

        Assert.Equal(expectedLength, bytesWritten);
        _output.WriteLine($"Value {value} encoded to {bytesWritten} bytes");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(127)]
    [InlineData(128)]
    [InlineData(255)]
    [InlineData(16383)]
    [InlineData(16384)]
    [InlineData(2097151)]
    [InlineData(2097152)]
    [InlineData(268435455)]
    [InlineData(268435456)]
    [InlineData(int.MaxValue)]
    public void VarInt_RoundTrip_ProducesSameValue(int value)
    {
        var buffer = new byte[5];
        var bytesWritten = KafkaProtocolPrimitives.WriteVarInt(buffer, value);

        var (decoded, bytesRead) = KafkaProtocolPrimitives.ReadVarInt(buffer);

        Assert.Equal(value, decoded);
        Assert.Equal(bytesWritten, bytesRead);
    }

    [Theory]
    [InlineData(0L, 1)]
    [InlineData(127L, 1)]
    [InlineData(128L, 2)]
    [InlineData(16383L, 2)]
    [InlineData(16384L, 3)]
    [InlineData(2097151L, 3)]
    [InlineData(2097152L, 4)]
    [InlineData(long.MaxValue, 9)]  // 63 bits / 7 = 9 bytes for max positive long
    public void WriteVarLong_ProducesCorrectLength(long value, int expectedLength)
    {
        var buffer = new byte[10];
        var bytesWritten = KafkaProtocolPrimitives.WriteVarLong(buffer, value);

        Assert.Equal(expectedLength, bytesWritten);
        _output.WriteLine($"Value {value} encoded to {bytesWritten} bytes");
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(127L)]
    [InlineData(128L)]
    [InlineData(16383L)]
    [InlineData(16384L)]
    [InlineData(2097151L)]
    [InlineData(2097152L)]
    [InlineData(268435455L)]
    [InlineData(268435456L)]
    [InlineData(long.MaxValue)]
    public void VarLong_RoundTrip_ProducesSameValue(long value)
    {
        var buffer = new byte[10];
        var bytesWritten = KafkaProtocolPrimitives.WriteVarLong(buffer, value);

        var (decoded, bytesRead) = KafkaProtocolPrimitives.ReadVarLong(buffer);

        Assert.Equal(value, decoded);
        Assert.Equal(bytesWritten, bytesRead);
    }

    [Fact]
    public void WriteVarInt_KnownEncodings_MatchExpected()
    {
        // Test against known varint encodings
        var testCases = new (int value, byte[] expected)[]
        {
            (0, new byte[] { 0x00 }),
            (1, new byte[] { 0x01 }),
            (127, new byte[] { 0x7F }),
            (128, new byte[] { 0x80, 0x01 }),
            (300, new byte[] { 0xAC, 0x02 }),
            (16384, new byte[] { 0x80, 0x80, 0x01 }),
        };

        foreach (var (value, expected) in testCases)
        {
            var buffer = new byte[5];
            var bytesWritten = KafkaProtocolPrimitives.WriteVarInt(buffer, value);

            Assert.Equal(expected.Length, bytesWritten);
            for (int i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], buffer[i]);
            }
            _output.WriteLine($"Value {value}: {BitConverter.ToString(buffer, 0, bytesWritten)}");
        }
    }

    [Fact]
    public void ReadVarInt_KnownEncodings_DecodesCorrectly()
    {
        var testCases = new (byte[] encoded, int expected)[]
        {
            (new byte[] { 0x00 }, 0),
            (new byte[] { 0x01 }, 1),
            (new byte[] { 0x7F }, 127),
            (new byte[] { 0x80, 0x01 }, 128),
            (new byte[] { 0xAC, 0x02 }, 300),
            (new byte[] { 0x80, 0x80, 0x01 }, 16384),
        };

        foreach (var (encoded, expected) in testCases)
        {
            var (value, bytesRead) = KafkaProtocolPrimitives.ReadVarInt(encoded);

            Assert.Equal(expected, value);
            Assert.Equal(encoded.Length, bytesRead);
        }
    }

    [Fact]
    public void ZigzagEncode_ProducesExpectedValues()
    {
        Assert.Equal(0u, KafkaProtocolPrimitives.ZigzagEncode(0));
        Assert.Equal(1u, KafkaProtocolPrimitives.ZigzagEncode(-1));
        Assert.Equal(2u, KafkaProtocolPrimitives.ZigzagEncode(1));
        Assert.Equal(3u, KafkaProtocolPrimitives.ZigzagEncode(-2));
        Assert.Equal(4u, KafkaProtocolPrimitives.ZigzagEncode(2));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(100)]
    [InlineData(-100)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void ZigzagEncode_RoundTrip_ProducesSameValue(int value)
    {
        var encoded = KafkaProtocolPrimitives.ZigzagEncode(value);
        var decoded = KafkaProtocolPrimitives.ZigzagDecode(encoded);

        Assert.Equal(value, decoded);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(100L)]
    [InlineData(-100L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void ZigzagEncodeLong_RoundTrip_ProducesSameValue(long value)
    {
        var encoded = KafkaProtocolPrimitives.ZigzagEncode(value);
        var decoded = KafkaProtocolPrimitives.ZigzagDecode(encoded);

        Assert.Equal(value, decoded);
    }

    [Fact]
    public void ReadVarInt_EmptyBuffer_ThrowsException()
    {
        Assert.Throws<InvalidDataException>(() =>
            KafkaProtocolPrimitives.ReadVarInt(ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void ReadVarInt_IncompleteData_ThrowsException()
    {
        // Buffer indicates more bytes needed but truncated
        var buffer = new byte[] { 0x80 }; // Continuation bit set but no more data
        Assert.Throws<InvalidDataException>(() =>
            KafkaProtocolPrimitives.ReadVarInt(buffer));
    }

    [Fact]
    public void VarInt_AllSingleByteValues_RoundTrip()
    {
        var buffer = new byte[5];
        for (int i = 0; i < 128; i++)
        {
            var bytesWritten = KafkaProtocolPrimitives.WriteVarInt(buffer, i);
            Assert.Equal(1, bytesWritten);

            var (decoded, bytesRead) = KafkaProtocolPrimitives.ReadVarInt(buffer);
            Assert.Equal(i, decoded);
            Assert.Equal(1, bytesRead);
        }
    }

    [Fact]
    public void VarInt_BoundaryValues_RoundTrip()
    {
        // Test values at encoding boundaries
        var boundaryValues = new[]
        {
            0, 127, 128,           // 1-2 byte boundary
            16383, 16384,          // 2-3 byte boundary
            2097151, 2097152,      // 3-4 byte boundary
            268435455, 268435456,  // 4-5 byte boundary
        };

        var buffer = new byte[5];
        foreach (var value in boundaryValues)
        {
            var bytesWritten = KafkaProtocolPrimitives.WriteVarInt(buffer, value);
            var (decoded, bytesRead) = KafkaProtocolPrimitives.ReadVarInt(buffer);

            Assert.Equal(value, decoded);
            Assert.Equal(bytesWritten, bytesRead);
        }
    }
}
