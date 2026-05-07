using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for Kafka protocol primitives (VarInt, VarLong, ZigZag encoding, etc.)
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ProtocolPrimitivesTests
{
    #region VarInt Tests

    [Theory]
    [InlineData(0, new byte[] { 0x00 })]
    [InlineData(1, new byte[] { 0x01 })]
    [InlineData(127, new byte[] { 0x7F })]
    public void WriteVarInt_SingleByte_EncodesCorrectly(int value, byte[] expected)
    {
        // Arrange
        var buffer = new byte[5];

        // Act
        var bytesWritten = KafkaProtocolPrimitives.WriteVarInt(buffer, value);

        // Assert
        Assert.Equal(expected.Length, bytesWritten);
        Assert.Equal(expected, buffer[..bytesWritten]);
    }

    [Theory]
    [InlineData(128, new byte[] { 0x80, 0x01 })]
    [InlineData(255, new byte[] { 0xFF, 0x01 })]
    [InlineData(16383, new byte[] { 0xFF, 0x7F })]
    public void WriteVarInt_TwoBytes_EncodesCorrectly(int value, byte[] expected)
    {
        // Arrange
        var buffer = new byte[5];

        // Act
        var bytesWritten = KafkaProtocolPrimitives.WriteVarInt(buffer, value);

        // Assert
        Assert.Equal(expected.Length, bytesWritten);
        Assert.Equal(expected, buffer[..bytesWritten]);
    }

    [Theory]
    [InlineData(16384, new byte[] { 0x80, 0x80, 0x01 })]
    [InlineData(2097151, new byte[] { 0xFF, 0xFF, 0x7F })]
    public void WriteVarInt_ThreeBytes_EncodesCorrectly(int value, byte[] expected)
    {
        // Arrange
        var buffer = new byte[5];

        // Act
        var bytesWritten = KafkaProtocolPrimitives.WriteVarInt(buffer, value);

        // Assert
        Assert.Equal(expected.Length, bytesWritten);
        Assert.Equal(expected, buffer[..bytesWritten]);
    }

    [Theory]
    [InlineData(new byte[] { 0x00 }, 0, 1)]
    [InlineData(new byte[] { 0x01 }, 1, 1)]
    [InlineData(new byte[] { 0x7F }, 127, 1)]
    [InlineData(new byte[] { 0x80, 0x01 }, 128, 2)]
    [InlineData(new byte[] { 0xFF, 0x7F }, 16383, 2)]
    [InlineData(new byte[] { 0x80, 0x80, 0x01 }, 16384, 3)]
    public void ReadVarInt_DecodesCorrectly(byte[] buffer, int expectedValue, int expectedBytesRead)
    {
        // Act
        var (value, bytesRead) = KafkaProtocolPrimitives.ReadVarInt(buffer);

        // Assert
        Assert.Equal(expectedValue, value);
        Assert.Equal(expectedBytesRead, bytesRead);
    }

    [Fact]
    public void ReadVarInt_EmptyBuffer_ThrowsException()
    {
        // Act & Assert
        Assert.Throws<InvalidDataException>(() => KafkaProtocolPrimitives.ReadVarInt([]));
    }

    [Fact]
    public void VarInt_RoundTrip_PreservesValue()
    {
        // Arrange
        var values = new[] { 0, 1, 127, 128, 16383, 16384, 2097151, 2097152, int.MaxValue / 2 };

        foreach (var original in values)
        {
            var buffer = new byte[5];

            // Act
            var bytesWritten = KafkaProtocolPrimitives.WriteVarInt(buffer, original);
            var (decoded, bytesRead) = KafkaProtocolPrimitives.ReadVarInt(buffer);

            // Assert
            Assert.Equal(original, decoded);
            Assert.Equal(bytesWritten, bytesRead);
        }
    }

    #endregion

    #region VarLong Tests

    [Theory]
    [InlineData(0L, 1)]
    [InlineData(1L, 1)]
    [InlineData(127L, 1)]
    [InlineData(128L, 2)]
    [InlineData(16383L, 2)]
    [InlineData(16384L, 3)]
    public void WriteVarLong_EncodesWithCorrectByteCount(long value, int expectedBytes)
    {
        // Arrange
        var buffer = new byte[10];

        // Act
        var bytesWritten = KafkaProtocolPrimitives.WriteVarLong(buffer, value);

        // Assert
        Assert.Equal(expectedBytes, bytesWritten);
    }

    [Fact]
    public void VarLong_RoundTrip_PreservesValue()
    {
        // Arrange
        var values = new[] { 0L, 1L, 127L, 128L, 16383L, 16384L, 2097151L, long.MaxValue / 2 };

        foreach (var original in values)
        {
            var buffer = new byte[10];

            // Act
            var bytesWritten = KafkaProtocolPrimitives.WriteVarLong(buffer, original);
            var (decoded, bytesRead) = KafkaProtocolPrimitives.ReadVarLong(buffer);

            // Assert
            Assert.Equal(original, decoded);
            Assert.Equal(bytesWritten, bytesRead);
        }
    }

    [Fact]
    public void ReadVarLong_EmptyBuffer_ThrowsException()
    {
        // Act & Assert
        Assert.Throws<InvalidDataException>(() => KafkaProtocolPrimitives.ReadVarLong([]));
    }

    #endregion

    #region ZigZag Encoding Tests

    [Theory]
    [InlineData(0, 0u)]
    [InlineData(-1, 1u)]
    [InlineData(1, 2u)]
    [InlineData(-2, 3u)]
    [InlineData(2, 4u)]
    [InlineData(int.MaxValue, 4294967294u)]
    [InlineData(int.MinValue, 4294967295u)]
    public void ZigzagEncode_Int_EncodesCorrectly(int value, uint expected)
    {
        // Act
        var encoded = KafkaProtocolPrimitives.ZigzagEncode(value);

        // Assert
        Assert.Equal(expected, encoded);
    }

    [Theory]
    [InlineData(0u, 0)]
    [InlineData(1u, -1)]
    [InlineData(2u, 1)]
    [InlineData(3u, -2)]
    [InlineData(4u, 2)]
    public void ZigzagDecode_Int_DecodesCorrectly(uint encoded, int expected)
    {
        // Act
        var decoded = KafkaProtocolPrimitives.ZigzagDecode(encoded);

        // Assert
        Assert.Equal(expected, decoded);
    }

    [Fact]
    public void ZigzagEncode_Int_RoundTrip()
    {
        var values = new[] { 0, 1, -1, 100, -100, int.MaxValue, int.MinValue };

        foreach (var original in values)
        {
            var encoded = KafkaProtocolPrimitives.ZigzagEncode(original);
            var decoded = KafkaProtocolPrimitives.ZigzagDecode(encoded);
            Assert.Equal(original, decoded);
        }
    }

    [Theory]
    [InlineData(0L, 0UL)]
    [InlineData(-1L, 1UL)]
    [InlineData(1L, 2UL)]
    [InlineData(-2L, 3UL)]
    public void ZigzagEncode_Long_EncodesCorrectly(long value, ulong expected)
    {
        // Act
        var encoded = KafkaProtocolPrimitives.ZigzagEncode(value);

        // Assert
        Assert.Equal(expected, encoded);
    }

    [Fact]
    public void ZigzagEncode_Long_RoundTrip()
    {
        var values = new[] { 0L, 1L, -1L, 100L, -100L, long.MaxValue, long.MinValue };

        foreach (var original in values)
        {
            var encoded = KafkaProtocolPrimitives.ZigzagEncode(original);
            var decoded = KafkaProtocolPrimitives.ZigzagDecode(encoded);
            Assert.Equal(original, decoded);
        }
    }

    #endregion

    #region KafkaProtocolWriter Tests

    [Fact]
    public void Writer_WriteInt8_WritesCorrectly()
    {
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt8(42);
        Assert.Equal(new byte[] { 42 }, writer.ToArray());
    }

    [Fact]
    public void Writer_WriteInt16_WritesBigEndian()
    {
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(0x0102);
        Assert.Equal(new byte[] { 0x01, 0x02 }, writer.ToArray());
    }

    [Fact]
    public void Writer_WriteInt32_WritesBigEndian()
    {
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt32(0x01020304);
        Assert.Equal(new byte[] { 0x01, 0x02, 0x03, 0x04 }, writer.ToArray());
    }

    [Fact]
    public void Writer_WriteInt64_WritesBigEndian()
    {
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt64(0x0102030405060708L);
        Assert.Equal(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }, writer.ToArray());
    }

    [Fact]
    public void Writer_WriteBoolean_True()
    {
        using var writer = new KafkaProtocolWriter();
        writer.WriteBoolean(true);
        Assert.Equal(new byte[] { 1 }, writer.ToArray());
    }

    [Fact]
    public void Writer_WriteBoolean_False()
    {
        using var writer = new KafkaProtocolWriter();
        writer.WriteBoolean(false);
        Assert.Equal(new byte[] { 0 }, writer.ToArray());
    }

    [Fact]
    public void Writer_WriteString_WritesLengthPrefixed()
    {
        using var writer = new KafkaProtocolWriter();
        writer.WriteString("test");
        var result = writer.ToArray();
        Assert.Equal(6, result.Length); // 2 bytes length + 4 bytes "test"
        Assert.Equal(0, result[0]); // High byte of length
        Assert.Equal(4, result[1]); // Low byte of length
        Assert.Equal("test", System.Text.Encoding.UTF8.GetString(result, 2, 4));
    }

    [Fact]
    public void Writer_WriteString_Null_WritesMinusOne()
    {
        using var writer = new KafkaProtocolWriter();
        writer.WriteString(null);
        var result = writer.ToArray();
        Assert.Equal(2, result.Length);
        Assert.Equal(0xFF, result[0]); // -1 as Int16 high byte
        Assert.Equal(0xFF, result[1]); // -1 as Int16 low byte
    }

    [Fact]
    public void Writer_WriteCompactString_WritesVarIntPrefixed()
    {
        using var writer = new KafkaProtocolWriter();
        writer.WriteCompactString("test");
        var result = writer.ToArray();
        Assert.Equal(5, result.Length); // 1 byte VarInt (5) + 4 bytes "test"
        Assert.Equal(5, result[0]); // length + 1 for compact encoding
    }

    [Fact]
    public void Writer_WriteBytes_WritesLengthPrefixed()
    {
        using var writer = new KafkaProtocolWriter();
        writer.WriteBytes(new byte[] { 1, 2, 3 });
        var result = writer.ToArray();
        Assert.Equal(7, result.Length); // 4 bytes length + 3 bytes data
        Assert.Equal(0, result[0]);
        Assert.Equal(0, result[1]);
        Assert.Equal(0, result[2]);
        Assert.Equal(3, result[3]); // length
        Assert.Equal(new byte[] { 1, 2, 3 }, result[4..]);
    }

    [Fact]
    public void Writer_WriteBytes_Null_WritesMinusOne()
    {
        using var writer = new KafkaProtocolWriter();
        writer.WriteBytes((byte[]?)null);
        var result = writer.ToArray();
        Assert.Equal(4, result.Length);
        Assert.Equal(-1, System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(result));
    }

    [Fact]
    public void Writer_WriteUuid_WritesCorrectFormat()
    {
        using var writer = new KafkaProtocolWriter();
        var guid = new Guid("12345678-1234-1234-1234-123456789abc");
        writer.WriteUuid(guid);
        var result = writer.ToArray();
        Assert.Equal(16, result.Length);
    }

    [Fact]
    public void Writer_Position_TracksWrittenBytes()
    {
        using var writer = new KafkaProtocolWriter();
        Assert.Equal(0, writer.Position);
        writer.WriteInt32(1);
        Assert.Equal(4, writer.Position);
        writer.WriteInt16(1);
        Assert.Equal(6, writer.Position);
    }

    #endregion

    #region KafkaProtocolReader Tests

    [Fact]
    public void Reader_ReadInt8_ReadsCorrectly()
    {
        var reader = new KafkaProtocolReader([42]);
        Assert.Equal(42, reader.ReadInt8());
    }

    [Fact]
    public void Reader_ReadInt16_ReadsBigEndian()
    {
        var reader = new KafkaProtocolReader([0x01, 0x02]);
        Assert.Equal(0x0102, reader.ReadInt16());
    }

    [Fact]
    public void Reader_ReadInt32_ReadsBigEndian()
    {
        var reader = new KafkaProtocolReader([0x01, 0x02, 0x03, 0x04]);
        Assert.Equal(0x01020304, reader.ReadInt32());
    }

    [Fact]
    public void Reader_ReadInt64_ReadsBigEndian()
    {
        var reader = new KafkaProtocolReader([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
        Assert.Equal(0x0102030405060708L, reader.ReadInt64());
    }

    [Fact]
    public void Reader_ReadBoolean_True()
    {
        var reader = new KafkaProtocolReader([1]);
        Assert.True(reader.ReadBoolean());
    }

    [Fact]
    public void Reader_ReadBoolean_False()
    {
        var reader = new KafkaProtocolReader([0]);
        Assert.False(reader.ReadBoolean());
    }

    [Fact]
    public void Reader_ReadString_ReadsLengthPrefixed()
    {
        var reader = new KafkaProtocolReader([0x00, 0x04, (byte)'t', (byte)'e', (byte)'s', (byte)'t']);
        Assert.Equal("test", reader.ReadString());
    }

    [Fact]
    public void Reader_ReadString_Null_ReturnsNull()
    {
        var reader = new KafkaProtocolReader([0xFF, 0xFF]); // -1 as Int16
        Assert.Null(reader.ReadString());
    }

    [Fact]
    public void Reader_ReadCompactString_ReadsVarIntPrefixed()
    {
        var reader = new KafkaProtocolReader([0x05, (byte)'t', (byte)'e', (byte)'s', (byte)'t']);
        Assert.Equal("test", reader.ReadCompactString());
    }

    [Fact]
    public void Reader_ReadBytes_ReadsLengthPrefixed()
    {
        var reader = new KafkaProtocolReader([0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03]);
        Assert.Equal(new byte[] { 1, 2, 3 }, reader.ReadBytes());
    }

    [Fact]
    public void Reader_ReadBytes_Null_ReturnsNull()
    {
        var reader = new KafkaProtocolReader([0xFF, 0xFF, 0xFF, 0xFF]); // -1 as Int32
        Assert.Null(reader.ReadBytes());
    }

    [Fact]
    public void Reader_Skip_AdvancesPosition()
    {
        var reader = new KafkaProtocolReader([1, 2, 3, 4, 5]);
        Assert.Equal(0, reader.Position);
        reader.Skip(3);
        Assert.Equal(3, reader.Position);
        Assert.Equal(2, reader.Remaining);
    }

    [Fact]
    public void Reader_Skip_BeyondBuffer_ThrowsException()
    {
        var reader = new KafkaProtocolReader([1, 2, 3]);
        Assert.Throws<InvalidDataException>(() => reader.Skip(5));
    }

    [Fact]
    public void Reader_Position_TracksReadBytes()
    {
        var reader = new KafkaProtocolReader([0, 0, 0, 1, 0, 1]);
        Assert.Equal(0, reader.Position);
        Assert.Equal(6, reader.Remaining);
        reader.ReadInt32();
        Assert.Equal(4, reader.Position);
        Assert.Equal(2, reader.Remaining);
    }

    [Fact]
    public void Reader_ReadBeyondBuffer_ThrowsException()
    {
        var reader = new KafkaProtocolReader([1, 2]);
        Assert.Throws<InvalidDataException>(() => reader.ReadInt32());
    }

    #endregion

    #region Writer/Reader RoundTrip Tests

    [Fact]
    public void WriterReader_AllTypes_RoundTrip()
    {
        using var writer = new KafkaProtocolWriter();

        // Write various types
        writer.WriteInt8(42);
        writer.WriteInt16(1234);
        writer.WriteInt32(123456);
        writer.WriteInt64(1234567890123L);
        writer.WriteBoolean(true);
        writer.WriteString("hello");
        writer.WriteBytes(new byte[] { 1, 2, 3 });
        writer.WriteVarInt(16384);
        writer.WriteVarLong(1234567890L);

        // Read back
        var reader = new KafkaProtocolReader(writer.ToArray());
        Assert.Equal(42, reader.ReadInt8());
        Assert.Equal(1234, reader.ReadInt16());
        Assert.Equal(123456, reader.ReadInt32());
        Assert.Equal(1234567890123L, reader.ReadInt64());
        Assert.True(reader.ReadBoolean());
        Assert.Equal("hello", reader.ReadString());
        Assert.Equal(new byte[] { 1, 2, 3 }, reader.ReadBytes());
        Assert.Equal(16384, reader.ReadVarInt());
        Assert.Equal(1234567890L, reader.ReadVarLong());
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void WriterReader_Uuid_RoundTrip()
    {
        using var writer = new KafkaProtocolWriter();
        var original = Guid.NewGuid();
        writer.WriteUuid(original);

        var reader = new KafkaProtocolReader(writer.ToArray());
        var decoded = reader.ReadUuid();
        Assert.Equal(original, decoded);
    }

    [Fact]
    public void WriterReader_CompactTypes_RoundTrip()
    {
        using var writer = new KafkaProtocolWriter();

        writer.WriteCompactString("compact");
        writer.WriteCompactBytes(new byte[] { 4, 5, 6 });

        var reader = new KafkaProtocolReader(writer.ToArray());
        Assert.Equal("compact", reader.ReadCompactString());
        Assert.Equal(new byte[] { 4, 5, 6 }, reader.ReadCompactBytes());
    }

    #endregion

    #region ProtocolVersions Tests

    [Theory]
    [InlineData(ApiKey.Produce, 8, false)]
    [InlineData(ApiKey.Produce, 9, true)]
    [InlineData(ApiKey.Produce, 10, true)]
    [InlineData(ApiKey.Fetch, 11, false)]
    [InlineData(ApiKey.Fetch, 12, true)]
    [InlineData(ApiKey.Fetch, 13, true)]
    [InlineData(ApiKey.Metadata, 8, false)]
    [InlineData(ApiKey.Metadata, 9, true)]
    [InlineData(ApiKey.ApiVersions, 2, false)]
    [InlineData(ApiKey.ApiVersions, 3, true)]
    public void IsFlexible_ReturnsCorrectValue(ApiKey apiKey, short version, bool expected)
    {
        // Act
        var result = ProtocolVersions.IsFlexible(apiKey, version);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void IsFlexible_SaslHandshake_NeverFlexible()
    {
        for (short v = 0; v < 10; v++)
        {
            Assert.False(ProtocolVersions.IsFlexible(ApiKey.SaslHandshake, v));
        }
    }

    [Fact]
    public void IsFlexible_BrokerRegistration_AlwaysFlexible()
    {
        for (short v = 0; v < 5; v++)
        {
            Assert.True(ProtocolVersions.IsFlexible(ApiKey.BrokerRegistration, v));
        }
    }

    [Fact]
    public void IsFlexible_BrokerHeartbeat_AlwaysFlexible()
    {
        for (short v = 0; v < 5; v++)
        {
            Assert.True(ProtocolVersions.IsFlexible(ApiKey.BrokerHeartbeat, v));
        }
    }

    #endregion
}
