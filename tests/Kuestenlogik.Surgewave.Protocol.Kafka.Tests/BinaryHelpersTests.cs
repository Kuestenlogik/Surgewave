using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for BinaryHelpers and BinaryReaderExtensions utility methods.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class BinaryHelpersTests
{
    #region ReadInt16BigEndian Tests

    [Theory]
    [InlineData(0, new byte[] { 0x00, 0x00 })]
    [InlineData(1, new byte[] { 0x00, 0x01 })]
    [InlineData(256, new byte[] { 0x01, 0x00 })]
    [InlineData(0x0102, new byte[] { 0x01, 0x02 })]
    [InlineData(-1, new byte[] { 0xFF, 0xFF })]
    [InlineData(short.MaxValue, new byte[] { 0x7F, 0xFF })]
    public void ReadInt16BigEndian_ReadsCorrectly(short expected, byte[] bytes)
    {
        // Arrange
        using var ms = new MemoryStream(bytes);
        using var reader = new BinaryReader(ms);

        // Act
        var result = BinaryHelpers.ReadInt16BigEndian(reader);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ReadInt16BigEndian_InsufficientData_ThrowsInvalidDataException()
    {
        // Arrange
        using var ms = new MemoryStream(new byte[] { 0x01 }); // only 1 byte
        using var reader = new BinaryReader(ms);

        // Act & Assert
        Assert.Throws<InvalidDataException>(() => BinaryHelpers.ReadInt16BigEndian(reader));
    }

    #endregion

    #region ReadInt32BigEndian Tests

    [Theory]
    [InlineData(0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
    [InlineData(1, new byte[] { 0x00, 0x00, 0x00, 0x01 })]
    [InlineData(0x01020304, new byte[] { 0x01, 0x02, 0x03, 0x04 })]
    [InlineData(-1, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
    public void ReadInt32BigEndian_ReadsCorrectly(int expected, byte[] bytes)
    {
        // Arrange
        using var ms = new MemoryStream(bytes);
        using var reader = new BinaryReader(ms);

        // Act
        var result = BinaryHelpers.ReadInt32BigEndian(reader);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ReadInt32BigEndian_InsufficientData_ThrowsInvalidDataException()
    {
        // Arrange
        using var ms = new MemoryStream(new byte[] { 0x00, 0x00, 0x01 }); // only 3 bytes
        using var reader = new BinaryReader(ms);

        // Act & Assert
        Assert.Throws<InvalidDataException>(() => BinaryHelpers.ReadInt32BigEndian(reader));
    }

    #endregion

    #region ReadString Tests

    [Fact]
    public void ReadString_ReadsKafkaEncodedString()
    {
        // Arrange - "hello" encoded as 2-byte length + UTF-8 bytes
        var bytes = new byte[] { 0x00, 0x05, (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o' };
        using var ms = new MemoryStream(bytes);
        using var reader = new BinaryReader(ms);

        // Act
        var result = BinaryHelpers.ReadString(reader);

        // Assert
        Assert.Equal("hello", result);
    }

    [Fact]
    public void ReadString_NegativeLength_ReturnsEmptyString()
    {
        // Arrange - length = -1 means null/empty in Kafka protocol (ReadString returns empty)
        var bytes = new byte[] { 0xFF, 0xFF };
        using var ms = new MemoryStream(bytes);
        using var reader = new BinaryReader(ms);

        // Act
        var result = BinaryHelpers.ReadString(reader);

        // Assert
        Assert.Equal(string.Empty, result);
    }

    [Fact]
    public void ReadNullableString_NegativeLength_ReturnsNull()
    {
        // Arrange
        var bytes = new byte[] { 0xFF, 0xFF };
        using var ms = new MemoryStream(bytes);
        using var reader = new BinaryReader(ms);

        // Act
        var result = BinaryHelpers.ReadNullableString(reader);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void ReadNullableString_ValidString_ReturnsString()
    {
        // Arrange - "test"
        var bytes = new byte[] { 0x00, 0x04, (byte)'t', (byte)'e', (byte)'s', (byte)'t' };
        using var ms = new MemoryStream(bytes);
        using var reader = new BinaryReader(ms);

        // Act
        var result = BinaryHelpers.ReadNullableString(reader);

        // Assert
        Assert.Equal("test", result);
    }

    [Fact]
    public void ReadString_EmptyString_ZeroLength()
    {
        // Arrange - length = 0
        var bytes = new byte[] { 0x00, 0x00 };
        using var ms = new MemoryStream(bytes);
        using var reader = new BinaryReader(ms);

        // Act
        var result = BinaryHelpers.ReadString(reader);

        // Assert
        Assert.Equal(string.Empty, result);
    }

    #endregion

    #region WriteInt32BigEndian Tests

    [Theory]
    [InlineData(0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
    [InlineData(1, new byte[] { 0x00, 0x00, 0x00, 0x01 })]
    [InlineData(0x01020304, new byte[] { 0x01, 0x02, 0x03, 0x04 })]
    [InlineData(-1, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
    public void WriteInt32BigEndian_WritesCorrectly(int value, byte[] expected)
    {
        // Act
        var result = BinaryHelpers.WriteInt32BigEndian(value);

        // Assert
        Assert.Equal(expected, result);
    }

    #endregion

    #region Extension Method Tests

    [Fact]
    public void ExtensionMethod_ReadInt16BigEndian_Works()
    {
        // Arrange
        var bytes = new byte[] { 0x01, 0x00 };
        using var ms = new MemoryStream(bytes);
        using var reader = new BinaryReader(ms);

        // Act
        var result = reader.ReadInt16BigEndian();

        // Assert
        Assert.Equal(0x0100, result);
    }

    [Fact]
    public void ExtensionMethod_ReadInt32BigEndian_Works()
    {
        // Arrange
        var bytes = new byte[] { 0x00, 0x00, 0x01, 0x00 };
        using var ms = new MemoryStream(bytes);
        using var reader = new BinaryReader(ms);

        // Act
        var result = reader.ReadInt32BigEndian();

        // Assert
        Assert.Equal(0x100, result);
    }

    [Fact]
    public void ExtensionMethod_ReadKafkaString_Works()
    {
        // Arrange
        // Length prefix 0x0009 + 9 ASCII bytes for "Surgewave"
        var bytes = new byte[]
        {
            0x00, 0x09,
            (byte)'S', (byte)'u', (byte)'r', (byte)'g', (byte)'e',
            (byte)'w', (byte)'a', (byte)'v', (byte)'e',
        };
        using var ms = new MemoryStream(bytes);
        using var reader = new BinaryReader(ms);

        // Act
        var result = reader.ReadKafkaString();

        // Assert
        Assert.Equal("Surgewave", result);
    }

    [Fact]
    public void ExtensionMethod_ToBytesBigEndian_Works()
    {
        // Act
        var bytes = 0x01020304.ToBytesBigEndian();

        // Assert
        Assert.Equal(new byte[] { 0x01, 0x02, 0x03, 0x04 }, bytes);
    }

    #endregion
}
