using Kuestenlogik.Surgewave.Client.SchemaRegistry;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests.Serialization;

/// <summary>
/// Tests for the Schema Registry wire format utilities.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SchemaRegistryWireFormatTests
{
    #region Constants Tests

    [Fact]
    public void MagicByte_IsZero()
    {
        Assert.Equal(0x00, SchemaRegistryWireFormat.MagicByte);
    }

    [Fact]
    public void HeaderSize_IsFiveBytes()
    {
        Assert.Equal(5, SchemaRegistryWireFormat.HeaderSize);
    }

    #endregion

    #region WriteHeader Tests

    [Fact]
    public void WriteHeader_WritesCorrectMagicByte()
    {
        // Arrange
        var buffer = new byte[5];

        // Act
        SchemaRegistryWireFormat.WriteHeader(buffer, 1);

        // Assert
        Assert.Equal(0x00, buffer[0]);
    }

    [Theory]
    [InlineData(1, new byte[] { 0x00, 0x00, 0x00, 0x01 })]
    [InlineData(256, new byte[] { 0x00, 0x00, 0x01, 0x00 })]
    [InlineData(65536, new byte[] { 0x00, 0x01, 0x00, 0x00 })]
    [InlineData(16777216, new byte[] { 0x01, 0x00, 0x00, 0x00 })]
    [InlineData(0x12345678, new byte[] { 0x12, 0x34, 0x56, 0x78 })]
    public void WriteHeader_WritesSchemaIdAsBigEndian(int schemaId, byte[] expectedIdBytes)
    {
        // Arrange
        var buffer = new byte[5];

        // Act
        SchemaRegistryWireFormat.WriteHeader(buffer, schemaId);

        // Assert - verify big-endian schema ID bytes
        Assert.Equal(expectedIdBytes[0], buffer[1]);
        Assert.Equal(expectedIdBytes[1], buffer[2]);
        Assert.Equal(expectedIdBytes[2], buffer[3]);
        Assert.Equal(expectedIdBytes[3], buffer[4]);
    }

    #endregion

    #region ReadSchemaId Tests

    [Theory]
    [InlineData(1)]
    [InlineData(256)]
    [InlineData(65536)]
    [InlineData(16777216)]
    [InlineData(0x12345678)]
    [InlineData(int.MaxValue)]
    public void ReadSchemaId_ReadsCorrectly(int schemaId)
    {
        // Arrange
        var buffer = new byte[5];
        SchemaRegistryWireFormat.WriteHeader(buffer, schemaId);

        // Act
        var result = SchemaRegistryWireFormat.ReadSchemaId(buffer);

        // Assert
        Assert.Equal(schemaId, result);
    }

    [Fact]
    public void ReadSchemaId_BufferTooSmall_ThrowsArgumentException()
    {
        // Arrange
        var buffer = new byte[4]; // Too small

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() =>
            SchemaRegistryWireFormat.ReadSchemaId(buffer));
        Assert.Contains("Buffer too small", ex.Message);
    }

    [Fact]
    public void ReadSchemaId_InvalidMagicByte_ThrowsInvalidOperationException()
    {
        // Arrange
        var buffer = new byte[] { 0x01, 0x00, 0x00, 0x00, 0x01 }; // Wrong magic byte

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SchemaRegistryWireFormat.ReadSchemaId(buffer));
        Assert.Contains("Invalid magic byte", ex.Message);
    }

    #endregion

    #region GetPayload Tests

    [Fact]
    public void GetPayload_ReturnsPayloadAfterHeader()
    {
        // Arrange
        var buffer = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x01, 0xAA, 0xBB, 0xCC };

        // Act
        var payload = SchemaRegistryWireFormat.GetPayload(buffer);

        // Assert
        Assert.Equal(3, payload.Length);
        Assert.Equal(0xAA, payload[0]);
        Assert.Equal(0xBB, payload[1]);
        Assert.Equal(0xCC, payload[2]);
    }

    [Fact]
    public void GetPayload_EmptyPayload_ReturnsEmptySpan()
    {
        // Arrange
        var buffer = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x01 }; // Header only

        // Act
        var payload = SchemaRegistryWireFormat.GetPayload(buffer);

        // Assert
        Assert.Equal(0, payload.Length);
    }

    [Fact]
    public void GetPayload_BufferTooSmall_ThrowsArgumentException()
    {
        // Arrange
        var buffer = new byte[4]; // Too small

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() =>
            SchemaRegistryWireFormat.GetPayload(buffer));
        Assert.Contains("Buffer too small", ex.Message);
    }

    #endregion

    #region Roundtrip Tests

    [Theory]
    [InlineData(1)]
    [InlineData(42)]
    [InlineData(1000)]
    [InlineData(int.MaxValue)]
    public void Roundtrip_WriteAndRead_PreservesSchemaId(int schemaId)
    {
        // Arrange
        var buffer = new byte[5];

        // Act
        SchemaRegistryWireFormat.WriteHeader(buffer, schemaId);
        var result = SchemaRegistryWireFormat.ReadSchemaId(buffer);

        // Assert
        Assert.Equal(schemaId, result);
    }

    [Fact]
    public void Roundtrip_WriteHeaderAndGetPayload_WorksTogether()
    {
        // Arrange
        var payload = new byte[] { 1, 2, 3, 4, 5 };
        var buffer = new byte[SchemaRegistryWireFormat.HeaderSize + payload.Length];

        // Act
        SchemaRegistryWireFormat.WriteHeader(buffer, 123);
        payload.CopyTo(buffer.AsSpan(SchemaRegistryWireFormat.HeaderSize));

        var schemaId = SchemaRegistryWireFormat.ReadSchemaId(buffer);
        var extractedPayload = SchemaRegistryWireFormat.GetPayload(buffer);

        // Assert
        Assert.Equal(123, schemaId);
        Assert.Equal(5, extractedPayload.Length);
        Assert.True(extractedPayload.SequenceEqual(payload));
    }

    #endregion
}
