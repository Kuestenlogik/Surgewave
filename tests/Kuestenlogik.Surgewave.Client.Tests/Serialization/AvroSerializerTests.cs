using Kuestenlogik.Surgewave.Schema.Registry.Client;
using Kuestenlogik.Surgewave.Schema.Registry.Serdes.Avro;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests.Serialization;

/// <summary>
/// Tests for Avro schema registry serializers.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class AvroSerializerTests
{
    private readonly MockSchemaRegistry _schemaRegistry = new();

    public record TestRecord(string Name, int Value);

    #region Serializer Tests

    [Fact]
    public async Task SerializeAsync_WithAutoRegister_RegistersSchemaAndSerializes()
    {
        // Arrange
        var config = new AvroSerializerConfig(_schemaRegistry)
        {
            AutoRegisterSchemas = true
        };
        var serializer = new SchemaRegistryAvroSerializer<TestRecord>(config);
        var record = new TestRecord("test", 42);

        // Act
        var result = await serializer.SerializeAsync(record, "test-topic");

        // Assert
        Assert.NotNull(result);
        Assert.True(result.Length > SchemaRegistryWireFormat.HeaderSize);

        // Verify wire format header
        Assert.Equal(SchemaRegistryWireFormat.MagicByte, result[0]);

        // Verify schema ID is in the header (big-endian)
        var schemaId = SchemaRegistryWireFormat.ReadSchemaId(result);
        Assert.True(schemaId > 0);
    }

    [Fact]
    public async Task SerializeAsync_NullValue_ReturnsNull()
    {
        // Arrange
        var config = new AvroSerializerConfig(_schemaRegistry);
        var serializer = new SchemaRegistryAvroSerializer<TestRecord>(config);

        // Act
        var result = await serializer.SerializeAsync(null, "test-topic");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task SerializeAsync_WithCustomSerializer_UsesCustomSerializer()
    {
        // Arrange
        var customPayload = new byte[] { 1, 2, 3, 4, 5 };
        var config = new AvroSerializerConfig(_schemaRegistry);
        var serializer = new SchemaRegistryAvroSerializer<TestRecord>(config, _ => customPayload);
        var record = new TestRecord("test", 42);

        // Act
        var result = await serializer.SerializeAsync(record, "test-topic");

        // Assert
        Assert.NotNull(result);
        Assert.Equal(SchemaRegistryWireFormat.HeaderSize + customPayload.Length, result.Length);

        // Verify payload
        var payload = result.AsSpan(SchemaRegistryWireFormat.HeaderSize);
        Assert.True(payload.SequenceEqual(customPayload));
    }

    [Fact]
    public async Task SerializeAsync_CachesSchemaId()
    {
        // Arrange
        var config = new AvroSerializerConfig(_schemaRegistry);
        var serializer = new SchemaRegistryAvroSerializer<TestRecord>(config);
        var record = new TestRecord("test", 42);

        // Act
        var result1 = await serializer.SerializeAsync(record, "test-topic");
        var result2 = await serializer.SerializeAsync(record, "test-topic");

        // Assert - both should have the same schema ID
        Assert.NotNull(result1);
        Assert.NotNull(result2);
        var schemaId1 = SchemaRegistryWireFormat.ReadSchemaId(result1);
        var schemaId2 = SchemaRegistryWireFormat.ReadSchemaId(result2);
        Assert.Equal(schemaId1, schemaId2);
    }

    [Fact]
    public async Task SerializeAsync_AsKeySerializer_UsesKeySubject()
    {
        // Arrange
        var config = new AvroSerializerConfig(_schemaRegistry)
        {
            IsKey = true
        };
        var serializer = new SchemaRegistryAvroSerializer<TestRecord>(config);
        var record = new TestRecord("test", 42);

        // Act
        var result = await serializer.SerializeAsync(record, "my-topic");

        // Assert
        Assert.NotNull(result);
        // With TopicNameStrategy, key subject should be "my-topic-key"
    }

    #endregion

    #region Deserializer Tests

    [Fact]
    public async Task DeserializeAsync_ValidData_ReturnsDeserializedRecord()
    {
        // Arrange
        _schemaRegistry.PreRegisterSchema(1, "test-topic-value", "{}", "AVRO");
        var config = new AvroSerializerConfig(_schemaRegistry);
        var deserializer = new SchemaRegistryAvroDeserializer<TestRecord>(config);

        // Create valid wire format data with JSON payload
        var json = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new TestRecord("test", 123));
        var data = new byte[SchemaRegistryWireFormat.HeaderSize + json.Length];
        SchemaRegistryWireFormat.WriteHeader(data, 1);
        json.CopyTo(data.AsSpan(SchemaRegistryWireFormat.HeaderSize));

        // Act
        var result = await deserializer.DeserializeAsync(data, "test-topic");

        // Assert
        Assert.Equal("test", result.Name);
        Assert.Equal(123, result.Value);
    }

    [Fact]
    public async Task DeserializeAsync_DataTooShort_ThrowsArgumentException()
    {
        // Arrange
        _schemaRegistry.PreRegisterSchema(1, "test-topic-value", "{}", "AVRO");
        var config = new AvroSerializerConfig(_schemaRegistry);
        var deserializer = new SchemaRegistryAvroDeserializer<TestRecord>(config);
        var shortData = new byte[] { 0x00, 0x01, 0x02 }; // Less than 5 bytes

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() =>
            deserializer.DeserializeAsync(shortData, "test-topic").AsTask());
    }

    [Fact]
    public async Task DeserializeAsync_UnknownSchemaId_ThrowsInvalidOperationException()
    {
        // Arrange - don't pre-register schema
        var config = new AvroSerializerConfig(_schemaRegistry);
        var deserializer = new SchemaRegistryAvroDeserializer<TestRecord>(config);

        var data = new byte[SchemaRegistryWireFormat.HeaderSize + 10];
        SchemaRegistryWireFormat.WriteHeader(data, 999); // Unknown schema ID

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            deserializer.DeserializeAsync(data, "test-topic").AsTask());
    }

    [Fact]
    public async Task DeserializeAsync_WithCustomDeserializer_UsesCustomDeserializer()
    {
        // Arrange
        _schemaRegistry.PreRegisterSchema(1, "test-topic-value", "{}", "AVRO");
        var config = new AvroSerializerConfig(_schemaRegistry);
        var expectedRecord = new TestRecord("custom", 999);
        var deserializer = new SchemaRegistryAvroDeserializer<TestRecord>(config, _ => expectedRecord);

        var data = new byte[SchemaRegistryWireFormat.HeaderSize + 5];
        SchemaRegistryWireFormat.WriteHeader(data, 1);

        // Act
        var result = await deserializer.DeserializeAsync(data, "test-topic");

        // Assert
        Assert.Equal(expectedRecord, result);
    }

    #endregion

    #region Roundtrip Tests

    [Fact]
    public async Task Roundtrip_SerializeDeserialize_PreservesData()
    {
        // Arrange
        var config = new AvroSerializerConfig(_schemaRegistry);
        var serializer = new SchemaRegistryAvroSerializer<TestRecord>(config);
        var deserializer = new SchemaRegistryAvroDeserializer<TestRecord>(config);
        var original = new TestRecord("roundtrip-test", 12345);

        // Act
        var serialized = await serializer.SerializeAsync(original, "test-topic");
        Assert.NotNull(serialized);
        var deserialized = await deserializer.DeserializeAsync(serialized, "test-topic");

        // Assert
        Assert.Equal(original.Name, deserialized.Name);
        Assert.Equal(original.Value, deserialized.Value);
    }

    #endregion
}
