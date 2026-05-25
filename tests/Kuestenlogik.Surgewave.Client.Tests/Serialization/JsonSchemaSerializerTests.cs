using Kuestenlogik.Surgewave.Client.SchemaRegistry;
using Kuestenlogik.Surgewave.Client.SchemaRegistry.Json;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests.Serialization;

/// <summary>
/// Tests for JSON Schema registry serializers.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class JsonSchemaSerializerTests
{
    private readonly MockSchemaRegistry _schemaRegistry = new();

    public record TestMessage(string Name, int Count, bool Active);

    private const string TestJsonSchema = """
    {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "name": { "type": "string" },
            "count": { "type": "integer" },
            "active": { "type": "boolean" }
        },
        "required": ["name", "count", "active"]
    }
    """;

    #region Serializer Tests

    [Fact]
    public async Task SerializeAsync_WithAutoRegister_RegistersSchemaAndSerializes()
    {
        // Arrange
        var config = new JsonSchemaSerializerConfig(_schemaRegistry)
        {
            AutoRegisterSchemas = true
        };
        var serializer = new SchemaRegistryJsonSerializer<TestMessage>(config);
        var message = new TestMessage("test", 42, true);

        // Act
        var result = await serializer.SerializeAsync(message, "test-topic");

        // Assert
        Assert.NotNull(result);
        Assert.True(result.Length > SchemaRegistryWireFormat.HeaderSize);

        // Verify wire format header
        Assert.Equal(SchemaRegistryWireFormat.MagicByte, result[0]);

        // Verify schema ID is positive
        var schemaId = SchemaRegistryWireFormat.ReadSchemaId(result);
        Assert.True(schemaId > 0);

        // Verify JSON payload
        var payload = result.AsSpan(SchemaRegistryWireFormat.HeaderSize);
        var json = System.Text.Encoding.UTF8.GetString(payload);
        Assert.Contains("name", json);
        Assert.Contains("count", json);
    }

    [Fact]
    public async Task SerializeAsync_NullValue_ReturnsNull()
    {
        // Arrange
        var config = new JsonSchemaSerializerConfig(_schemaRegistry);
        var serializer = new SchemaRegistryJsonSerializer<TestMessage>(config);

        // Act
        var result = await serializer.SerializeAsync(null, "test-topic");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task SerializeAsync_UsesCamelCaseByDefault()
    {
        // Arrange
        var config = new JsonSchemaSerializerConfig(_schemaRegistry);
        var serializer = new SchemaRegistryJsonSerializer<TestMessage>(config);
        var message = new TestMessage("test", 42, true);

        // Act
        var result = await serializer.SerializeAsync(message, "test-topic");

        // Assert
        Assert.NotNull(result);
        var payload = result.AsSpan(SchemaRegistryWireFormat.HeaderSize);
        var json = System.Text.Encoding.UTF8.GetString(payload);

        // Verify camelCase property names
        Assert.Contains("\"name\"", json);
        Assert.Contains("\"count\"", json);
        Assert.Contains("\"active\"", json);
    }

    [Fact]
    public async Task SerializeAsync_CachesSchemaId()
    {
        // Arrange
        var config = new JsonSchemaSerializerConfig(_schemaRegistry);
        var serializer = new SchemaRegistryJsonSerializer<TestMessage>(config);
        var message = new TestMessage("test", 42, true);

        // Act
        var result1 = await serializer.SerializeAsync(message, "test-topic");
        var result2 = await serializer.SerializeAsync(message, "test-topic");

        // Assert - both should have the same schema ID
        Assert.NotNull(result1);
        Assert.NotNull(result2);
        var schemaId1 = SchemaRegistryWireFormat.ReadSchemaId(result1);
        var schemaId2 = SchemaRegistryWireFormat.ReadSchemaId(result2);
        Assert.Equal(schemaId1, schemaId2);
    }

    #endregion

    #region Deserializer Tests

    [Fact]
    public async Task DeserializeAsync_ValidData_ReturnsDeserializedMessage()
    {
        // Arrange
        _schemaRegistry.PreRegisterSchema(1, "test-topic-value", TestJsonSchema, "JSON");
        var config = new JsonSchemaSerializerConfig(_schemaRegistry)
        {
            ValidateOnDeserialize = false
        };
        var deserializer = new SchemaRegistryJsonDeserializer<TestMessage>(config);

        // Create valid wire format data with JSON payload
        var json = """{"name":"test","count":123,"active":true}"""u8.ToArray();
        var data = new byte[SchemaRegistryWireFormat.HeaderSize + json.Length];
        SchemaRegistryWireFormat.WriteHeader(data, 1);
        json.CopyTo(data.AsSpan(SchemaRegistryWireFormat.HeaderSize));

        // Act
        var result = await deserializer.DeserializeAsync(data, "test-topic");

        // Assert
        Assert.Equal("test", result.Name);
        Assert.Equal(123, result.Count);
        Assert.True(result.Active);
    }

    [Fact]
    public async Task DeserializeAsync_DataTooShort_ThrowsArgumentException()
    {
        // Arrange
        _schemaRegistry.PreRegisterSchema(1, "test-topic-value", TestJsonSchema, "JSON");
        var config = new JsonSchemaSerializerConfig(_schemaRegistry);
        var deserializer = new SchemaRegistryJsonDeserializer<TestMessage>(config);
        var shortData = new byte[] { 0x00, 0x01, 0x02 };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() =>
            deserializer.DeserializeAsync(shortData, "test-topic").AsTask());
    }

    [Fact]
    public async Task DeserializeAsync_UnknownSchemaId_ThrowsInvalidOperationException()
    {
        // Arrange - don't pre-register schema
        var config = new JsonSchemaSerializerConfig(_schemaRegistry);
        var deserializer = new SchemaRegistryJsonDeserializer<TestMessage>(config);

        var data = new byte[SchemaRegistryWireFormat.HeaderSize + 10];
        SchemaRegistryWireFormat.WriteHeader(data, 999);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            deserializer.DeserializeAsync(data, "test-topic").AsTask());
    }

    #endregion

    #region Roundtrip Tests

    [Fact]
    public async Task Roundtrip_SerializeDeserialize_PreservesData()
    {
        // Arrange
        var config = new JsonSchemaSerializerConfig(_schemaRegistry);
        var serializer = new SchemaRegistryJsonSerializer<TestMessage>(config);
        var deserializer = new SchemaRegistryJsonDeserializer<TestMessage>(config);
        var original = new TestMessage("roundtrip-test", 12345, false);

        // Act
        var serialized = await serializer.SerializeAsync(original, "test-topic");
        Assert.NotNull(serialized);
        var deserialized = await deserializer.DeserializeAsync(serialized, "test-topic");

        // Assert
        Assert.Equal(original.Name, deserialized.Name);
        Assert.Equal(original.Count, deserialized.Count);
        Assert.Equal(original.Active, deserialized.Active);
    }

    [Fact]
    public async Task Roundtrip_MultipleMessages_PreservesAllData()
    {
        // Arrange
        var config = new JsonSchemaSerializerConfig(_schemaRegistry);
        var serializer = new SchemaRegistryJsonSerializer<TestMessage>(config);
        var deserializer = new SchemaRegistryJsonDeserializer<TestMessage>(config);

        var messages = new[]
        {
            new TestMessage("first", 1, true),
            new TestMessage("second", 2, false),
            new TestMessage("third", 3, true)
        };

        // Act & Assert
        foreach (var original in messages)
        {
            var serialized = await serializer.SerializeAsync(original, "test-topic");
            Assert.NotNull(serialized);
            var deserialized = await deserializer.DeserializeAsync(serialized, "test-topic");

            Assert.Equal(original.Name, deserialized.Name);
            Assert.Equal(original.Count, deserialized.Count);
            Assert.Equal(original.Active, deserialized.Active);
        }
    }

    #endregion
}
