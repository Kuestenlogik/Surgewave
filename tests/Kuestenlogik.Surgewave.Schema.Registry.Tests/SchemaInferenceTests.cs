using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Schema.Registry.Inference;
using Xunit;

namespace Kuestenlogik.Surgewave.Schema.Registry.Tests;

/// <summary>
/// Tests for the live schema inference engine.
/// </summary>
public sealed class SchemaInferenceTests
{
    private readonly ITestOutputHelper _output;
    private readonly SchemaInferenceEngine _engine = new();

    public SchemaInferenceTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void InferFromMessage_SimpleObject()
    {
        // Arrange
        var json = """{"id": 1, "name": "Alice", "active": true}"""u8;

        // Act
        var schema = _engine.InferFromMessage(json);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("object", schema.Type);
        Assert.Equal(3, schema.Properties.Count);
        Assert.Equal(1, schema.SampleCount);

        Assert.Equal("integer", schema.Properties["id"].Type);
        Assert.Equal("string", schema.Properties["name"].Type);
        Assert.Equal("boolean", schema.Properties["active"].Type);

        Assert.Contains("id", schema.Required);
        Assert.Contains("name", schema.Required);
        Assert.Contains("active", schema.Required);

        var schemaString = SchemaInferenceEngine.ToJsonSchemaString(schema);
        _output.WriteLine(schemaString);
    }

    [Fact]
    public void InferFromMessage_NestedObject()
    {
        // Arrange
        var json = """
            {
                "user": {
                    "id": 42,
                    "address": {
                        "city": "Berlin",
                        "zip": "10115"
                    }
                },
                "score": 95.5
            }
            """u8;

        // Act
        var schema = _engine.InferFromMessage(json);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("object", schema.Type);
        Assert.Equal(2, schema.Properties.Count);

        var userProp = schema.Properties["user"];
        Assert.Equal("object", userProp.Type);
        Assert.NotNull(userProp.ObjectSchema);
        Assert.Equal(2, userProp.ObjectSchema.Properties.Count);

        var addressProp = userProp.ObjectSchema.Properties["address"];
        Assert.Equal("object", addressProp.Type);
        Assert.NotNull(addressProp.ObjectSchema);
        Assert.Contains("city", addressProp.ObjectSchema.Properties.Keys);
        Assert.Contains("zip", addressProp.ObjectSchema.Properties.Keys);

        Assert.Equal("number", schema.Properties["score"].Type);

        var schemaString = SchemaInferenceEngine.ToJsonSchemaString(schema);
        _output.WriteLine(schemaString);
    }

    [Fact]
    public void InferFromMessage_ArrayField()
    {
        // Arrange
        var json = """{"tags": ["important", "urgent"], "scores": [1, 2, 3]}"""u8;

        // Act
        var schema = _engine.InferFromMessage(json);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("array", schema.Properties["tags"].Type);
        Assert.NotNull(schema.Properties["tags"].Items);
        Assert.Equal("string", schema.Properties["tags"].Items!.Type);

        Assert.Equal("array", schema.Properties["scores"].Type);
        Assert.NotNull(schema.Properties["scores"].Items);
        Assert.Equal("integer", schema.Properties["scores"].Items!.Type);

        var schemaString = SchemaInferenceEngine.ToJsonSchemaString(schema);
        _output.WriteLine(schemaString);
    }

    [Fact]
    public void InferFromBatch_MergesFields()
    {
        // Arrange - "email" field is in message 1 but not message 2
        var messages = new List<ReadOnlyMemory<byte>>
        {
            Encoding.UTF8.GetBytes("""{"id": 1, "name": "Alice", "email": "alice@example.com"}"""),
            Encoding.UTF8.GetBytes("""{"id": 2, "name": "Bob"}""")
        };

        // Act
        var schema = _engine.InferFromBatch(messages);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal(2, schema.SampleCount);
        Assert.Equal(3, schema.Properties.Count);

        // id and name are required (in both messages)
        Assert.Contains("id", schema.Required);
        Assert.Contains("name", schema.Required);

        // email is optional (only in first message)
        Assert.DoesNotContain("email", schema.Required);

        var schemaString = SchemaInferenceEngine.ToJsonSchemaString(schema);
        _output.WriteLine(schemaString);
    }

    [Fact]
    public void InferFromBatch_WidensTypes()
    {
        // Arrange - "value" is integer in first, float in second
        var messages = new List<ReadOnlyMemory<byte>>
        {
            Encoding.UTF8.GetBytes("""{"value": 42}"""),
            Encoding.UTF8.GetBytes("""{"value": 3.14}""")
        };

        // Act
        var schema = _engine.InferFromBatch(messages);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("number", schema.Properties["value"].Type);

        var schemaString = SchemaInferenceEngine.ToJsonSchemaString(schema);
        _output.WriteLine(schemaString);
    }

    [Fact]
    public void InferFromBatch_DetectsDateTimeFormat()
    {
        // Arrange
        var messages = new List<ReadOnlyMemory<byte>>
        {
            Encoding.UTF8.GetBytes("""{"created_at": "2024-01-15T10:30:00Z"}"""),
            Encoding.UTF8.GetBytes("""{"created_at": "2024-02-20T14:45:00Z"}""")
        };

        // Act
        var schema = _engine.InferFromBatch(messages);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("string", schema.Properties["created_at"].Type);
        Assert.Equal("date-time", schema.Properties["created_at"].Format);

        var schemaString = SchemaInferenceEngine.ToJsonSchemaString(schema);
        _output.WriteLine(schemaString);
    }

    [Fact]
    public void InferFromBatch_DetectsEmailFormat()
    {
        // Arrange
        var messages = new List<ReadOnlyMemory<byte>>
        {
            Encoding.UTF8.GetBytes("""{"contact": "alice@example.com"}"""),
            Encoding.UTF8.GetBytes("""{"contact": "bob@company.org"}""")
        };

        // Act
        var schema = _engine.InferFromBatch(messages);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("string", schema.Properties["contact"].Type);
        Assert.Equal("email", schema.Properties["contact"].Format);
    }

    [Fact]
    public void MergeSchemas_CombinesRequired()
    {
        // Arrange
        var schema1 = _engine.InferFromMessage(
            Encoding.UTF8.GetBytes("""{"a": 1, "b": 2, "c": 3}"""));
        var schema2 = _engine.InferFromMessage(
            Encoding.UTF8.GetBytes("""{"a": 10, "b": 20, "d": 40}"""));

        Assert.NotNull(schema1);
        Assert.NotNull(schema2);

        // Act
        var merged = _engine.MergeSchemas(schema1, schema2);

        // Assert
        Assert.Equal(4, merged.Properties.Count); // a, b, c, d
        Assert.Contains("a", merged.Required);      // in both
        Assert.Contains("b", merged.Required);      // in both
        Assert.DoesNotContain("c", merged.Required); // only in schema1
        Assert.DoesNotContain("d", merged.Required); // only in schema2
    }

    [Fact]
    public void MergeSchemas_HandlesNullable()
    {
        // Arrange
        var messages = new List<ReadOnlyMemory<byte>>
        {
            Encoding.UTF8.GetBytes("""{"name": "Alice", "nickname": "Ali"}"""),
            Encoding.UTF8.GetBytes("""{"name": "Bob", "nickname": null}""")
        };

        // Act
        var schema = _engine.InferFromBatch(messages);

        // Assert
        Assert.NotNull(schema);
        Assert.False(schema.Properties["name"].Nullable);
        Assert.True(schema.Properties["nickname"].Nullable);

        var schemaString = SchemaInferenceEngine.ToJsonSchemaString(schema);
        _output.WriteLine(schemaString);
        // Verify the nullable field renders as ["string", "null"]
        Assert.Contains("null", schemaString);
    }

    [Fact]
    public void InferFromMessage_EmptyObject()
    {
        // Arrange
        var json = "{}"u8;

        // Act
        var schema = _engine.InferFromMessage(json);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("object", schema.Type);
        Assert.Empty(schema.Properties);
        Assert.Empty(schema.Required);
    }

    [Fact]
    public void InferFromMessage_InvalidJson_ReturnsNull()
    {
        // Arrange
        var invalid = "not valid json"u8;

        // Act
        var schema = _engine.InferFromMessage(invalid);

        // Assert
        Assert.Null(schema);
    }

    [Fact]
    public void InferFromMessage_EmptySpan_ReturnsNull()
    {
        // Act
        var schema = _engine.InferFromMessage(ReadOnlySpan<byte>.Empty);

        // Assert
        Assert.Null(schema);
    }

    [Fact]
    public void InferFromBatch_EmptyList_ReturnsNull()
    {
        // Act
        var schema = _engine.InferFromBatch([]);

        // Assert
        Assert.Null(schema);
    }

    [Fact]
    public void InferFromBatch_SkipsInvalidMessages()
    {
        // Arrange - mix of valid and invalid JSON
        var messages = new List<ReadOnlyMemory<byte>>
        {
            Encoding.UTF8.GetBytes("""{"id": 1}"""),
            Encoding.UTF8.GetBytes("not valid json"),
            Encoding.UTF8.GetBytes("""{"id": 2, "name": "Bob"}""")
        };

        // Act
        var schema = _engine.InferFromBatch(messages);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal(2, schema.SampleCount); // Only 2 valid messages
        Assert.Contains("id", schema.Properties.Keys);
    }

    [Fact]
    public void InferFromMessage_DetectsUuidFormat()
    {
        // Arrange
        var json = """{"id": "550e8400-e29b-41d4-a716-446655440000"}"""u8;

        // Act
        var schema = _engine.InferFromMessage(json);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("uuid", schema.Properties["id"].Format);
    }

    [Fact]
    public void InferFromMessage_DetectsUriFormat()
    {
        // Arrange
        var json = """{"url": "https://example.com/api/v1"}"""u8;

        // Act
        var schema = _engine.InferFromMessage(json);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("uri", schema.Properties["url"].Format);
    }

    [Fact]
    public void InferFromMessage_DetectsIpv4Format()
    {
        // Arrange
        var json = """{"ip": "192.168.1.100"}"""u8;

        // Act
        var schema = _engine.InferFromMessage(json);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("ipv4", schema.Properties["ip"].Format);
    }

    [Fact]
    public void InferFromMessage_DetectsIpv6Format()
    {
        // Arrange
        var json = """{"ip": "2001:0db8:85a3:0000:0000:8a2e:0370:7334"}"""u8;

        // Act
        var schema = _engine.InferFromMessage(json);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("ipv6", schema.Properties["ip"].Format);
    }

    [Fact]
    public void InferFromBatch_TypeConflict_WidensToString()
    {
        // Arrange - "value" is integer in first, string in second
        var messages = new List<ReadOnlyMemory<byte>>
        {
            Encoding.UTF8.GetBytes("""{"value": 42}"""),
            Encoding.UTF8.GetBytes("""{"value": "hello"}""")
        };

        // Act
        var schema = _engine.InferFromBatch(messages);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("string", schema.Properties["value"].Type);
    }

    [Fact]
    public void InferFromMessage_ArrayOfObjects()
    {
        // Arrange
        var json = """{"items": [{"name": "A", "qty": 1}, {"name": "B", "qty": 2}]}"""u8;

        // Act
        var schema = _engine.InferFromMessage(json);

        // Assert
        Assert.NotNull(schema);
        var itemsProp = schema.Properties["items"];
        Assert.Equal("array", itemsProp.Type);
        Assert.NotNull(itemsProp.Items);
        Assert.Equal("object", itemsProp.Items.Type);
        Assert.Contains("name", itemsProp.Items.Properties.Keys);
        Assert.Contains("qty", itemsProp.Items.Properties.Keys);
    }

    [Fact]
    public void InferFromMessage_LongNumber()
    {
        // Arrange - large number that exceeds int32 range
        var json = """{"bigId": 9999999999}"""u8;

        // Act
        var schema = _engine.InferFromMessage(json);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("integer", schema.Properties["bigId"].Type);
    }

    [Fact]
    public void ToJsonSchemaString_ProducesValidJsonSchema()
    {
        // Arrange
        var json = """{"id": 1, "name": "Test", "active": true, "tags": ["a", "b"]}"""u8;
        var schema = _engine.InferFromMessage(json);
        Assert.NotNull(schema);

        // Act
        var schemaString = SchemaInferenceEngine.ToJsonSchemaString(schema);

        // Assert
        var doc = JsonDocument.Parse(schemaString);
        var root = doc.RootElement;

        Assert.Equal("https://json-schema.org/draft/2020-12/schema", root.GetProperty("$schema").GetString());
        Assert.Equal("object", root.GetProperty("type").GetString());
        Assert.True(root.TryGetProperty("properties", out _));
        Assert.True(root.TryGetProperty("required", out _));

        _output.WriteLine(schemaString);
    }

    [Fact]
    public void InferFromBatch_LargeRealisticBatch()
    {
        // Arrange - simulate a realistic IoT event stream
        var messages = new List<ReadOnlyMemory<byte>>();
        for (int i = 0; i < 50; i++)
        {
            var json = $$"""
                {
                    "device_id": "{{Guid.NewGuid()}}",
                    "timestamp": "2024-03-{{(i % 28) + 1:D2}}T{{i % 24:D2}}:00:00Z",
                    "temperature": {{20 + (i % 15)}}.{{i % 10}},
                    "humidity": {{40 + i % 30}},
                    "battery_percent": {{100 - i % 20}},
                    "location": {
                        "lat": 52.520008,
                        "lon": 13.404954
                    },
                    "online": true
                }
                """;
            messages.Add(Encoding.UTF8.GetBytes(json));
        }

        // Act
        var schema = _engine.InferFromBatch(messages);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal(50, schema.SampleCount);
        Assert.Equal(7, schema.Properties.Count);

        Assert.Equal("uuid", schema.Properties["device_id"].Format);
        Assert.Equal("date-time", schema.Properties["timestamp"].Format);
        Assert.Equal("number", schema.Properties["temperature"].Type);
        Assert.Equal("integer", schema.Properties["humidity"].Type);
        Assert.Equal("boolean", schema.Properties["online"].Type);

        var locationSchema = schema.Properties["location"].ObjectSchema;
        Assert.NotNull(locationSchema);
        Assert.Contains("lat", locationSchema.Properties.Keys);
        Assert.Contains("lon", locationSchema.Properties.Keys);

        // All fields should be required since they're in all messages
        Assert.Equal(7, schema.Required.Count);

        var schemaString = SchemaInferenceEngine.ToJsonSchemaString(schema);
        _output.WriteLine(schemaString);
    }

    [Fact]
    public void InferFromBatch_MergesFormatConflicts()
    {
        // Arrange - first message has email, second has non-email string
        var messages = new List<ReadOnlyMemory<byte>>
        {
            Encoding.UTF8.GetBytes("""{"contact": "alice@example.com"}"""),
            Encoding.UTF8.GetBytes("""{"contact": "call 555-1234"}""")
        };

        // Act
        var schema = _engine.InferFromBatch(messages);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal("string", schema.Properties["contact"].Type);
        // Format should be null since they disagree
        Assert.Null(schema.Properties["contact"].Format);
    }

    [Fact]
    public void InferFromMessage_NullValue_MarksNullable()
    {
        // Arrange
        var json = """{"name": "Alice", "nickname": null}"""u8;

        // Act
        var schema = _engine.InferFromMessage(json);

        // Assert
        Assert.NotNull(schema);
        Assert.True(schema.Properties["nickname"].Nullable);
        Assert.False(schema.Properties["name"].Nullable);
    }
}
