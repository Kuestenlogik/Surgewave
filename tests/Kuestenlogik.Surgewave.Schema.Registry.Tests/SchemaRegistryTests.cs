using Kuestenlogik.Surgewave.Schema.Registry;
using Kuestenlogik.Surgewave.Schema.Registry.Handlers;
using Microsoft.Extensions.Logging;
using Xunit;
using RegistrySchema = Kuestenlogik.Surgewave.Schema.Registry.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Tests;

/// <summary>
/// Tests for the Surgewave Schema Registry.
/// </summary>
public sealed class SchemaRegistryTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;
    private readonly SchemaStore _store;
    private readonly CompatibilityChecker _checker;

    // Sample Avro schemas
    private const string AvroUserSchemaV1 = """
        {
            "type": "record",
            "name": "User",
            "namespace": "com.example",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        }
        """;

    private const string AvroUserSchemaV2 = """
        {
            "type": "record",
            "name": "User",
            "namespace": "com.example",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": null}
            ]
        }
        """;

    private const string AvroUserSchemaIncompatible = """
        {
            "type": "record",
            "name": "User",
            "namespace": "com.example",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"}
            ]
        }
        """;

    // Sample JSON Schemas
    private const string JsonUserSchemaV1 = """
        {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            },
            "required": ["id", "name"]
        }
        """;

    private const string JsonUserSchemaV2 = """
        {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string"}
            },
            "required": ["id", "name"]
        }
        """;

    // Sample Protobuf schema
    private const string ProtobufUserSchemaV1 = """
        syntax = "proto3";
        message User {
            int64 id = 1;
            string name = 2;
        }
        """;

    private const string ProtobufUserSchemaV2 = """
        syntax = "proto3";
        message User {
            int64 id = 1;
            string name = 2;
            string email = 3;
        }
        """;

    public SchemaRegistryTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddFilter(level => level >= LogLevel.Debug);
        });

        _store = new SchemaStore(_loggerFactory.CreateLogger<SchemaStore>());

        // Create handler registry with all built-in handlers
        ISchemaTypeHandler[] handlers =
        [
            new AvroSchemaHandler(),
            new JsonSchemaHandler(),
            new ProtobufSchemaHandler()
        ];
        var handlerRegistry = new SchemaTypeHandlerRegistry(handlers);

        _checker = new CompatibilityChecker(_loggerFactory.CreateLogger<CompatibilityChecker>(), handlerRegistry);
    }

    public void Dispose()
    {
        _store.Dispose();
        _loggerFactory.Dispose();
    }

    [Fact]
    public void SchemaStore_RegistersAvroSchema()
    {
        // Act
        var schema = _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);

        // Assert
        Assert.Equal(1, schema.Id);
        Assert.Equal("user-value", schema.Subject);
        Assert.Equal(1, schema.Version);
        Assert.Equal(SchemaType.Avro, schema.SchemaType);
    }

    [Fact]
    public void SchemaStore_RegistersSameSchemaWithSameId()
    {
        // Act
        var schema1 = _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);
        var schema2 = _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);

        // Assert - Same schema should return same ID
        Assert.Equal(schema1.Id, schema2.Id);
        Assert.Equal(schema1.Version, schema2.Version);
    }

    [Fact]
    public void SchemaStore_RegistersNewVersionForNewSchema()
    {
        // Act
        var schema1 = _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);
        var schema2 = _store.RegisterSchema("user-value", AvroUserSchemaV2, SchemaType.Avro);

        // Assert
        Assert.Equal(1, schema1.Version);
        Assert.Equal(2, schema2.Version);
        Assert.NotEqual(schema1.Id, schema2.Id);
    }

    [Fact]
    public void SchemaStore_GetsSubjects()
    {
        // Arrange
        _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);
        _store.RegisterSchema("product-value", AvroUserSchemaV1, SchemaType.Avro);

        // Act
        var subjects = _store.GetSubjects();

        // Assert
        Assert.Equal(2, subjects.Count);
        Assert.Contains("user-value", subjects);
        Assert.Contains("product-value", subjects);
    }

    [Fact]
    public void SchemaStore_GetsVersions()
    {
        // Arrange
        _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);
        _store.RegisterSchema("user-value", AvroUserSchemaV2, SchemaType.Avro);

        // Act
        var versions = _store.GetVersions("user-value");

        // Assert
        Assert.Equal(2, versions.Count);
        Assert.Contains(1, versions);
        Assert.Contains(2, versions);
    }

    [Fact]
    public void SchemaStore_GetsSchemaByVersion()
    {
        // Arrange
        _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);
        _store.RegisterSchema("user-value", AvroUserSchemaV2, SchemaType.Avro);

        // Act
        var schema = _store.GetSchema("user-value", 1);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal(1, schema.Version);
        Assert.Contains("User", schema.SchemaString);
    }

    [Fact]
    public void SchemaStore_GetsLatestSchema()
    {
        // Arrange
        _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);
        _store.RegisterSchema("user-value", AvroUserSchemaV2, SchemaType.Avro);

        // Act
        var schema = _store.GetLatestSchema("user-value");

        // Assert
        Assert.NotNull(schema);
        Assert.Equal(2, schema.Version);
        Assert.Contains("email", schema.SchemaString);
    }

    [Fact]
    public void SchemaStore_GetsSchemaById()
    {
        // Arrange
        var registered = _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);

        // Act
        var schema = _store.GetSchemaById(registered.Id);

        // Assert
        Assert.NotNull(schema);
        Assert.Equal(registered.Id, schema.Id);
    }

    [Fact]
    public void SchemaStore_DeletesSubject()
    {
        // Arrange
        _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);
        _store.RegisterSchema("user-value", AvroUserSchemaV2, SchemaType.Avro);

        // Act
        var deleted = _store.DeleteSubject("user-value");

        // Assert
        Assert.Equal(2, deleted.Count);
        Assert.DoesNotContain("user-value", _store.GetSubjects());
    }

    [Fact]
    public void SchemaStore_DeletesVersion()
    {
        // Arrange
        _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);
        _store.RegisterSchema("user-value", AvroUserSchemaV2, SchemaType.Avro);

        // Act
        var deleted = _store.DeleteVersion("user-value", 1, permanent: true);

        // Assert
        Assert.Equal(1, deleted);
        var versions = _store.GetVersions("user-value");
        Assert.Single(versions);
        Assert.DoesNotContain(1, versions);
    }

    [Fact]
    public void SchemaStore_LooksUpSchemaId()
    {
        // Arrange
        var registered = _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);

        // Act
        var foundId = _store.LookupSchemaId("user-value", AvroUserSchemaV1, SchemaType.Avro);

        // Assert
        Assert.Equal(registered.Id, foundId);
    }

    [Fact]
    public void SchemaStore_SetsAndGetsCompatibility()
    {
        // Arrange
        _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);

        // Act
        _store.SetCompatibility("user-value", CompatibilityMode.Full);
        var mode = _store.GetCompatibility("user-value");

        // Assert
        Assert.Equal(CompatibilityMode.Full, mode);
    }

    [Fact]
    public void CompatibilityChecker_ValidatesAvroSchema()
    {
        // Act
        var (isValid, error) = _checker.ValidateSchema(AvroUserSchemaV1, SchemaType.Avro);

        // Assert
        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void CompatibilityChecker_RejectsInvalidAvroSchema()
    {
        // Arrange
        var invalidSchema = "{ invalid json }";

        // Act
        var (isValid, error) = _checker.ValidateSchema(invalidSchema, SchemaType.Avro);

        // Assert
        Assert.False(isValid);
        Assert.NotNull(error);
    }

    [Fact]
    public void CompatibilityChecker_ValidatesJsonSchema()
    {
        // Act
        var (isValid, error) = _checker.ValidateSchema(JsonUserSchemaV1, SchemaType.Json);

        // Assert
        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void CompatibilityChecker_ValidatesProtobufSchema()
    {
        // Act
        var (isValid, error) = _checker.ValidateSchema(ProtobufUserSchemaV1, SchemaType.Protobuf);

        // Assert
        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void CompatibilityChecker_AvroBackwardCompatible_WithOptionalField()
    {
        // Arrange
        var existingSchemas = new[]
        {
            new RegistrySchema
            {
                Id = 1,
                Subject = "user-value",
                Version = 1,
                SchemaType = SchemaType.Avro,
                SchemaString = AvroUserSchemaV1
            }
        };

        // Act - V2 adds optional field, should be backward compatible
        var result = _checker.CheckCompatibility(
            AvroUserSchemaV2,
            SchemaType.Avro,
            existingSchemas,
            CompatibilityMode.Backward);

        // Assert
        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void CompatibilityChecker_AvroNotBackwardCompatible_WithTypeChange()
    {
        // Arrange
        var existingSchemas = new[]
        {
            new RegistrySchema
            {
                Id = 1,
                Subject = "user-value",
                Version = 1,
                SchemaType = SchemaType.Avro,
                SchemaString = AvroUserSchemaV1
            }
        };

        // Act - Incompatible changes id type from long to string
        var result = _checker.CheckCompatibility(
            AvroUserSchemaIncompatible,
            SchemaType.Avro,
            existingSchemas,
            CompatibilityMode.Backward);

        // Assert
        Assert.False(result.IsCompatible);
        Assert.NotNull(result.Messages);
        _output.WriteLine(string.Join("\n", result.Messages));
    }

    [Fact]
    public void CompatibilityChecker_NoneMode_AlwaysCompatible()
    {
        // Arrange
        var existingSchemas = new[]
        {
            new RegistrySchema
            {
                Id = 1,
                Subject = "user-value",
                Version = 1,
                SchemaType = SchemaType.Avro,
                SchemaString = AvroUserSchemaV1
            }
        };

        // Act
        var result = _checker.CheckCompatibility(
            AvroUserSchemaIncompatible,
            SchemaType.Avro,
            existingSchemas,
            CompatibilityMode.None);

        // Assert
        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void CompatibilityChecker_JsonBackwardCompatible_WithOptionalField()
    {
        // Arrange
        var existingSchemas = new[]
        {
            new RegistrySchema
            {
                Id = 1,
                Subject = "user-value",
                Version = 1,
                SchemaType = SchemaType.Json,
                SchemaString = JsonUserSchemaV1
            }
        };

        // Act
        var result = _checker.CheckCompatibility(
            JsonUserSchemaV2,
            SchemaType.Json,
            existingSchemas,
            CompatibilityMode.Backward);

        // Assert
        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void CompatibilityChecker_ProtobufBackwardCompatible_WithNewField()
    {
        // Arrange
        var existingSchemas = new[]
        {
            new RegistrySchema
            {
                Id = 1,
                Subject = "user-value",
                Version = 1,
                SchemaType = SchemaType.Protobuf,
                SchemaString = ProtobufUserSchemaV1
            }
        };

        // Act
        var result = _checker.CheckCompatibility(
            ProtobufUserSchemaV2,
            SchemaType.Protobuf,
            existingSchemas,
            CompatibilityMode.Backward);

        // Assert
        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void SchemaStore_RegistersJsonSchema()
    {
        // Act
        var schema = _store.RegisterSchema("user-value", JsonUserSchemaV1, SchemaType.Json);

        // Assert
        Assert.Equal(1, schema.Id);
        Assert.Equal(SchemaType.Json, schema.SchemaType);
    }

    [Fact]
    public void SchemaStore_RegistersProtobufSchema()
    {
        // Act
        var schema = _store.RegisterSchema("user-value", ProtobufUserSchemaV1, SchemaType.Protobuf);

        // Assert
        Assert.Equal(1, schema.Id);
        Assert.Equal(SchemaType.Protobuf, schema.SchemaType);
    }

    [Fact]
    public void SchemaStore_GlobalCompatibilityDefault()
    {
        // Assert - Default should be Backward
        Assert.Equal(CompatibilityMode.Backward, _store.GlobalCompatibility);
    }

    [Fact]
    public void SchemaStore_SetsGlobalCompatibility()
    {
        // Act
        _store.GlobalCompatibility = CompatibilityMode.Full;

        // Assert
        Assert.Equal(CompatibilityMode.Full, _store.GlobalCompatibility);
    }

    [Fact]
    public void SchemaStore_SubjectInheritsGlobalCompatibility()
    {
        // Arrange - set global before any registrations
        _store.GlobalCompatibility = CompatibilityMode.Forward;

        // Act - get compatibility for non-existent subject (falls through to global)
        var mode = _store.GetCompatibility("non-existent-subject");

        // Assert - subjects without explicit config inherit global
        Assert.Equal(CompatibilityMode.Forward, mode);
    }

    [Fact]
    public void SchemaStore_RegistersSameSchemaUnderDifferentSubjects()
    {
        // Act
        var schema1 = _store.RegisterSchema("user-value", AvroUserSchemaV1, SchemaType.Avro);
        var schema2 = _store.RegisterSchema("product-value", AvroUserSchemaV1, SchemaType.Avro);

        // Assert - Same schema ID but different subjects
        Assert.Equal(schema1.Id, schema2.Id);
        Assert.Equal("user-value", schema1.Subject);
        Assert.Equal("product-value", schema2.Subject);
    }
}
