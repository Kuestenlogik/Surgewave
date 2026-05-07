using Kuestenlogik.Surgewave.Schema.Registry;
using Kuestenlogik.Surgewave.Schema.Registry.Handlers;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Schema.Registry.Tests.Handlers;

/// <summary>
/// Tests for all schema type handlers: Hyperion, MessagePack, CBOR, Bond, Thrift,
/// MemoryPack, Cap'n Proto, and Orleans.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SchemaHandlerTests
{
    #region Hyperion

    [Fact]
    public void Hyperion_TypeName_IsHYPERION()
    {
        var handler = new HyperionSchemaHandler();
        Assert.Equal("HYPERION", handler.TypeName);
    }

    [Fact]
    public void Hyperion_Validate_AcceptsAnySchema()
    {
        var handler = new HyperionSchemaHandler();

        var (isValid, error) = handler.Validate("MyApp.OrderEvent");

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void Hyperion_Validate_AcceptsEmptySchema()
    {
        var handler = new HyperionSchemaHandler();

        var (isValid, error) = handler.Validate("");

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void Hyperion_CheckCompatibility_AlwaysCompatible()
    {
        var handler = new HyperionSchemaHandler();
        var existing = CreateSchemaList("v1-schema");

        var result = handler.CheckCompatibility("v2-schema", existing, CompatibilityMode.Full);

        Assert.True(result.IsCompatible);
    }

    #endregion

    #region MessagePack

    [Fact]
    public void MessagePack_TypeName_IsMSGPACK()
    {
        var handler = new MessagePackSchemaHandler();
        Assert.Equal("MSGPACK", handler.TypeName);
    }

    [Fact]
    public void MessagePack_Validate_AcceptsAnySchema()
    {
        var handler = new MessagePackSchemaHandler();

        var (isValid, error) = handler.Validate("MyApp.OrderEvent");

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void MessagePack_Validate_AcceptsEmptySchema()
    {
        var handler = new MessagePackSchemaHandler();

        var (isValid, error) = handler.Validate("");

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void MessagePack_CheckCompatibility_AlwaysCompatible()
    {
        var handler = new MessagePackSchemaHandler();
        var existing = CreateSchemaList("v1-schema");

        var result = handler.CheckCompatibility("v2-schema", existing, CompatibilityMode.Full);

        Assert.True(result.IsCompatible);
    }

    #endregion

    #region CBOR

    [Fact]
    public void Cbor_TypeName_IsCBOR()
    {
        var handler = new CborSchemaHandler();
        Assert.Equal("CBOR", handler.TypeName);
    }

    [Fact]
    public void Cbor_Validate_AcceptsAnySchema()
    {
        var handler = new CborSchemaHandler();

        var (isValid, error) = handler.Validate("MyApp.SensorReading");

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void Cbor_Validate_AcceptsEmptySchema()
    {
        var handler = new CborSchemaHandler();

        var (isValid, error) = handler.Validate("");

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void Cbor_CheckCompatibility_AlwaysCompatible()
    {
        var handler = new CborSchemaHandler();
        var existing = CreateSchemaList("v1-schema");

        var result = handler.CheckCompatibility("v2-schema", existing, CompatibilityMode.BackwardTransitive);

        Assert.True(result.IsCompatible);
    }

    #endregion

    #region Bond

    [Fact]
    public void Bond_TypeName_IsBOND()
    {
        var handler = new BondSchemaHandler();
        Assert.Equal("BOND", handler.TypeName);
    }

    [Fact]
    public void Bond_Validate_ValidSchema_ReturnsTrue()
    {
        var handler = new BondSchemaHandler();
        const string schema = """
            struct Order {
                0: required int64 id;
                1: optional string name;
            }
            """;

        var (isValid, error) = handler.Validate(schema);

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void Bond_Validate_NoStructs_ReturnsFalse()
    {
        var handler = new BondSchemaHandler();

        var (isValid, error) = handler.Validate("not a valid bond schema");

        Assert.False(isValid);
        Assert.Contains("no struct definitions found", error);
    }

    [Fact]
    public void Bond_Validate_DuplicateOrdinals_ReturnsFalse()
    {
        var handler = new BondSchemaHandler();
        const string schema = """
            struct Order {
                0: required int64 id;
                0: optional string name;
            }
            """;

        var (isValid, error) = handler.Validate(schema);

        Assert.False(isValid);
        Assert.Contains("Duplicate field ordinals", error);
    }

    [Fact]
    public void Bond_CheckCompatibility_NoExistingSchemas_ReturnsCompatible()
    {
        var handler = new BondSchemaHandler();
        const string schema = """
            struct Order {
                0: required int64 id;
            }
            """;

        var result = handler.CheckCompatibility(schema, [], CompatibilityMode.Backward);

        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void Bond_CheckCompatibility_ModeNone_ReturnsCompatible()
    {
        var handler = new BondSchemaHandler();
        var existing = CreateSchemaListWithString("""
            struct Order {
                0: required int64 id;
            }
            """);

        var result = handler.CheckCompatibility("completely different", existing, CompatibilityMode.None);

        Assert.True(result.IsCompatible);
    }

    #endregion

    #region Thrift

    [Fact]
    public void Thrift_TypeName_IsTHRIFT()
    {
        var handler = new ThriftSchemaHandler();
        Assert.Equal("THRIFT", handler.TypeName);
    }

    [Fact]
    public void Thrift_Validate_ValidSchema_ReturnsTrue()
    {
        var handler = new ThriftSchemaHandler();
        const string schema = """
            struct Order {
                1: required i64 id;
                2: optional string name;
            }
            """;

        var (isValid, error) = handler.Validate(schema);

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void Thrift_Validate_NoStructs_ReturnsFalse()
    {
        var handler = new ThriftSchemaHandler();

        var (isValid, error) = handler.Validate("not a valid thrift schema");

        Assert.False(isValid);
        Assert.Contains("no struct definitions found", error);
    }

    [Fact]
    public void Thrift_Validate_DuplicateFieldIds_ReturnsFalse()
    {
        var handler = new ThriftSchemaHandler();
        const string schema = """
            struct Order {
                1: required i64 id;
                1: optional string name;
            }
            """;

        var (isValid, error) = handler.Validate(schema);

        Assert.False(isValid);
        Assert.Contains("Duplicate field IDs", error);
    }

    [Fact]
    public void Thrift_CheckCompatibility_NoExistingSchemas_ReturnsCompatible()
    {
        var handler = new ThriftSchemaHandler();
        const string schema = """
            struct Order {
                1: required i64 id;
            }
            """;

        var result = handler.CheckCompatibility(schema, [], CompatibilityMode.Backward);

        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void Thrift_CheckCompatibility_ModeNone_ReturnsCompatible()
    {
        var handler = new ThriftSchemaHandler();
        var existing = CreateSchemaListWithString("""
            struct Order {
                1: required i64 id;
            }
            """);

        var result = handler.CheckCompatibility("completely different", existing, CompatibilityMode.None);

        Assert.True(result.IsCompatible);
    }

    #endregion

    #region MemoryPack

    [Fact]
    public void MemoryPack_TypeName_IsMEMORYPACK()
    {
        var handler = new MemoryPackSchemaHandler();
        Assert.Equal("MEMORYPACK", handler.TypeName);
    }

    [Fact]
    public void MemoryPack_Validate_AcceptsAnySchema()
    {
        var handler = new MemoryPackSchemaHandler();

        var (isValid, error) = handler.Validate("MyApp.OrderEvent");

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void MemoryPack_Validate_AcceptsEmptySchema()
    {
        var handler = new MemoryPackSchemaHandler();

        var (isValid, error) = handler.Validate("");

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void MemoryPack_CheckCompatibility_AlwaysCompatible()
    {
        var handler = new MemoryPackSchemaHandler();
        var existing = CreateSchemaList("v1-schema");

        var result = handler.CheckCompatibility("v2-schema", existing, CompatibilityMode.FullTransitive);

        Assert.True(result.IsCompatible);
    }

    #endregion

    #region Cap'n Proto

    [Fact]
    public void CapnProto_TypeName_IsCAPNPROTO()
    {
        var handler = new CapnProtoSchemaHandler();
        Assert.Equal("CAPNPROTO", handler.TypeName);
    }

    [Fact]
    public void CapnProto_Validate_ValidSchema_ReturnsTrue()
    {
        var handler = new CapnProtoSchemaHandler();
        const string schema = """
            struct Order {
                id @0 :Int64;
                name @1 :Text;
            }
            """;

        var (isValid, error) = handler.Validate(schema);

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void CapnProto_Validate_NoStructs_ReturnsFalse()
    {
        var handler = new CapnProtoSchemaHandler();

        var (isValid, error) = handler.Validate("not a valid capnproto schema");

        Assert.False(isValid);
        Assert.Contains("no struct definitions found", error);
    }

    [Fact]
    public void CapnProto_Validate_DuplicateOrdinals_ReturnsFalse()
    {
        var handler = new CapnProtoSchemaHandler();
        const string schema = """
            struct Order {
                id @0 :Int64;
                name @0 :Text;
            }
            """;

        var (isValid, error) = handler.Validate(schema);

        Assert.False(isValid);
        Assert.Contains("Duplicate field ordinals", error);
    }

    [Fact]
    public void CapnProto_CheckCompatibility_NoExistingSchemas_ReturnsCompatible()
    {
        var handler = new CapnProtoSchemaHandler();
        const string schema = """
            struct Order {
                id @0 :Int64;
            }
            """;

        var result = handler.CheckCompatibility(schema, [], CompatibilityMode.Forward);

        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void CapnProto_CheckCompatibility_ModeNone_ReturnsCompatible()
    {
        var handler = new CapnProtoSchemaHandler();
        var existing = CreateSchemaListWithString("""
            struct Order {
                id @0 :Int64;
            }
            """);

        var result = handler.CheckCompatibility("completely different", existing, CompatibilityMode.None);

        Assert.True(result.IsCompatible);
    }

    #endregion

    #region Orleans

    [Fact]
    public void Orleans_TypeName_IsORLEANS()
    {
        var handler = new OrleansSchemaHandler();
        Assert.Equal("ORLEANS", handler.TypeName);
    }

    [Fact]
    public void Orleans_Validate_AcceptsAnySchema()
    {
        var handler = new OrleansSchemaHandler();

        var (isValid, error) = handler.Validate("MyApp.GrainState");

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void Orleans_Validate_AcceptsEmptySchema()
    {
        var handler = new OrleansSchemaHandler();

        var (isValid, error) = handler.Validate("");

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void Orleans_CheckCompatibility_AlwaysCompatible()
    {
        var handler = new OrleansSchemaHandler();
        var existing = CreateSchemaList("v1-schema");

        var result = handler.CheckCompatibility("v2-schema", existing, CompatibilityMode.ForwardTransitive);

        Assert.True(result.IsCompatible);
    }

    #endregion

    #region Helpers

    private static IReadOnlyList<Schema> CreateSchemaList(string schemaString) =>
    [
        new Schema
        {
            Id = 1,
            Subject = "test-subject",
            Version = 1,
            SchemaType = SchemaType.Avro,
            SchemaString = schemaString
        }
    ];

    private static IReadOnlyList<Schema> CreateSchemaListWithString(string schemaString) =>
    [
        new Schema
        {
            Id = 1,
            Subject = "test-subject",
            Version = 1,
            SchemaType = SchemaType.Avro,
            SchemaString = schemaString
        }
    ];

    #endregion
}
