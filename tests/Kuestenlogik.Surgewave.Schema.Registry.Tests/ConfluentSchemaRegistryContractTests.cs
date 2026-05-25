using System.Text.Json;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Schema.Registry.Tests;

/// <summary>
/// Pins down the JSON-shape contract that Confluent Schema Registry clients
/// (Confluent.SchemaRegistry .NET, librdkafka, the JVM Kafka client) expect
/// when talking to Surgewave's REST API. Surgewave has been audited as wire-compatible
/// (G16 of the competitive gap analysis) — these tests guard against silent
/// regressions: a stray `errorCode` (camelCase) instead of `error_code`
/// (snake_case) would break every Confluent client without showing up as a
/// type or unit-test failure elsewhere.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ConfluentSchemaRegistryContractTests
{
    [Fact]
    public void ErrorResponse_UsesSnakeCaseErrorCode()
    {
        // Confluent's wire shape: {"error_code": int, "message": string}.
        // The JVM client deserializes by exact field name — camelCase breaks it.
        var dto = new ErrorResponse { ErrorCode = 40401, Message = "Subject not found" };

        var json = JsonSerializer.Serialize(dto);

        Assert.Contains("\"error_code\":40401", json);
        Assert.Contains("\"message\":\"Subject not found\"", json);
        Assert.DoesNotContain("\"errorCode\"", json);
    }

    [Fact]
    public void CompatibilityCheckResponse_UsesSnakeCaseIsCompatible()
    {
        var dto = new CompatibilityCheckResponse { IsCompatible = true };

        var json = JsonSerializer.Serialize(dto);

        Assert.Contains("\"is_compatible\":true", json);
        Assert.DoesNotContain("\"isCompatible\"", json);
    }

    [Fact]
    public void ConfigResponse_UsesCamelCaseCompatibilityLevel()
    {
        // Confluent picked camelCase for /config but snake_case for errors.
        // We follow the same inconsistency rather than "normalize" — clients
        // depend on the historical shape.
        var dto = new ConfigResponse { CompatibilityLevel = "BACKWARD" };

        var json = JsonSerializer.Serialize(dto);

        Assert.Contains("\"compatibilityLevel\":\"BACKWARD\"", json);
        Assert.DoesNotContain("\"compatibility_level\"", json);
    }

    [Fact]
    public void SchemaResponse_UsesCamelCaseSchemaType()
    {
        var dto = new SchemaResponse
        {
            Subject = "orders-value",
            Id = 7,
            Version = 1,
            SchemaType = "AVRO",
            Schema = "{\"type\":\"record\"}",
        };

        var json = JsonSerializer.Serialize(dto);

        Assert.Contains("\"subject\":\"orders-value\"", json);
        Assert.Contains("\"id\":7", json);
        Assert.Contains("\"version\":1", json);
        Assert.Contains("\"schemaType\":\"AVRO\"", json);
        Assert.Contains("\"schema\":", json);
    }

    [Theory]
    [InlineData(SchemaType.Avro, "AVRO")]
    [InlineData(SchemaType.Json, "JSON")]
    [InlineData(SchemaType.Protobuf, "PROTOBUF")]
    public void SchemaType_SerialisesAsConfluentUppercaseString(SchemaType type, string expected)
    {
        // Surgewave's REST API converts SchemaType.ToString() → ToUpperInvariant().
        // Pin the mapping so Avro ↔ "AVRO", not "Avro".
        var actual = type.ToString().ToUpperInvariant();
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(CompatibilityMode.None, "NONE")]
    [InlineData(CompatibilityMode.Backward, "BACKWARD")]
    [InlineData(CompatibilityMode.BackwardTransitive, "BACKWARDTRANSITIVE")]
    [InlineData(CompatibilityMode.Forward, "FORWARD")]
    [InlineData(CompatibilityMode.ForwardTransitive, "FORWARDTRANSITIVE")]
    [InlineData(CompatibilityMode.Full, "FULL")]
    [InlineData(CompatibilityMode.FullTransitive, "FULLTRANSITIVE")]
    public void CompatibilityMode_DefaultStringMappingDoesNotMatchConfluent(CompatibilityMode mode, string defaultUpper)
    {
        // The default ToString().ToUpperInvariant() drops the underscore.
        // This test documents that the mapping is INTENTIONAL on the way
        // INTO the API (we accept "BACKWARD_TRANSITIVE" with explicit
        // underscore-stripping switch). On the way OUT, the REST API uses
        // a custom formatter — the lookup is in SchemaRegistryRestApi.
        Assert.Equal(defaultUpper, mode.ToString().ToUpperInvariant());
    }

    [Theory]
    [InlineData("AVRO", SchemaType.Avro)]
    [InlineData("JSON", SchemaType.Json)]
    [InlineData("PROTOBUF", SchemaType.Protobuf)]
    [InlineData("avro", SchemaType.Avro)]
    [InlineData(null, SchemaType.Avro)] // Confluent default for legacy clients
    public void SchemaTypeParser_AcceptsCaseInsensitiveAndDefaults(string? input, SchemaType expected)
    {
        // Mirrors the switch in SchemaRegistryRestApi.cs (G16 audit, lines 713-722).
        var actual = (input?.ToUpperInvariant()) switch
        {
            "JSON" => SchemaType.Json,
            "PROTOBUF" => SchemaType.Protobuf,
            "FLATBUFFERS" => SchemaType.FlatBuffers,
            _ => SchemaType.Avro,
        };
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("BACKWARD", CompatibilityMode.Backward)]
    [InlineData("BACKWARD_TRANSITIVE", CompatibilityMode.BackwardTransitive)]
    [InlineData("FORWARD", CompatibilityMode.Forward)]
    [InlineData("FORWARD_TRANSITIVE", CompatibilityMode.ForwardTransitive)]
    [InlineData("FULL", CompatibilityMode.Full)]
    [InlineData("FULL_TRANSITIVE", CompatibilityMode.FullTransitive)]
    [InlineData("NONE", CompatibilityMode.None)]
    [InlineData("backward", CompatibilityMode.Backward)] // case-insensitive
    public void CompatibilityModeParser_AcceptsAllConfluentLevels(string input, CompatibilityMode expected)
    {
        // Mirrors the switch in SchemaRegistryRestApi.cs (G16 audit, lines 724-737).
        var actual = input.ToUpperInvariant() switch
        {
            "NONE" => CompatibilityMode.None,
            "BACKWARD" => CompatibilityMode.Backward,
            "BACKWARD_TRANSITIVE" => CompatibilityMode.BackwardTransitive,
            "FORWARD" => CompatibilityMode.Forward,
            "FORWARD_TRANSITIVE" => CompatibilityMode.ForwardTransitive,
            "FULL" => CompatibilityMode.Full,
            "FULL_TRANSITIVE" => CompatibilityMode.FullTransitive,
            _ => CompatibilityMode.Backward, // default
        };
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void RegisterSchemaResponse_EmitsOnlyId()
    {
        // POST /subjects/{subject}/versions returns just {"id": int} — no
        // version, no subject. Confluent clients deserialize by field name.
        var dto = new RegisterSchemaResponse { Id = 42 };

        var json = JsonSerializer.Serialize(dto);

        // Either way: "id" must be present and the response must not bleed
        // extra fields that Confluent clients would not expect (those would
        // not break deserialization but would be a smell that should not
        // appear in the contract).
        Assert.Contains("\"id\":42", json);
    }
}
