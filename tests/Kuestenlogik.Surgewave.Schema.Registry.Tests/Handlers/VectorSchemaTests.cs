using Kuestenlogik.Surgewave.Schema.Registry;
using Kuestenlogik.Surgewave.Schema.Registry.Client;
using Kuestenlogik.Surgewave.Schema.Registry.Handlers;
using Kuestenlogik.Surgewave.Schema.Registry.Serdes.Avro;
using Kuestenlogik.Surgewave.Schema.Registry.Serdes.Json;
using Kuestenlogik.Surgewave.Schema.Registry.Vectors;
using Kuestenlogik.Surgewave.Testing;
using Xunit;
using RegistrySchema = Kuestenlogik.Surgewave.Schema.Registry.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Tests.Handlers;

/// <summary>
/// The vector schema primitive (#14): <c>vector(dim, dtype)</c> declared per format — Avro
/// <c>"logicalType": "vector", "dim": N</c>, JSON Schema <c>"format": "vector"</c> +
/// <c>x-vector-dim</c>/<c>x-vector-dtype</c>, Protobuf <c>[(surgewave.vector).dim = N]</c> —
/// and enforced identically everywhere: a vector field's dim and dtype must never change, and
/// vector-ness must not appear on or disappear from an existing field.
///
/// <para>These tests pin the STRICTNESS deliberately. A 768→1536 "widening" passes every
/// classic type check (it is still an array of float) and then silently corrupts every consumer
/// that indexes, allocates, or computes against the declared dimension. Do not relax the
/// equality rule into a warning.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class VectorSchemaTests
{
    private static readonly AvroSchemaHandler Avro = new();
    private static readonly JsonSchemaHandler Json = new();
    private static readonly ProtobufSchemaHandler Protobuf = new();

    private static string AvroSchema(string embeddingType) => $$"""
        {
            "type": "record",
            "name": "DocumentChunk",
            "fields": [
                {"name": "text", "type": "string"},
                {"name": "embedding", "type": {{embeddingType}}}
            ]
        }
        """;

    private const string AvroVector768 = """{"type": "array", "items": "float", "logicalType": "vector", "dim": 768}""";
    private const string AvroVector1536 = """{"type": "array", "items": "float", "logicalType": "vector", "dim": 1536}""";
    private const string AvroVectorF64 = """{"type": "array", "items": "double", "logicalType": "vector", "dim": 768}""";
    private const string AvroPlainFloatArray = """{"type": "array", "items": "float"}""";

    private static string JsonSchema(string embedding) => $$"""
        {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "embedding": {{embedding}}
            }
        }
        """;

    private const string JsonVector768 =
        """{"type": "array", "items": {"type": "number"}, "format": "vector", "x-vector-dim": 768, "x-vector-dtype": "f32"}""";
    private const string JsonVector1536 =
        """{"type": "array", "items": {"type": "number"}, "format": "vector", "x-vector-dim": 1536, "x-vector-dtype": "f32"}""";
    private const string JsonVectorF64 =
        """{"type": "array", "items": {"type": "number"}, "format": "vector", "x-vector-dim": 768, "x-vector-dtype": "f64"}""";

    private static string ProtoSchema(string field) => $$"""
        syntax = "proto3";
        message DocumentChunk {
            string text = 1;
            {{field}}
        }
        """;

    private static RegistrySchema Existing(string schemaString, SchemaType type) => new()
    {
        Id = 1,
        Subject = "chunk-value",
        Version = 1,
        SchemaType = type,
        SchemaString = schemaString,
    };

    // ---- Gemeinsames Modell ----

    [Theory]
    [InlineData("f32", VectorDtype.F32)]
    [InlineData("F64", VectorDtype.F64)]
    [InlineData(" i8 ", VectorDtype.I8)]
    public void DtypeNames_ParseCaseInsensitively(string name, VectorDtype expected)
    {
        Assert.True(VectorType.TryParseDtype(name, out var dtype));
        Assert.Equal(expected, dtype);
    }

    [Theory]
    [InlineData("f128")]
    [InlineData("float")]
    [InlineData("")]
    public void UnknownDtypeNames_AreRejected(string name)
    {
        Assert.False(VectorType.TryParseDtype(name, out _));
    }

    [Fact]
    public void SharedRule_RejectsEveryChange_AndOnlyChanges()
    {
        var v768 = new VectorType(768, VectorDtype.F32);

        Assert.Null(VectorType.CheckCompatible(null, null));
        Assert.Null(VectorType.CheckCompatible(v768, v768));
        Assert.NotNull(VectorType.CheckCompatible(v768, new VectorType(1536, VectorDtype.F32)));
        Assert.NotNull(VectorType.CheckCompatible(v768, new VectorType(768, VectorDtype.F64)));
        Assert.NotNull(VectorType.CheckCompatible(v768, null));
        Assert.NotNull(VectorType.CheckCompatible(null, v768));
    }

    // ---- Avro ----

    [Fact]
    public void Avro_ValidVectorSchema_Validates()
    {
        // Beweist zugleich, dass Chr.Avro den unbekannten logicalType spec-konform toleriert —
        // fiele das, wiese Validate jedes Vector-Schema ab und dieser Test schlüge an.
        var (isValid, error) = Avro.Validate(AvroSchema(AvroVector768));
        Assert.True(isValid, error);
    }

    [Theory]
    [InlineData("""{"type": "array", "items": "float", "logicalType": "vector"}""")]
    [InlineData("""{"type": "array", "items": "float", "logicalType": "vector", "dim": 0}""")]
    [InlineData("""{"type": "array", "items": "string", "logicalType": "vector", "dim": 8}""")]
    public void Avro_MalformedVector_IsRejected(string embeddingType)
    {
        var (isValid, _) = Avro.Validate(AvroSchema(embeddingType));
        Assert.False(isValid);
    }

    [Fact]
    public void Avro_SameVector_IsCompatible()
    {
        var result = Avro.CheckCompatibility(
            AvroSchema(AvroVector768),
            [Existing(AvroSchema(AvroVector768), SchemaType.Avro)],
            CompatibilityMode.Backward);
        Assert.True(result.IsCompatible, string.Join("; ", result.Messages ?? []));
    }

    [Theory]
    [InlineData(AvroVector1536)]     // dim 768 -> 1536
    [InlineData(AvroVectorF64)]      // dtype f32 -> f64 (klassisch sogar promotable!)
    [InlineData(AvroPlainFloatArray)] // Vector-Annotation entfernt
    public void Avro_VectorChange_IsIncompatible(string newEmbeddingType)
    {
        var result = Avro.CheckCompatibility(
            AvroSchema(newEmbeddingType),
            [Existing(AvroSchema(AvroVector768), SchemaType.Avro)],
            CompatibilityMode.Backward);
        Assert.False(result.IsCompatible);
        Assert.Contains(result.Messages!, m => m.Contains("vector", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Avro_PlainFloatArrays_StayUnaffected()
    {
        // Regressionswächter: die Vector-Regel darf gewöhnliche Arrays nicht anfassen.
        var result = Avro.CheckCompatibility(
            AvroSchema(AvroPlainFloatArray),
            [Existing(AvroSchema(AvroPlainFloatArray), SchemaType.Avro)],
            CompatibilityMode.Backward);
        Assert.True(result.IsCompatible);
    }

    // ---- JSON Schema ----

    [Fact]
    public void Json_ValidVectorSchema_Validates()
    {
        var (isValid, error) = Json.Validate(JsonSchema(JsonVector768));
        Assert.True(isValid, error);
    }

    [Theory]
    [InlineData("""{"type": "array", "items": {"type": "number"}, "format": "vector"}""")]
    [InlineData("""{"type": "array", "items": {"type": "number"}, "format": "vector", "x-vector-dim": -1}""")]
    [InlineData("""{"type": "array", "items": {"type": "number"}, "x-vector-dim": 8, "x-vector-dtype": "f128"}""")]
    [InlineData("""{"type": "string", "format": "vector", "x-vector-dim": 8}""")]
    public void Json_MalformedVector_IsRejected(string embedding)
    {
        var (isValid, _) = Json.Validate(JsonSchema(embedding));
        Assert.False(isValid);
    }

    [Fact]
    public void Json_SameVector_IsCompatible()
    {
        var result = Json.CheckCompatibility(
            JsonSchema(JsonVector768),
            [Existing(JsonSchema(JsonVector768), SchemaType.Json)],
            CompatibilityMode.Backward);
        Assert.True(result.IsCompatible, string.Join("; ", result.Messages ?? []));
    }

    [Theory]
    [InlineData(JsonVector1536)]
    [InlineData(JsonVectorF64)]
    public void Json_VectorChange_IsIncompatible(string newEmbedding)
    {
        var result = Json.CheckCompatibility(
            JsonSchema(newEmbedding),
            [Existing(JsonSchema(JsonVector768), SchemaType.Json)],
            CompatibilityMode.Backward);
        Assert.False(result.IsCompatible);
        Assert.Contains(result.Messages!, m => m.Contains("vector", StringComparison.OrdinalIgnoreCase));
    }

    // ---- Protobuf ----

    [Fact]
    public void Protobuf_ValidVectorField_Validates()
    {
        var (isValid, error) = Protobuf.Validate(
            ProtoSchema("repeated float embedding = 2 [(surgewave.vector).dim = 768];"));
        Assert.True(isValid, error);
    }

    [Theory]
    [InlineData("float embedding = 2 [(surgewave.vector).dim = 768];")]          // nicht repeated
    [InlineData("repeated string embedding = 2 [(surgewave.vector).dim = 768];")] // falscher Typ
    [InlineData("repeated float embedding = 2 [(surgewave.vector).dim = 0];")]    // dim 0
    public void Protobuf_MalformedVector_IsRejected(string field)
    {
        var (isValid, _) = Protobuf.Validate(ProtoSchema(field));
        Assert.False(isValid);
    }

    [Fact]
    public void Protobuf_SameVector_IsCompatible()
    {
        const string field = "repeated float embedding = 2 [(surgewave.vector).dim = 768];";
        var result = Protobuf.CheckCompatibility(
            ProtoSchema(field),
            [Existing(ProtoSchema(field), SchemaType.Protobuf)],
            CompatibilityMode.Backward);
        Assert.True(result.IsCompatible, string.Join("; ", result.Messages ?? []));
    }

    [Theory]
    [InlineData("repeated float embedding = 2 [(surgewave.vector).dim = 1536];")] // dim-Wechsel
    [InlineData("repeated float embedding = 2;")]                                 // Annotation entfernt
    public void Protobuf_VectorChange_IsIncompatible(string newField)
    {
        var result = Protobuf.CheckCompatibility(
            ProtoSchema(newField),
            [Existing(ProtoSchema("repeated float embedding = 2 [(surgewave.vector).dim = 768];"), SchemaType.Protobuf)],
            CompatibilityMode.Backward);
        Assert.False(result.IsCompatible);
        Assert.Contains(result.Messages!, m => m.Contains("vector", StringComparison.OrdinalIgnoreCase));
    }

    // ---- Client-Attribut -> Schemagenerierung -> Registry-Validierung (end-to-end) ----

    private sealed class DocumentChunk
    {
        public string Text { get; set; } = "";

        [SurgewaveVector(4)]
        public float[] Embedding { get; set; } = [];
    }

    private sealed class BrokenChunk
    {
        [SurgewaveVector(4)]
        public string[] Embedding { get; set; } = [];
    }

    [Fact]
    public void AvroAnnotator_StampsVector_AndTheHandlerAcceptsIt()
    {
        var builder = new Chr.Avro.Abstract.SchemaBuilder();
        var writer = new Chr.Avro.Representation.JsonSchemaWriter();
        var schemaJson = writer.Write(builder.BuildSchema<DocumentChunk>());

        var annotated = AvroVectorSchemaAnnotator.Annotate(schemaJson, typeof(DocumentChunk));

        Assert.Contains("\"logicalType\":\"vector\"", annotated);
        Assert.Contains("\"dim\":4", annotated);
        var (isValid, error) = Avro.Validate(annotated);
        Assert.True(isValid, error);
    }

    [Fact]
    public void JsonAnnotator_StampsVector_AndTheHandlerAcceptsIt()
    {
        var schema = JsonVectorSchemaAnnotator.Apply(
            NJsonSchema.JsonSchema.FromType<DocumentChunk>(), typeof(DocumentChunk));

        var json = schema.ToJson();
        Assert.Contains("x-vector-dim", json);
        var (isValid, error) = Json.Validate(json);
        Assert.True(isValid, error);
    }

    [Fact]
    public void Annotators_RejectNonFloatMembers_AtSchemaGeneration()
    {
        // Fail fast beim ersten Serialisieren statt still ein Schema ohne Garantie zu liefern.
        Assert.Throws<InvalidOperationException>(() =>
            JsonVectorSchemaAnnotator.Apply(
                NJsonSchema.JsonSchema.FromType<BrokenChunk>(), typeof(BrokenChunk)));
    }
}
