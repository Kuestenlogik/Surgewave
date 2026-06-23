using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Schema;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — Schema-Registry-shaped payloads in the
/// Protocol.Native.Payloads.Schema sub-namespace. These back the
/// schema-registry RPCs that Avro / Protobuf / JSON-Schema clients
/// (Confluent compat layer) round-trip on every produce. A framing
/// regression here surfaces as "schema-id not found" or "compatibility
/// check always passes" reports — hard to root-cause from a customer
/// without a wire pin.
///
/// Covers <see cref="SchemaPayload"/>, <see cref="SchemaReferencePayload"/>,
/// <see cref="SchemaRegistrationPayload"/>, <see cref="CompatibilityResultPayload"/>.
/// </summary>
public sealed class SchemaPayloadRoundTripTests
{
    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // SchemaReference (used inside SchemaPayload)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void SchemaReferencePayload_RoundTrip_PreservesAllFields()
    {
        var original = new SchemaReferencePayload
        {
            Name = "common.proto",
            Subject = "common-value",
            Version = 3,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return SchemaReferencePayload.Read(ref r); });
        Assert.Equal("common.proto", parsed.Name);
        Assert.Equal("common-value", parsed.Subject);
        Assert.Equal(3, parsed.Version);
    }

    // ───────────────────────────────────────────────────────────────
    // Schema (the dominant Schema-Registry response payload)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void SchemaPayload_RoundTrip_AvroPayload_NoReferences()
    {
        // Typical Avro schema — single-file JSON, no PROTOBUF imports.
        var original = new SchemaPayload
        {
            Id = 42,
            Subject = "events-value",
            Version = 5,
            SchemaType = 0, // 0 = AVRO in Confluent's convention
            SchemaString = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
            References = null,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return SchemaPayload.Read(ref r); });

        Assert.Equal(42, parsed.Id);
        Assert.Equal("events-value", parsed.Subject);
        Assert.Equal(5, parsed.Version);
        Assert.Equal((byte)0, parsed.SchemaType);
        Assert.Contains("\"name\":\"Event\"", parsed.SchemaString);
        Assert.Null(parsed.References);
    }

    [Fact]
    public void SchemaPayload_RoundTrip_ProtobufWithReferences()
    {
        // PROTOBUF schema with two imports — references is non-null and
        // carries Name + Subject + Version per reference.
        var original = new SchemaPayload
        {
            Id = 100,
            Subject = "orders-value",
            Version = 1,
            SchemaType = 2, // 2 = PROTOBUF in Confluent's convention
            SchemaString = "syntax = \"proto3\"; import \"common.proto\"; import \"types.proto\"; message Order { ... }",
            References = new[]
            {
                new SchemaReferencePayload { Name = "common.proto", Subject = "common-value", Version = 3 },
                new SchemaReferencePayload { Name = "types.proto",  Subject = "types-value",  Version = 7 },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return SchemaPayload.Read(ref r); });

        Assert.Equal((byte)2, parsed.SchemaType);
        Assert.NotNull(parsed.References);
        Assert.Equal(2, parsed.References!.Count);
        Assert.Equal("common.proto", parsed.References[0].Name);
        Assert.Equal(7, parsed.References[1].Version);
    }

    [Fact]
    public void SchemaPayload_RoundTrip_EmptyReferenceListBecomesNull()
    {
        // The Write side writes a 0-count for null OR empty references; Read
        // returns null when refCount=0. Pin that the asymmetric encoding
        // round-trips cleanly (null in → null back).
        var original = new SchemaPayload
        {
            Id = 1,
            Subject = "s",
            Version = 1,
            SchemaType = 0,
            SchemaString = "{}",
            References = Array.Empty<SchemaReferencePayload>(), // empty, not null
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return SchemaPayload.Read(ref r); });
        // Wire shape doesn't distinguish empty from null; Read normalises
        // to null for both. Document that contract.
        Assert.Null(parsed.References);
    }

    // ───────────────────────────────────────────────────────────────
    // SchemaRegistration (RegisterSchema response — id + version pair)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void SchemaRegistrationPayload_RoundTrip_PreservesIdAndVersion()
    {
        var original = new SchemaRegistrationPayload { SchemaId = 9_999, Version = 2 };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return SchemaRegistrationPayload.Read(ref r); });
        Assert.Equal(9_999, parsed.SchemaId);
        Assert.Equal(2, parsed.Version);
    }

    // ───────────────────────────────────────────────────────────────
    // CompatibilityResult (CheckCompatibility response)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void CompatibilityResultPayload_RoundTrip_CompatibleWithNoMessages()
    {
        var original = new CompatibilityResultPayload
        {
            IsCompatible = true,
            Messages = null,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CompatibilityResultPayload.Read(ref r); });

        Assert.True(parsed.IsCompatible);
        Assert.Null(parsed.Messages);
    }

    [Fact]
    public void CompatibilityResultPayload_RoundTrip_IncompatibleWithDetailedReasons()
    {
        var original = new CompatibilityResultPayload
        {
            IsCompatible = false,
            Messages = new[]
            {
                "Field 'order_id' was removed (BACKWARD breaks)",
                "Required field 'customer_id' added without default (FORWARD breaks)",
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CompatibilityResultPayload.Read(ref r); });

        Assert.False(parsed.IsCompatible);
        Assert.NotNull(parsed.Messages);
        Assert.Equal(2, parsed.Messages!.Count);
        Assert.Contains("order_id", parsed.Messages[0]);
        Assert.Contains("customer_id", parsed.Messages[1]);
    }

    [Fact]
    public void CompatibilityResultPayload_RoundTrip_EmptyMessagesBecomesNull()
    {
        // Same null-vs-empty asymmetry as SchemaPayload.References.
        var original = new CompatibilityResultPayload
        {
            IsCompatible = true,
            Messages = Array.Empty<string>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CompatibilityResultPayload.Read(ref r); });
        Assert.Null(parsed.Messages);
    }
}
