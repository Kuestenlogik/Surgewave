using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Header-parsing edge cases for the span-based request header (#83): tagged-field skipping with
/// real payloads, ClientId bounds and interning. These go through the public ParseRequest entry
/// point, which is the same path the connection handler takes.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RequestHeaderParseTests
{
    private readonly KafkaProtocolHandler _handler = new();

    /// <summary>Writes a Kafka request header. ClientId is a plain STRING in every header version.</summary>
    private static void WriteHeader(List<byte> frame, short apiKey, short apiVersion, int correlationId, string? clientId)
    {
        Span<byte> scratch = stackalloc byte[4];
        BinaryPrimitives.WriteInt16BigEndian(scratch, apiKey);
        frame.AddRange(scratch[..2].ToArray());
        BinaryPrimitives.WriteInt16BigEndian(scratch, apiVersion);
        frame.AddRange(scratch[..2].ToArray());
        BinaryPrimitives.WriteInt32BigEndian(scratch, correlationId);
        frame.AddRange(scratch[..4].ToArray());

        if (clientId is null)
        {
            BinaryPrimitives.WriteInt16BigEndian(scratch, -1);
            frame.AddRange(scratch[..2].ToArray());
        }
        else
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(clientId);
            BinaryPrimitives.WriteInt16BigEndian(scratch, (short)bytes.Length);
            frame.AddRange(scratch[..2].ToArray());
            frame.AddRange(bytes);
        }
    }

    private static void WriteUnsignedVarInt(List<byte> frame, uint value)
    {
        while ((value & ~0x7Fu) != 0)
        {
            frame.Add((byte)((value & 0x7F) | 0x80));
            value >>= 7;
        }
        frame.Add((byte)value);
    }

    /// <summary>ApiVersions v3 has a flexible header, so it exercises the tagged-fields path.</summary>
    private static List<byte> BuildApiVersionsV3Frame(string? clientId, params (uint Tag, byte[] Payload)[] headerTags)
    {
        var frame = new List<byte>();
        WriteHeader(frame, apiKey: 18, apiVersion: 3, correlationId: 7, clientId);

        WriteUnsignedVarInt(frame, (uint)headerTags.Length);
        foreach (var (tag, payload) in headerTags)
        {
            WriteUnsignedVarInt(frame, tag);
            WriteUnsignedVarInt(frame, (uint)payload.Length);
            frame.AddRange(payload);
        }

        // Body: ClientSoftwareName + ClientSoftwareVersion (compact strings) + body tagged fields
        WriteUnsignedVarInt(frame, 1); // empty compact string => length+1 = 1
        WriteUnsignedVarInt(frame, 1);
        WriteUnsignedVarInt(frame, 0); // no body tagged fields
        return frame;
    }

    [Fact]
    public void ParseRequest_FlexibleHeader_SkipsNonEmptyTaggedFields()
    {
        // The old parser only ever saw empty tagged fields in tests; this one carries real payloads.
        var frame = BuildApiVersionsV3Frame(
            "test-client",
            (Tag: 0u, Payload: new byte[] { 1, 2, 3 }),
            (Tag: 1u, Payload: new byte[] { 9 }));

        var request = _handler.ParseRequest(frame.ToArray());

        var apiVersions = Assert.IsType<ApiVersionsRequest>(request);
        Assert.Equal(7, apiVersions.CorrelationId);
        Assert.Equal("test-client", apiVersions.ClientId);
    }

    [Fact]
    public void ParseRequest_TaggedFieldSizeBeyondBuffer_Throws()
    {
        var frame = new List<byte>();
        WriteHeader(frame, apiKey: 18, apiVersion: 3, correlationId: 1, "c");
        WriteUnsignedVarInt(frame, 1);   // one tagged field
        WriteUnsignedVarInt(frame, 0);   // tag id
        WriteUnsignedVarInt(frame, 250); // claims 250 bytes...
        frame.AddRange(new byte[] { 1, 2 }); // ...but only 2 are here

        Assert.Throws<InvalidDataException>(() => _handler.ParseRequest(frame.ToArray()));
    }

    [Fact]
    public void ParseRequest_NullClientId_BecomesEmptyString()
    {
        var frame = BuildApiVersionsV3Frame(clientId: null);

        var request = _handler.ParseRequest(frame.ToArray());

        Assert.Equal(string.Empty, Assert.IsType<ApiVersionsRequest>(request).ClientId);
    }

    [Fact]
    public void ParseRequest_ClientIdLengthBeyondBuffer_Throws()
    {
        var frame = new List<byte>();
        Span<byte> scratch = stackalloc byte[4];
        BinaryPrimitives.WriteInt16BigEndian(scratch, 18);
        frame.AddRange(scratch[..2].ToArray());
        BinaryPrimitives.WriteInt16BigEndian(scratch, 3);
        frame.AddRange(scratch[..2].ToArray());
        BinaryPrimitives.WriteInt32BigEndian(scratch, 1);
        frame.AddRange(scratch[..4].ToArray());
        BinaryPrimitives.WriteInt16BigEndian(scratch, 100); // claims 100 bytes of ClientId...
        frame.AddRange(scratch[..2].ToArray());
        frame.AddRange("short"u8.ToArray()); // ...but only 5 are here

        Assert.Throws<InvalidDataException>(() => _handler.ParseRequest(frame.ToArray()));
    }

    [Fact]
    public void ParseRequest_SameClientId_IsInterned()
    {
        var first = _handler.ParseRequest(BuildApiVersionsV3Frame("interned-client").ToArray());
        var second = _handler.ParseRequest(BuildApiVersionsV3Frame("interned-client").ToArray());

        var a = Assert.IsType<ApiVersionsRequest>(first).ClientId;
        var b = Assert.IsType<ApiVersionsRequest>(second).ClientId;

        Assert.Equal(a, b);
        Assert.Same(a, b); // the shared wire-string cache hands back the same instance
    }

    [Fact]
    public void ParseRequest_TooShortForHeader_Throws()
    {
        Assert.Throws<InvalidDataException>(() => _handler.ParseRequest(new byte[] { 0, 18, 0 }));
    }
}
