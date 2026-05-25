using Kuestenlogik.Surgewave.Core.Serialization;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Serialization;

/// <summary>
/// Tests for <see cref="ContentTypeDetector"/> — auto-detection of message content type
/// from raw payload bytes.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ContentTypeDetectorTests
{
    [Fact]
    public void Detect_EmptyPayload_ReturnsOctetStream()
    {
        var result = ContentTypeDetector.Detect(ReadOnlySpan<byte>.Empty);

        Assert.Equal(ContentTypes.OctetStream, result);
    }

    [Fact]
    public void Detect_JsonObject_ReturnsJson()
    {
        byte[] payload = """{"key":"value"}"""u8.ToArray();

        var result = ContentTypeDetector.Detect(payload);

        Assert.Equal(ContentTypes.Json, result);
    }

    [Fact]
    public void Detect_JsonArray_ReturnsJson()
    {
        byte[] payload = """[1,2,3]"""u8.ToArray();

        var result = ContentTypeDetector.Detect(payload);

        Assert.Equal(ContentTypes.Json, result);
    }

    [Fact]
    public void Detect_ConfluentWireFormat_ReturnsConfluentSchema()
    {
        // Confluent wire format: magic byte 0x00 followed by 4-byte schema ID
        byte[] payload = [0x00, 0x00, 0x00, 0x00, 0x01, 0xFF, 0xAB];

        var result = ContentTypeDetector.Detect(payload);

        Assert.Equal("application/x-confluent-schema", result);
    }

    [Theory]
    [InlineData(0x80)] // fixmap lower bound
    [InlineData(0x85)] // fixmap mid-range
    [InlineData(0x8F)] // fixmap upper bound
    public void Detect_MessagePackFixmap_ReturnsMessagePack(byte firstByte)
    {
        byte[] payload = [firstByte, 0x01, 0x02];

        var result = ContentTypeDetector.Detect(payload);

        Assert.Equal(ContentTypes.MessagePack, result);
    }

    [Theory]
    [InlineData(0x90)] // fixarray lower bound
    [InlineData(0x9F)] // fixarray upper bound
    public void Detect_MessagePackFixarray_ReturnsMessagePack(byte firstByte)
    {
        byte[] payload = [firstByte, 0x01, 0x02];

        var result = ContentTypeDetector.Detect(payload);

        Assert.Equal(ContentTypes.MessagePack, result);
    }

    [Theory]
    [InlineData(0xDC)] // array16
    [InlineData(0xDD)] // array32
    [InlineData(0xDE)] // map16
    [InlineData(0xDF)] // map32
    public void Detect_MessagePackMapArray16_32_ReturnsMessagePack(byte firstByte)
    {
        byte[] payload = [firstByte, 0x00, 0x01];

        var result = ContentTypeDetector.Detect(payload);

        Assert.Equal(ContentTypes.MessagePack, result);
    }

    [Fact]
    public void Detect_PrintableAsciiText_ReturnsTextPlain()
    {
        byte[] payload = "Hello, World!"u8.ToArray();

        var result = ContentTypeDetector.Detect(payload);

        Assert.Equal("text/plain", result);
    }

    [Fact]
    public void Detect_BinaryData_ReturnsOctetStream()
    {
        // Non-printable bytes outside all detection ranges
        byte[] payload = [0x01, 0x02, 0x03, 0x04, 0x05];

        var result = ContentTypeDetector.Detect(payload);

        Assert.Equal(ContentTypes.OctetStream, result);
    }

    [Fact]
    public void Detect_SingleNullByte_ReturnsOctetStream()
    {
        // Single 0x00 byte is too short for Confluent wire format (needs 5 bytes)
        byte[] payload = [0x00];

        var result = ContentTypeDetector.Detect(payload);

        Assert.Equal(ContentTypes.OctetStream, result);
    }

    [Fact]
    public void Detect_ConfluentWireFormatExactly5Bytes_ReturnsConfluentSchema()
    {
        // Minimum valid Confluent wire format: exactly 5 bytes
        byte[] payload = [0x00, 0x00, 0x00, 0x00, 0x01];

        var result = ContentTypeDetector.Detect(payload);

        Assert.Equal("application/x-confluent-schema", result);
    }

    [Fact]
    public void Detect_ConfluentWireFormatTooShort_ReturnsOctetStream()
    {
        // Only 4 bytes starting with 0x00 — not enough for Confluent format
        byte[] payload = [0x00, 0x01, 0x02, 0x03];

        var result = ContentTypeDetector.Detect(payload);

        Assert.Equal(ContentTypes.OctetStream, result);
    }
}
