using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for parsing ApiVersions v3+ requests with flexible headers.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ApiVersionsV3ParsingTests
{
    private readonly ITestOutputHelper _output;

    public ApiVersionsV3ParsingTests(ITestOutputHelper output)
    {
        _output = output;
    }

    /// <summary>
    /// This test parses an ApiVersions v3 request exactly as sent by Confluent.Kafka client.
    /// The request format is:
    /// - Header v2: ApiKey (2), ApiVersion (2), CorrelationId (4), ClientId (STRING), TaggedFields (varint)
    /// - Body: ClientSoftwareName (COMPACT_STRING), ClientSoftwareVersion (COMPACT_STRING), TaggedFields (varint)
    /// </summary>
    [Fact]
    public void ParseApiVersionsV3Request_FromConfluentKafkaClient()
    {
        // This is the exact hex dump from Confluent.Kafka client ApiVersions v3 request
        // 00 12 = ApiKey 18 (ApiVersions)
        // 00 03 = ApiVersion 3
        // 00 00 00 01 = CorrelationId 1
        // 00 13 = ClientId length (19)
        // 73 74 6F 72 6D 2D 74 65 73 74 2D 70 72 6F 64 75 63 65 72 = "surgewave-test-producer"
        // 00 = Header tagged fields count (0)
        // 17 = ClientSoftwareName length (23 - 1 = 22 chars + 1 for compact encoding)
        // 63 6F 6E 66 6C 75 65 6E 74 2D 6B 61 66 6B 61 2D 64 6F 74 6E 65 74 = "confluent-kafka-dotnet" (22 chars)
        // 06 = ClientSoftwareVersion length (6 - 1 = 5 chars + 1 for compact encoding)
        // 32 2E 38 2E 30 = "2.8.0" (5 chars)
        // 00 = Body tagged fields count (0)

        byte[] requestBytes = new byte[]
        {
            0x00, 0x12, // ApiKey = 18 (ApiVersions)
            0x00, 0x03, // ApiVersion = 3
            0x00, 0x00, 0x00, 0x01, // CorrelationId = 1
            0x00, 0x13, // ClientId length = 19
            0x73, 0x74, 0x6F, 0x72, 0x6D, 0x2D, 0x74, 0x65, 0x73, 0x74, 0x2D, 0x70, 0x72, 0x6F, 0x64, 0x75, 0x63, 0x65, 0x72, // "surgewave-test-producer"
            0x00, // Header tagged fields count = 0
            0x17, // ClientSoftwareName compact length = 23 (22 + 1)
            0x63, 0x6F, 0x6E, 0x66, 0x6C, 0x75, 0x65, 0x6E, 0x74, 0x2D, 0x6B, 0x61, 0x66, 0x6B, 0x61, 0x2D, 0x64, 0x6F, 0x74, 0x6E, 0x65, 0x74, // "confluent-kafka-dotnet"
            0x06, // ClientSoftwareVersion compact length = 6 (5 + 1)
            0x32, 0x2E, 0x38, 0x2E, 0x30, // "2.8.0"
            0x00 // Body tagged fields count = 0
        };

        _output.WriteLine($"Request bytes length: {requestBytes.Length}");
        _output.WriteLine($"Request hex: {BitConverter.ToString(requestBytes).Replace("-", " ")}");

        // Parse using the protocol handler
        var handler = new KafkaProtocolHandler();
        var request = handler.ParseRequest(requestBytes);

        _output.WriteLine($"Request type: {request.GetType().Name}");
        _output.WriteLine($"CorrelationId: {request.CorrelationId}");

        Assert.Equal("ApiVersionsRequest", request.GetType().Name);
        Assert.Equal(1, request.CorrelationId);
    }

    [Fact]
    public void ParseApiVersionsV3Request_ManualTrace()
    {
        // Let's trace through the parsing step by step
        byte[] requestBytes = new byte[]
        {
            0x00, 0x12, // ApiKey = 18 (ApiVersions)
            0x00, 0x03, // ApiVersion = 3
            0x00, 0x00, 0x00, 0x01, // CorrelationId = 1
            0x00, 0x13, // ClientId length = 19
            0x73, 0x74, 0x6F, 0x72, 0x6D, 0x2D, 0x74, 0x65, 0x73, 0x74, 0x2D, 0x70, 0x72, 0x6F, 0x64, 0x75, 0x63, 0x65, 0x72, // "surgewave-test-producer"
            0x00, // Header tagged fields count = 0
            0x17, // ClientSoftwareName compact length = 23 (22 + 1)
            0x63, 0x6F, 0x6E, 0x66, 0x6C, 0x75, 0x65, 0x6E, 0x74, 0x2D, 0x6B, 0x61, 0x66, 0x6B, 0x61, 0x2D, 0x64, 0x6F, 0x74, 0x6E, 0x65, 0x74, // "confluent-kafka-dotnet"
            0x06, // ClientSoftwareVersion compact length = 6 (5 + 1)
            0x32, 0x2E, 0x38, 0x2E, 0x30, // "2.8.0"
            0x00 // Body tagged fields count = 0
        };

        _output.WriteLine($"Total bytes: {requestBytes.Length}");

        // Manual parsing trace
        using var ms = new MemoryStream(requestBytes);
        using var reader = new BinaryReader(ms);

        // Read header fields
        var apiKey = BinaryHelpers.ReadInt16BigEndian(reader);
        _output.WriteLine($"After ApiKey: position={ms.Position}, apiKey={apiKey}");

        var apiVersion = BinaryHelpers.ReadInt16BigEndian(reader);
        _output.WriteLine($"After ApiVersion: position={ms.Position}, apiVersion={apiVersion}");

        var correlationId = BinaryHelpers.ReadInt32BigEndian(reader);
        _output.WriteLine($"After CorrelationId: position={ms.Position}, correlationId={correlationId}");

        var clientId = BinaryHelpers.ReadString(reader);
        _output.WriteLine($"After ClientId: position={ms.Position}, clientId='{clientId}'");

        // At this point we should be at position 29 (byte 0x00 for header tagged fields)
        _output.WriteLine($"Next byte at position {ms.Position}: 0x{requestBytes[ms.Position]:X2}");

        // For ApiVersions v3, we have a flexible header (v2) with tagged fields
        // The next byte should be the varint count of header tagged fields (should be 0)
        var headerTaggedFields = reader.ReadByte();
        _output.WriteLine($"Header tagged fields count: {headerTaggedFields}, position after: {ms.Position}");

        // Now we should be at position 30, ready to read the body
        // Body has: ClientSoftwareName (compact string), ClientSoftwareVersion (compact string), tagged fields
        var remainingBytes = requestBytes.Length - ms.Position;
        _output.WriteLine($"Remaining bytes for body: {remainingBytes}");

        // Create KafkaProtocolReader for body parsing
        var bodyBytes = reader.ReadBytes((int)remainingBytes);
        var bodyReader = new KafkaProtocolReader(bodyBytes);

        var clientSoftwareName = bodyReader.ReadCompactString();
        _output.WriteLine($"ClientSoftwareName: '{clientSoftwareName}'");

        var clientSoftwareVersion = bodyReader.ReadCompactString();
        _output.WriteLine($"ClientSoftwareVersion: '{clientSoftwareVersion}'");

        var bodyTaggedFields = bodyReader.ReadVarInt();
        _output.WriteLine($"Body tagged fields count: {bodyTaggedFields}");

        Assert.Equal("confluent-kafka-dotnet", clientSoftwareName);
        Assert.Equal("2.8.0", clientSoftwareVersion);
    }
}
