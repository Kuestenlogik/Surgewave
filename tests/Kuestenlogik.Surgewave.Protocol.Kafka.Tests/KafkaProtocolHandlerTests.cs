using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for KafkaProtocolHandler - parsing requests and writing responses.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class KafkaProtocolHandlerTests
{
    private readonly KafkaProtocolHandler _handler = new();

    #region Protocol Identity Tests

    [Fact]
    public void ProtocolHandler_Name_IsKafka()
    {
        Assert.Equal("kafka", _handler.ProtocolName);
    }

    [Fact]
    public void ProtocolHandler_Version_IsNonEmpty()
    {
        Assert.False(string.IsNullOrEmpty(_handler.ProtocolVersion));
    }

    #endregion

    #region ParseRequest Tests

    [Fact]
    public void ParseRequest_ApiVersionsV0_ReturnsApiVersionsRequest()
    {
        // Arrange - ApiVersions v0 request (no body)
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(18); // ApiKey = ApiVersions
        writer.WriteInt16(0);  // ApiVersion = 0
        writer.WriteInt32(1);  // CorrelationId
        writer.WriteString("test-client"); // ClientId

        // Act
        var request = _handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.IsType<ApiVersionsRequest>(request);
        Assert.Equal(1, request.CorrelationId);
        Assert.Equal("test-client", request.ClientId);
    }

    [Fact]
    public void ParseRequest_ApiVersionsV3_ReturnsApiVersionsRequestWithSoftwareInfo()
    {
        // Arrange - ApiVersions v3 request with flexible header + body
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(18); // ApiKey = ApiVersions
        writer.WriteInt16(3);  // ApiVersion = 3 (flexible)
        writer.WriteInt32(42); // CorrelationId
        writer.WriteString("test-client"); // ClientId (always STRING, even in flexible)
        writer.WriteVarInt(0); // Header tagged fields (empty)
        writer.WriteCompactString("my-app"); // ClientSoftwareName
        writer.WriteCompactString("1.0.0"); // ClientSoftwareVersion
        writer.WriteVarInt(0); // Body tagged fields

        // Act
        var request = _handler.ParseRequest(writer.ToArray());

        // Assert
        var apiVersionsRequest = Assert.IsType<ApiVersionsRequest>(request);
        Assert.Equal(42, request.CorrelationId);
        Assert.Equal("my-app", apiVersionsRequest.ClientSoftwareName);
        Assert.Equal("1.0.0", apiVersionsRequest.ClientSoftwareVersion);
    }

    [Fact]
    public void ParseRequest_MetadataV0_ReturnsMetadataRequest()
    {
        // Arrange - Metadata v0 request
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(3);   // ApiKey = Metadata
        writer.WriteInt16(0);   // ApiVersion = 0
        writer.WriteInt32(10);  // CorrelationId
        writer.WriteString("test-client");
        writer.WriteInt32(1);   // 1 topic
        writer.WriteString("test-topic");

        // Act
        var request = _handler.ParseRequest(writer.ToArray());

        // Assert
        var metaRequest = Assert.IsType<MetadataRequest>(request);
        Assert.Equal(10, request.CorrelationId);
        Assert.NotNull(metaRequest.Topics);
        Assert.Single(metaRequest.Topics);
        Assert.Equal("test-topic", metaRequest.Topics[0].Name);
    }

    [Fact]
    public void ParseRequest_MetadataV0_AllTopics_NullTopics()
    {
        // Arrange - Metadata v0 with -1 topics (all topics)
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(3);  // ApiKey = Metadata
        writer.WriteInt16(0);  // ApiVersion = 0
        writer.WriteInt32(5);  // CorrelationId
        writer.WriteString("client");
        writer.WriteInt32(-1); // null array

        // Act
        var request = _handler.ParseRequest(writer.ToArray());

        // Assert
        var metaRequest = Assert.IsType<MetadataRequest>(request);
        Assert.Null(metaRequest.Topics);
    }

    [Fact]
    public void ParseRequest_SaslHandshakeV0_ReturnsSaslHandshakeRequest()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(17);  // ApiKey = SaslHandshake
        writer.WriteInt16(0);   // ApiVersion = 0
        writer.WriteInt32(7);   // CorrelationId
        writer.WriteString("client");
        writer.WriteString("PLAIN"); // Mechanism

        // Act
        var request = _handler.ParseRequest(writer.ToArray());

        // Assert
        var saslRequest = Assert.IsType<SaslHandshakeRequest>(request);
        Assert.Equal(7, request.CorrelationId);
        Assert.Equal("PLAIN", saslRequest.Mechanism);
    }

    [Fact]
    public void ParseRequest_HeartbeatV0_ReturnsHeartbeatRequest()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(12);   // ApiKey = Heartbeat
        writer.WriteInt16(0);    // ApiVersion = 0
        writer.WriteInt32(100);  // CorrelationId
        writer.WriteString("consumer-client");
        writer.WriteString("my-group"); // GroupId
        writer.WriteInt32(3);    // GenerationId
        writer.WriteString("member-123"); // MemberId

        // Act
        var request = _handler.ParseRequest(writer.ToArray());

        // Assert
        var heartbeat = Assert.IsType<HeartbeatRequest>(request);
        Assert.Equal(100, request.CorrelationId);
        Assert.Equal("my-group", heartbeat.GroupId);
        Assert.Equal(3, heartbeat.GenerationId);
        Assert.Equal("member-123", heartbeat.MemberId);
    }

    [Fact]
    public void ParseRequest_TooShort_ThrowsInvalidDataException()
    {
        // Arrange - fewer than 8 bytes (minimum header)
        var tooShort = new byte[] { 0x00, 0x12, 0x00 };

        // Act & Assert
        Assert.Throws<InvalidDataException>(() => _handler.ParseRequest(tooShort));
    }

    [Fact]
    public void ParseResponse_ThrowsNotSupportedException()
    {
        // Act & Assert
        Assert.Throws<NotSupportedException>(() =>
            _handler.ParseResponse(new byte[] { 1, 2, 3, 4 }));
    }

    #endregion

    #region ReadRequestAsync Tests

    [Fact]
    public async Task ReadRequestAsync_ValidKafkaRequest_ParsesSuccessfully()
    {
        // Arrange - write a properly framed request to a stream
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(18); // ApiKey = ApiVersions
        writer.WriteInt16(0);  // ApiVersion = 0
        writer.WriteInt32(1);  // CorrelationId
        writer.WriteString("client");

        var body = writer.ToArray();
        using var stream = new MemoryStream();
        // Write 4-byte big-endian size prefix
        var sizeBuffer = new byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(sizeBuffer, body.Length);
        stream.Write(sizeBuffer);
        stream.Write(body);
        stream.Position = 0;

        // Act
        var (size, request) = await _handler.ReadRequestAsync(stream);

        // Assert
        Assert.Equal(body.Length, size);
        Assert.IsType<ApiVersionsRequest>(request);
    }

    [Fact]
    public async Task ReadRequestAsync_ZeroSize_ThrowsInvalidDataException()
    {
        // Arrange - size of 0
        using var stream = new MemoryStream();
        var sizeBuffer = new byte[4]; // all zeros = size 0
        stream.Write(sizeBuffer);
        stream.Position = 0;

        // Act & Assert
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await _handler.ReadRequestAsync(stream));
    }

    [Fact]
    public async Task ReadRequestAsync_NegativeSize_ThrowsInvalidDataException()
    {
        // Arrange - negative size
        using var stream = new MemoryStream();
        var sizeBuffer = new byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(sizeBuffer, -1);
        stream.Write(sizeBuffer);
        stream.Position = 0;

        // Act & Assert
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await _handler.ReadRequestAsync(stream));
    }

    [Fact]
    public async Task ReadRequestAsync_ExcessiveSize_ThrowsInvalidDataException()
    {
        // Arrange - size > 100MB
        using var stream = new MemoryStream();
        var sizeBuffer = new byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(sizeBuffer, 200 * 1024 * 1024);
        stream.Write(sizeBuffer);
        stream.Position = 0;

        // Act & Assert
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await _handler.ReadRequestAsync(stream));
    }

    #endregion

    #region WriteResponseAsync Tests

    [Fact]
    public async Task WriteResponseAsync_ApiVersionsResponse_WritesFramedData()
    {
        // Arrange
        var response = ApiVersionsResponse.CreateDefault(1, 0);
        using var stream = new MemoryStream();

        // Act
        await _handler.WriteResponseAsync(stream, response);

        // Assert - stream should have at least 4 bytes (size prefix) + content
        Assert.True(stream.Length > 4, "Response should have size prefix and body");
        stream.Position = 0;
        var sizeBuffer = new byte[4];
        stream.Read(sizeBuffer);
        var bodySize = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);
        Assert.True(bodySize > 0, "Body size should be positive");
        Assert.Equal(bodySize, stream.Length - 4);
    }

    [Fact]
    public async Task WriteResponseAsync_NonKafkaResponse_ThrowsArgumentException()
    {
        // Arrange - a non-KafkaResponse IProtocolResponse
        var nonKafkaResponse = NSubstitute.Substitute.For<Kuestenlogik.Surgewave.Protocol.IProtocolResponse>();
        using var stream = new MemoryStream();

        // Act & Assert
        await Assert.ThrowsAnyAsync<ArgumentException>(
            async () => await _handler.WriteResponseAsync(stream, nonKafkaResponse));
    }

    #endregion
}
