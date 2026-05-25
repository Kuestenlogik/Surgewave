using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for SASL Handshake and SASL Authenticate request/response types.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SaslRequestResponseTests
{
    private readonly KafkaProtocolHandler _handler = new();

    #region SaslHandshakeRequest Tests

    [Fact]
    public void SaslHandshakeRequest_Parse_V0_Plain()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(17);       // ApiKey = SaslHandshake
        writer.WriteInt16(0);        // ApiVersion = 0
        writer.WriteInt32(1);        // CorrelationId
        writer.WriteString("client");
        writer.WriteString("PLAIN"); // Mechanism

        // Act
        var request = (SaslHandshakeRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.Equal("PLAIN", request.Mechanism);
        Assert.Equal(1, request.CorrelationId);
        Assert.Equal(0, request.ApiVersion);
    }

    [Fact]
    public void SaslHandshakeRequest_Parse_V1_ScramSha256()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(17);              // ApiKey = SaslHandshake
        writer.WriteInt16(1);              // ApiVersion = 1
        writer.WriteInt32(7);              // CorrelationId
        writer.WriteString("client");
        writer.WriteString("SCRAM-SHA-256"); // Mechanism

        // Act
        var request = (SaslHandshakeRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.Equal("SCRAM-SHA-256", request.Mechanism);
    }

    [Fact]
    public void SaslHandshakeResponse_CreateSuccess_ReturnsCorrectResponse()
    {
        // Act
        var response = SaslHandshakeResponse.CreateSuccess(
            correlationId: 5,
            apiVersion: 0,
            enabledMechanisms: new[] { "PLAIN", "SCRAM-SHA-256" });

        // Assert
        Assert.Equal(ErrorCode.None, response.ErrorCode);
        Assert.Equal(5, response.CorrelationId);
        Assert.Equal(2, response.EnabledMechanisms.Length);
        Assert.Contains("PLAIN", response.EnabledMechanisms);
        Assert.Contains("SCRAM-SHA-256", response.EnabledMechanisms);
    }

    [Fact]
    public void SaslHandshakeResponse_CreateError_ReturnsErrorResponse()
    {
        // Act
        var response = SaslHandshakeResponse.CreateError(
            correlationId: 3,
            apiVersion: 0,
            errorCode: ErrorCode.UnsupportedSaslMechanism,
            enabledMechanisms: new[] { "PLAIN" });

        // Assert
        Assert.Equal(ErrorCode.UnsupportedSaslMechanism, response.ErrorCode);
        Assert.Equal(3, response.CorrelationId);
    }

    [Fact]
    public void SaslHandshakeResponse_WriteTo_ContainsCorrelationId()
    {
        // Arrange
        var response = SaslHandshakeResponse.CreateSuccess(42, 0, new[] { "PLAIN" });

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert - first 4 bytes are CorrelationId
        var correlationId = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(bytes);
        Assert.Equal(42, correlationId);
    }

    [Fact]
    public void SaslHandshakeResponse_WriteTo_ContainsErrorCode()
    {
        // Arrange
        var response = SaslHandshakeResponse.CreateError(1, 0, ErrorCode.UnsupportedSaslMechanism, new[] { "PLAIN" });

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert - bytes[4..5] = error code (big-endian int16)
        var errorCode = (ErrorCode)System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(bytes.AsSpan(4));
        Assert.Equal(ErrorCode.UnsupportedSaslMechanism, errorCode);
    }

    [Fact]
    public void SaslHandshakeRequest_WriteTo_ContainsMechanism()
    {
        // Arrange
        var request = new SaslHandshakeRequest
        {
            ApiKey = ApiKey.SaslHandshake,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "client",
            Mechanism = "GSSAPI"
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);
        var bytes = writer.ToArray();
        var asText = System.Text.Encoding.UTF8.GetString(bytes);

        // Assert
        Assert.NotEmpty(bytes);
        Assert.Contains("GSSAPI", asText);
    }

    #endregion

    #region SaslAuthenticateRequest Tests

    [Fact]
    public void SaslAuthenticateRequest_Parse_V0_ReturnsRequest()
    {
        // Arrange - SaslAuthenticate v0: just auth bytes
        var authBytes = new byte[] { 0x00, 0x41, 0x42, 0x43 }; // \0ABC

        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(36);        // ApiKey = SaslAuthenticate
        writer.WriteInt16(0);         // ApiVersion = 0
        writer.WriteInt32(2);         // CorrelationId
        writer.WriteString("client");
        writer.WriteBytes(authBytes);

        // Act
        var request = _handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.NotNull(request);
        Assert.Equal(2, request.CorrelationId);
        Assert.Equal(ApiKey.SaslAuthenticate, ((KafkaRequest)request).ApiKey);
    }

    #endregion
}
