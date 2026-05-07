using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Tests for Surgewave native protocol headers (SurgewaveRequestHeader, SurgewaveResponseHeader).
/// These are core framing types used by the TCP transport.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SurgewaveProtocolHeaderTests
{
    #region SurgewaveRequestHeader Tests

    [Fact]
    public void SurgewaveRequestHeader_WriteTo_ThenReadFrom_RoundTrips()
    {
        // Arrange
        var original = new SurgewaveRequestHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = 42u,
            OpCode = SurgewaveOpCode.Produce,
            PayloadLength = 1024
        };
        var buffer = new byte[SurgewaveNativeProtocol.HeaderSize];

        // Act
        original.WriteTo(buffer);
        var decoded = SurgewaveRequestHeader.ReadFrom(buffer);

        // Assert
        Assert.Equal(original.Flags, decoded.Flags);
        Assert.Equal(original.RequestId, decoded.RequestId);
        Assert.Equal(original.OpCode, decoded.OpCode);
        Assert.Equal(original.PayloadLength, decoded.PayloadLength);
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(1u)]
    [InlineData(uint.MaxValue)]
    [InlineData(0x0000FFFFu)]
    public void SurgewaveRequestHeader_RequestId_RoundTrips(uint requestId)
    {
        // Arrange
        var header = new SurgewaveRequestHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = requestId,
            OpCode = SurgewaveOpCode.Fetch,
            PayloadLength = 0
        };
        var buffer = new byte[SurgewaveNativeProtocol.HeaderSize];

        // Act
        header.WriteTo(buffer);
        var decoded = SurgewaveRequestHeader.ReadFrom(buffer);

        // Assert
        Assert.Equal(requestId, decoded.RequestId);
    }

    [Fact]
    public void SurgewaveRequestHeader_PayloadLength_RoundTrips_LargeValue()
    {
        // Arrange
        const int largePayload = 10 * 1024 * 1024; // 10 MB
        var header = new SurgewaveRequestHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = 1u,
            OpCode = SurgewaveOpCode.ProduceBatch,
            PayloadLength = largePayload
        };
        var buffer = new byte[SurgewaveNativeProtocol.HeaderSize];

        // Act
        header.WriteTo(buffer);
        var decoded = SurgewaveRequestHeader.ReadFrom(buffer);

        // Assert
        Assert.Equal(largePayload, decoded.PayloadLength);
    }

    [Theory]
    [InlineData(SurgewaveOpCode.Produce)]
    [InlineData(SurgewaveOpCode.Fetch)]
    [InlineData(SurgewaveOpCode.Handshake)]
    [InlineData(SurgewaveOpCode.CreateTopic)]
    [InlineData(SurgewaveOpCode.CommitOffset)]
    public void SurgewaveRequestHeader_OpCode_RoundTrips(SurgewaveOpCode opCode)
    {
        // Arrange
        var header = new SurgewaveRequestHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = 1u,
            OpCode = opCode,
            PayloadLength = 0
        };
        var buffer = new byte[SurgewaveNativeProtocol.HeaderSize];

        // Act
        header.WriteTo(buffer);
        var decoded = SurgewaveRequestHeader.ReadFrom(buffer);

        // Assert
        Assert.Equal(opCode, decoded.OpCode);
    }

    [Fact]
    public void SurgewaveRequestHeader_HeaderSize_MatchesExpected()
    {
        // The header size constant must match the actual written byte count
        Assert.Equal(12, SurgewaveNativeProtocol.HeaderSize);
    }

    #endregion

    #region SurgewaveResponseHeader Tests

    [Fact]
    public void SurgewaveResponseHeader_WriteTo_ThenReadFrom_RoundTrips()
    {
        // Arrange
        var original = new SurgewaveResponseHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = 99u,
            OpCode = SurgewaveOpCode.ProduceAck,
            ErrorCode = SurgewaveErrorCode.None,
            PayloadLength = 512
        };
        var buffer = new byte[SurgewaveResponseHeader.Size];

        // Act
        original.WriteTo(buffer);
        var decoded = SurgewaveResponseHeader.ReadFrom(buffer);

        // Assert
        Assert.Equal(original.Flags, decoded.Flags);
        Assert.Equal(original.RequestId, decoded.RequestId);
        Assert.Equal(original.OpCode, decoded.OpCode);
        Assert.Equal(original.ErrorCode, decoded.ErrorCode);
        Assert.Equal(original.PayloadLength, decoded.PayloadLength);
    }

    [Theory]
    [InlineData(SurgewaveErrorCode.None)]
    [InlineData(SurgewaveErrorCode.TopicNotFound)]
    [InlineData(SurgewaveErrorCode.AuthenticationFailed)]
    [InlineData(SurgewaveErrorCode.MessageTooLarge)]
    [InlineData(SurgewaveErrorCode.Timeout)]
    public void SurgewaveResponseHeader_ErrorCode_RoundTrips(SurgewaveErrorCode errorCode)
    {
        // Arrange
        var header = new SurgewaveResponseHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = 1u,
            OpCode = SurgewaveOpCode.Error,
            ErrorCode = errorCode,
            PayloadLength = 0
        };
        var buffer = new byte[SurgewaveResponseHeader.Size];

        // Act
        header.WriteTo(buffer);
        var decoded = SurgewaveResponseHeader.ReadFrom(buffer);

        // Assert
        Assert.Equal(errorCode, decoded.ErrorCode);
    }

    [Fact]
    public void SurgewaveResponseHeader_Size_IsCorrect()
    {
        // flags(1) + reserved(1) + requestId(4) + opCode(2) + errorCode(2) + payloadLength(4) = 14
        Assert.Equal(14, SurgewaveResponseHeader.Size);
    }

    [Fact]
    public void SurgewaveResponseHeader_BigEndian_RequestId()
    {
        // Arrange - set a known request ID with distinct byte pattern
        var header = new SurgewaveResponseHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = 0x01020304u,
            OpCode = SurgewaveOpCode.None,
            ErrorCode = SurgewaveErrorCode.None,
            PayloadLength = 0
        };
        var buffer = new byte[SurgewaveResponseHeader.Size];

        // Act
        header.WriteTo(buffer);

        // Assert - big endian: bytes[2..5] = 0x01, 0x02, 0x03, 0x04
        Assert.Equal(0x01, buffer[2]);
        Assert.Equal(0x02, buffer[3]);
        Assert.Equal(0x03, buffer[4]);
        Assert.Equal(0x04, buffer[5]);
    }

    [Fact]
    public void SurgewaveResponseHeader_PayloadLength_ZeroIsValid()
    {
        // Arrange
        var header = new SurgewaveResponseHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = 1u,
            OpCode = SurgewaveOpCode.Ping,
            ErrorCode = SurgewaveErrorCode.None,
            PayloadLength = 0
        };
        var buffer = new byte[SurgewaveResponseHeader.Size];

        // Act
        header.WriteTo(buffer);
        var decoded = SurgewaveResponseHeader.ReadFrom(buffer);

        // Assert
        Assert.Equal(0, decoded.PayloadLength);
    }

    #endregion

    #region SurgewaveNativeProtocol Constants Tests

    [Fact]
    public void SurgewaveNativeProtocol_Magic_IsFourBytes()
    {
        var magic = SurgewaveNativeProtocol.Magic;
        Assert.Equal(4, magic.Length);
    }

    [Fact]
    public void SurgewaveNativeProtocol_Magic_IsSTRM()
    {
        var magic = SurgewaveNativeProtocol.Magic;
        Assert.Equal((byte)'S', magic[0]);
        Assert.Equal((byte)'T', magic[1]);
        Assert.Equal((byte)'R', magic[2]);
        Assert.Equal((byte)'M', magic[3]);
    }

    [Fact]
    public void SurgewaveNativeProtocol_Version_IsOne()
    {
        Assert.Equal(1, SurgewaveNativeProtocol.Version);
    }

    [Fact]
    public void SurgewaveNativeProtocol_MaxPayloadSize_Is100MB()
    {
        Assert.Equal(100 * 1024 * 1024, SurgewaveNativeProtocol.MaxPayloadSize);
    }

    #endregion
}
