using Kuestenlogik.Surgewave.Protocol.Native;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Additional tests for request and response header serialization
/// </summary>
public sealed class RequestResponseHeaderTests
{
    [Fact]
    public void RequestHeader_MaxValues_RoundTrip()
    {
        var header = new SurgewaveRequestHeader
        {
            Flags = (SurgewaveProtocolFlags)0xFF,
            RequestId = uint.MaxValue,
            OpCode = SurgewaveOpCode.Error,
            PayloadLength = int.MaxValue
        };

        var buffer = new byte[SurgewaveNativeProtocol.HeaderSize];
        header.WriteTo(buffer);
        var parsed = SurgewaveRequestHeader.ReadFrom(buffer);

        Assert.Equal(uint.MaxValue, parsed.RequestId);
        Assert.Equal(SurgewaveOpCode.Error, parsed.OpCode);
        Assert.Equal(int.MaxValue, parsed.PayloadLength);
    }

    [Fact]
    public void RequestHeader_ZeroValues_RoundTrip()
    {
        var header = new SurgewaveRequestHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = 0,
            OpCode = SurgewaveOpCode.None,
            PayloadLength = 0
        };

        var buffer = new byte[SurgewaveNativeProtocol.HeaderSize];
        header.WriteTo(buffer);
        var parsed = SurgewaveRequestHeader.ReadFrom(buffer);

        Assert.Equal(SurgewaveProtocolFlags.None, parsed.Flags);
        Assert.Equal(0u, parsed.RequestId);
        Assert.Equal(SurgewaveOpCode.None, parsed.OpCode);
        Assert.Equal(0, parsed.PayloadLength);
    }

    [Fact]
    public void RequestHeader_ReservedByte_IsIgnored()
    {
        var header = new SurgewaveRequestHeader
        {
            Flags = SurgewaveProtocolFlags.Compressed,
            RequestId = 100,
            OpCode = SurgewaveOpCode.Produce,
            PayloadLength = 256
        };

        var buffer = new byte[SurgewaveNativeProtocol.HeaderSize];
        header.WriteTo(buffer);

        // Write something to the reserved byte (index 1) - should be ignored on read
        buffer[1] = 0xAB;

        var parsed = SurgewaveRequestHeader.ReadFrom(buffer);
        Assert.Equal(SurgewaveProtocolFlags.Compressed, parsed.Flags);
        Assert.Equal(100u, parsed.RequestId);
        Assert.Equal(SurgewaveOpCode.Produce, parsed.OpCode);
        Assert.Equal(256, parsed.PayloadLength);
    }

    [Fact]
    public void ResponseHeader_MaxValues_RoundTrip()
    {
        var header = new SurgewaveResponseHeader
        {
            Flags = SurgewaveProtocolFlags.LastInBatch,
            RequestId = uint.MaxValue,
            OpCode = SurgewaveOpCode.Error,
            ErrorCode = SurgewaveErrorCode.CrossTopicTxnDisabled,
            PayloadLength = int.MaxValue
        };

        var buffer = new byte[SurgewaveResponseHeader.Size];
        header.WriteTo(buffer);
        var parsed = SurgewaveResponseHeader.ReadFrom(buffer);

        Assert.Equal(uint.MaxValue, parsed.RequestId);
        Assert.Equal(SurgewaveOpCode.Error, parsed.OpCode);
        Assert.Equal(SurgewaveErrorCode.CrossTopicTxnDisabled, parsed.ErrorCode);
        Assert.Equal(int.MaxValue, parsed.PayloadLength);
    }

    [Fact]
    public void ResponseHeader_AllFlags_Preserved()
    {
        var allFlags = SurgewaveProtocolFlags.Compressed | SurgewaveProtocolFlags.Streaming |
                       SurgewaveProtocolFlags.BatchRequest | SurgewaveProtocolFlags.NoResponse |
                       SurgewaveProtocolFlags.LastInBatch;

        var header = new SurgewaveResponseHeader
        {
            Flags = allFlags,
            RequestId = 1,
            OpCode = SurgewaveOpCode.FetchResponse,
            ErrorCode = SurgewaveErrorCode.None,
            PayloadLength = 100
        };

        var buffer = new byte[SurgewaveResponseHeader.Size];
        header.WriteTo(buffer);
        var parsed = SurgewaveResponseHeader.ReadFrom(buffer);

        Assert.Equal(allFlags, parsed.Flags);
        Assert.True(parsed.Flags.HasFlag(SurgewaveProtocolFlags.Compressed));
        Assert.True(parsed.Flags.HasFlag(SurgewaveProtocolFlags.Streaming));
        Assert.True(parsed.Flags.HasFlag(SurgewaveProtocolFlags.BatchRequest));
        Assert.True(parsed.Flags.HasFlag(SurgewaveProtocolFlags.NoResponse));
        Assert.True(parsed.Flags.HasFlag(SurgewaveProtocolFlags.LastInBatch));
    }

    [Fact]
    public void RequestHeader_Flags_FirstByte_Position()
    {
        var header = new SurgewaveRequestHeader
        {
            Flags = SurgewaveProtocolFlags.Compressed,  // value = 1
            RequestId = 0,
            OpCode = SurgewaveOpCode.None,
            PayloadLength = 0
        };

        var buffer = new byte[SurgewaveNativeProtocol.HeaderSize];
        header.WriteTo(buffer);

        Assert.Equal(1, buffer[0]); // Flags at index 0
    }

    [Fact]
    public void ResponseHeader_Size_Constant_Is14()
    {
        Assert.Equal(14, SurgewaveResponseHeader.Size);
    }

    [Fact]
    public void ProtocolConstants_MaxPayloadSize_Is100MB()
    {
        Assert.Equal(100 * 1024 * 1024, SurgewaveNativeProtocol.MaxPayloadSize);
    }

    [Theory]
    [InlineData(SurgewaveOpCode.Ping, SurgewaveErrorCode.None)]
    [InlineData(SurgewaveOpCode.Produce, SurgewaveErrorCode.MessageTooLarge)]
    [InlineData(SurgewaveOpCode.Fetch, SurgewaveErrorCode.TopicNotFound)]
    [InlineData(SurgewaveOpCode.JoinGroup, SurgewaveErrorCode.RebalanceInProgress)]
    [InlineData(SurgewaveOpCode.CreateTopic, SurgewaveErrorCode.InvalidConfig)]
    public void ResponseHeader_VariousOpCodeErrorCode_RoundTrip(SurgewaveOpCode opCode, SurgewaveErrorCode errorCode)
    {
        var header = new SurgewaveResponseHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = 42,
            OpCode = opCode,
            ErrorCode = errorCode,
            PayloadLength = 0
        };

        var buffer = new byte[SurgewaveResponseHeader.Size];
        header.WriteTo(buffer);
        var parsed = SurgewaveResponseHeader.ReadFrom(buffer);

        Assert.Equal(opCode, parsed.OpCode);
        Assert.Equal(errorCode, parsed.ErrorCode);
    }
}
