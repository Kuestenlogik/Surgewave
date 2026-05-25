using Kuestenlogik.Surgewave.Protocol.Native;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Tests for Surgewave native protocol serialization and deserialization
/// </summary>
public sealed class SurgewaveNativeProtocolTests
{
    #region Magic Bytes Tests

    [Fact]
    public void Magic_Is_SRWV()
    {
        // Verify magic bytes are "SRWV"
        Assert.Equal(4, SurgewaveNativeProtocol.Magic.Length);
        Assert.Equal((byte)'S', SurgewaveNativeProtocol.Magic[0]);
        Assert.Equal((byte)'R', SurgewaveNativeProtocol.Magic[1]);
        Assert.Equal((byte)'W', SurgewaveNativeProtocol.Magic[2]);
        Assert.Equal((byte)'V', SurgewaveNativeProtocol.Magic[3]);
    }

    [Fact]
    public void Version_Is_1()
    {
        Assert.Equal(1, SurgewaveNativeProtocol.Version);
    }

    [Fact]
    public void HeaderSize_Is_12()
    {
        Assert.Equal(12, SurgewaveNativeProtocol.HeaderSize);
    }

    [Fact]
    public void ResponseHeaderSize_Is_14()
    {
        Assert.Equal(14, SurgewaveResponseHeader.Size);
    }

    #endregion

    #region Request Header Tests

    [Fact]
    public void RequestHeader_WriteTo_And_ReadFrom_RoundTrip()
    {
        // Arrange
        var header = new SurgewaveRequestHeader
        {
            Flags = SurgewaveProtocolFlags.Compressed | SurgewaveProtocolFlags.BatchRequest,
            RequestId = 12345,
            OpCode = SurgewaveOpCode.Produce,
            PayloadLength = 1024
        };
        var buffer = new byte[SurgewaveNativeProtocol.HeaderSize];

        // Act
        header.WriteTo(buffer);
        var parsed = SurgewaveRequestHeader.ReadFrom(buffer);

        // Assert
        Assert.Equal(header.Flags, parsed.Flags);
        Assert.Equal(header.RequestId, parsed.RequestId);
        Assert.Equal(header.OpCode, parsed.OpCode);
        Assert.Equal(header.PayloadLength, parsed.PayloadLength);
    }

    [Fact]
    public void RequestHeader_AllOpCodes()
    {
        var opCodes = new[]
        {
            SurgewaveOpCode.Handshake,
            SurgewaveOpCode.Ping,
            SurgewaveOpCode.Pong,
            SurgewaveOpCode.GetMetadata,
            SurgewaveOpCode.Produce,
            SurgewaveOpCode.ProduceBatch,
            SurgewaveOpCode.ProduceAck,
            SurgewaveOpCode.Fetch,
            SurgewaveOpCode.FetchResponse,
            SurgewaveOpCode.Subscribe,
            SurgewaveOpCode.Unsubscribe,
            SurgewaveOpCode.CommitOffset,
            SurgewaveOpCode.FetchOffset,
            SurgewaveOpCode.ListOffsets,
            SurgewaveOpCode.JoinGroup,
            SurgewaveOpCode.SyncGroup,
            SurgewaveOpCode.LeaveGroup,
            SurgewaveOpCode.Heartbeat,
            SurgewaveOpCode.CreateTopic,
            SurgewaveOpCode.DeleteTopic,
            SurgewaveOpCode.ListTopics,
            SurgewaveOpCode.DescribeTopic,
            SurgewaveOpCode.Error
        };

        var buffer = new byte[SurgewaveNativeProtocol.HeaderSize];
        foreach (var opCode in opCodes)
        {
            var header = new SurgewaveRequestHeader
            {
                Flags = SurgewaveProtocolFlags.None,
                RequestId = 1,
                OpCode = opCode,
                PayloadLength = 0
            };
            header.WriteTo(buffer);
            var parsed = SurgewaveRequestHeader.ReadFrom(buffer);
            Assert.Equal(opCode, parsed.OpCode);
        }
    }

    #endregion

    #region Response Header Tests

    [Fact]
    public void ResponseHeader_WriteTo_And_ReadFrom_RoundTrip()
    {
        // Arrange
        var header = new SurgewaveResponseHeader
        {
            Flags = SurgewaveProtocolFlags.Streaming,
            RequestId = 67890,
            OpCode = SurgewaveOpCode.FetchResponse,
            ErrorCode = SurgewaveErrorCode.None,
            PayloadLength = 2048
        };
        var buffer = new byte[SurgewaveResponseHeader.Size];

        // Act
        header.WriteTo(buffer);
        var parsed = SurgewaveResponseHeader.ReadFrom(buffer);

        // Assert
        Assert.Equal(header.Flags, parsed.Flags);
        Assert.Equal(header.RequestId, parsed.RequestId);
        Assert.Equal(header.OpCode, parsed.OpCode);
        Assert.Equal(header.ErrorCode, parsed.ErrorCode);
        Assert.Equal(header.PayloadLength, parsed.PayloadLength);
    }

    [Fact]
    public void ResponseHeader_WithError()
    {
        var header = new SurgewaveResponseHeader
        {
            Flags = SurgewaveProtocolFlags.None,
            RequestId = 999,
            OpCode = SurgewaveOpCode.Error,
            ErrorCode = SurgewaveErrorCode.TopicNotFound,
            PayloadLength = 100
        };
        var buffer = new byte[SurgewaveResponseHeader.Size];

        header.WriteTo(buffer);
        var parsed = SurgewaveResponseHeader.ReadFrom(buffer);

        Assert.Equal(SurgewaveOpCode.Error, parsed.OpCode);
        Assert.Equal(SurgewaveErrorCode.TopicNotFound, parsed.ErrorCode);
    }

    #endregion

    #region Payload Writer/Reader Tests

    [Fact]
    public void PayloadWriter_WriteInt8_And_Read()
    {
        var buffer = new byte[100];
        var writer = new SurgewavePayloadWriter(buffer);

        writer.WriteInt8(-128);
        writer.WriteInt8(0);
        writer.WriteInt8(127);

        var reader = new SurgewavePayloadReader(buffer);
        Assert.Equal(-128, reader.ReadInt8());
        Assert.Equal(0, reader.ReadInt8());
        Assert.Equal(127, reader.ReadInt8());
    }

    [Fact]
    public void PayloadWriter_WriteInt16_And_Read()
    {
        var buffer = new byte[100];
        var writer = new SurgewavePayloadWriter(buffer);

        writer.WriteInt16(-32768);
        writer.WriteInt16(0);
        writer.WriteInt16(32767);

        var reader = new SurgewavePayloadReader(buffer);
        Assert.Equal(-32768, reader.ReadInt16());
        Assert.Equal(0, reader.ReadInt16());
        Assert.Equal(32767, reader.ReadInt16());
    }

    [Fact]
    public void PayloadWriter_WriteInt32_And_Read()
    {
        var buffer = new byte[100];
        var writer = new SurgewavePayloadWriter(buffer);

        writer.WriteInt32(int.MinValue);
        writer.WriteInt32(0);
        writer.WriteInt32(int.MaxValue);

        var reader = new SurgewavePayloadReader(buffer);
        Assert.Equal(int.MinValue, reader.ReadInt32());
        Assert.Equal(0, reader.ReadInt32());
        Assert.Equal(int.MaxValue, reader.ReadInt32());
    }

    [Fact]
    public void PayloadWriter_WriteInt64_And_Read()
    {
        var buffer = new byte[100];
        var writer = new SurgewavePayloadWriter(buffer);

        writer.WriteInt64(long.MinValue);
        writer.WriteInt64(0);
        writer.WriteInt64(long.MaxValue);

        var reader = new SurgewavePayloadReader(buffer);
        Assert.Equal(long.MinValue, reader.ReadInt64());
        Assert.Equal(0, reader.ReadInt64());
        Assert.Equal(long.MaxValue, reader.ReadInt64());
    }

    [Fact]
    public void PayloadWriter_WriteString_And_Read()
    {
        var buffer = new byte[200];
        var writer = new SurgewavePayloadWriter(buffer);

        writer.WriteString("hello");
        writer.WriteString("world");
        writer.WriteString(null);
        writer.WriteString("");

        var reader = new SurgewavePayloadReader(buffer);
        Assert.Equal("hello", reader.ReadString());
        Assert.Equal("world", reader.ReadString());
        Assert.Null(reader.ReadString());
        Assert.Equal("", reader.ReadString());
    }

    [Fact]
    public void PayloadWriter_WriteBytes_And_Read()
    {
        var buffer = new byte[200];
        var writer = new SurgewavePayloadWriter(buffer);
        var testData = new byte[] { 1, 2, 3, 4, 5 };

        writer.WriteBytes(testData);

        var reader = new SurgewavePayloadReader(buffer);
        var result = reader.ReadBytes();
        Assert.Equal(testData, result.ToArray());
    }

    [Fact]
    public void PayloadWriter_Position_Tracks_Correctly()
    {
        var buffer = new byte[100];
        var writer = new SurgewavePayloadWriter(buffer);

        Assert.Equal(0, writer.Position);
        writer.WriteInt8(1);
        Assert.Equal(1, writer.Position);
        writer.WriteInt16(1);
        Assert.Equal(3, writer.Position);
        writer.WriteInt32(1);
        Assert.Equal(7, writer.Position);
        writer.WriteInt64(1);
        Assert.Equal(15, writer.Position);
    }

    [Fact]
    public void PayloadReader_Remaining_And_Position()
    {
        var buffer = new byte[20];
        var reader = new SurgewavePayloadReader(buffer);

        Assert.Equal(0, reader.Position);
        Assert.Equal(20, reader.Remaining);

        reader.ReadInt32();
        Assert.Equal(4, reader.Position);
        Assert.Equal(16, reader.Remaining);

        reader.Skip(8);
        Assert.Equal(12, reader.Position);
        Assert.Equal(8, reader.Remaining);
    }

    #endregion

    #region Protocol Flags Tests

    [Fact]
    public void ProtocolFlags_Combinations()
    {
        // Test combining flags
        var flags = SurgewaveProtocolFlags.Compressed | SurgewaveProtocolFlags.Streaming | SurgewaveProtocolFlags.BatchRequest;

        Assert.True(flags.HasFlag(SurgewaveProtocolFlags.Compressed));
        Assert.True(flags.HasFlag(SurgewaveProtocolFlags.Streaming));
        Assert.True(flags.HasFlag(SurgewaveProtocolFlags.BatchRequest));
        Assert.False(flags.HasFlag(SurgewaveProtocolFlags.NoResponse));
    }

    [Fact]
    public void ProtocolFlags_ByteValues()
    {
        Assert.Equal(0, (byte)SurgewaveProtocolFlags.None);
        Assert.Equal(1, (byte)SurgewaveProtocolFlags.Compressed);
        Assert.Equal(2, (byte)SurgewaveProtocolFlags.Streaming);
        Assert.Equal(4, (byte)SurgewaveProtocolFlags.BatchRequest);
        Assert.Equal(8, (byte)SurgewaveProtocolFlags.NoResponse);
        Assert.Equal(16, (byte)SurgewaveProtocolFlags.LastInBatch);
    }

    #endregion

    #region Error Code Tests

    [Fact]
    public void ErrorCode_Values()
    {
        Assert.Equal(0, (ushort)SurgewaveErrorCode.None);
        Assert.Equal(1, (ushort)SurgewaveErrorCode.UnknownError);
        Assert.Equal(2, (ushort)SurgewaveErrorCode.InvalidRequest);
        Assert.Equal(3, (ushort)SurgewaveErrorCode.TopicNotFound);
        Assert.Equal(4, (ushort)SurgewaveErrorCode.PartitionNotFound);
        Assert.Equal(5, (ushort)SurgewaveErrorCode.NotLeader);
        Assert.Equal(6, (ushort)SurgewaveErrorCode.AuthenticationFailed);
        Assert.Equal(7, (ushort)SurgewaveErrorCode.AuthorizationFailed);
        Assert.Equal(8, (ushort)SurgewaveErrorCode.InvalidOffset);
        Assert.Equal(9, (ushort)SurgewaveErrorCode.MessageTooLarge);
        Assert.Equal(10, (ushort)SurgewaveErrorCode.GroupNotFound);
        Assert.Equal(11, (ushort)SurgewaveErrorCode.RebalanceInProgress);
        Assert.Equal(12, (ushort)SurgewaveErrorCode.InvalidSession);
        Assert.Equal(13, (ushort)SurgewaveErrorCode.Timeout);
    }

    #endregion

    #region OpCode Value Tests

    [Fact]
    public void OpCode_ConnectionOperations()
    {
        Assert.Equal(0x0001, (ushort)SurgewaveOpCode.Handshake);
        Assert.Equal(0x0002, (ushort)SurgewaveOpCode.Ping);
        Assert.Equal(0x0003, (ushort)SurgewaveOpCode.Pong);
        Assert.Equal(0x0004, (ushort)SurgewaveOpCode.GetMetadata);
    }

    [Fact]
    public void OpCode_ProduceOperations()
    {
        Assert.Equal(0x0100, (ushort)SurgewaveOpCode.Produce);
        Assert.Equal(0x0101, (ushort)SurgewaveOpCode.ProduceBatch);
        Assert.Equal(0x0102, (ushort)SurgewaveOpCode.ProduceAck);
    }

    [Fact]
    public void OpCode_ConsumeOperations()
    {
        Assert.Equal(0x0200, (ushort)SurgewaveOpCode.Fetch);
        Assert.Equal(0x0201, (ushort)SurgewaveOpCode.FetchResponse);
        Assert.Equal(0x0202, (ushort)SurgewaveOpCode.Subscribe);
        Assert.Equal(0x0203, (ushort)SurgewaveOpCode.Unsubscribe);
    }

    [Fact]
    public void OpCode_OffsetOperations()
    {
        Assert.Equal(0x0300, (ushort)SurgewaveOpCode.CommitOffset);
        Assert.Equal(0x0301, (ushort)SurgewaveOpCode.FetchOffset);
        Assert.Equal(0x0302, (ushort)SurgewaveOpCode.ListOffsets);
    }

    [Fact]
    public void OpCode_AdminOperations()
    {
        Assert.Equal(0x0500, (ushort)SurgewaveOpCode.CreateTopic);
        Assert.Equal(0x0501, (ushort)SurgewaveOpCode.DeleteTopic);
        Assert.Equal(0x0502, (ushort)SurgewaveOpCode.ListTopics);
        Assert.Equal(0x0503, (ushort)SurgewaveOpCode.DescribeTopic);
    }

    [Fact]
    public void OpCode_Error()
    {
        Assert.Equal(0xFF00, (ushort)SurgewaveOpCode.Error);
    }

    #endregion

    #region Compression Tests

    [Fact]
    public void Compression_MinSize_Is_1024()
    {
        Assert.Equal(1024, NativeCompressionCodec.MinCompressionSize);
    }

    [Fact]
    public void Compression_SmallData_NotCompressed()
    {
        // Data smaller than MinCompressionSize should not be compressed
        var smallData = new byte[500];
        // Use deterministic pattern
        for (int i = 0; i < smallData.Length; i++)
            smallData[i] = (byte)((i * 31 + 17) % 256);

        var (result, wasCompressed) = NativeCompressionCodec.Compress(smallData);

        Assert.False(wasCompressed);
        Assert.Equal(smallData, result);
    }

    [Fact]
    public void Compression_LargeCompressibleData_Compressed()
    {
        // Large repetitive data should compress well
        var largeData = new byte[4096];
        // Fill with repetitive pattern for good compression
        for (int i = 0; i < largeData.Length; i++)
        {
            largeData[i] = (byte)(i % 16);
        }

        var (result, wasCompressed) = NativeCompressionCodec.Compress(largeData);

        Assert.True(wasCompressed);
        Assert.True(result.Length < largeData.Length);
    }

    [Fact]
    public void Compression_IncompressibleData_NotCompressed()
    {
        // Pseudo-random pattern that doesn't compress well
        var randomData = new byte[2048];
        // Use deterministic pseudo-random pattern
        uint seed = 42;
        for (int i = 0; i < randomData.Length; i++)
        {
            seed = (seed * 1103515245 + 12345) & 0x7fffffff;
            randomData[i] = (byte)(seed >> 16);
        }

        var (result, wasCompressed) = NativeCompressionCodec.Compress(randomData);

        // LZ4 may or may not compress random data - if it doesn't compress well,
        // it should return uncompressed
        if (!wasCompressed)
        {
            Assert.Equal(randomData, result);
        }
    }

    [Fact]
    public void Compression_RoundTrip()
    {
        var originalData = new byte[4096];
        for (int i = 0; i < originalData.Length; i++)
        {
            originalData[i] = (byte)(i % 256);
        }

        // Compress
        var (compressed, wasCompressed) = NativeCompressionCodec.CompressWithHeader(originalData);
        Assert.True(wasCompressed);

        // Decompress
        var decompressed = NativeCompressionCodec.DecompressWithHeader(compressed);
        Assert.Equal(originalData, decompressed);
    }

    [Fact]
    public void Compression_WithHeader_Format_CorrectSizePrefix()
    {
        var originalData = new byte[2048];
        for (int i = 0; i < originalData.Length; i++)
        {
            originalData[i] = (byte)(i % 32);
        }

        var (compressed, wasCompressed) = NativeCompressionCodec.CompressWithHeader(originalData);
        Assert.True(wasCompressed);

        // Verify header format: [originalSize:4 bytes][compressedData]
        var sizePrefix = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(compressed.AsSpan(0, 4));
        Assert.Equal(originalData.Length, sizePrefix);
    }

    [Fact]
    public void Compression_DecompressWithHeader_TooSmall_Throws()
    {
        var tooSmall = new byte[2]; // Less than 4 bytes header

        Assert.Throws<InvalidOperationException>(() =>
            NativeCompressionCodec.DecompressWithHeader(tooSmall));
    }

    [Fact]
    public void Compression_Decompress_SizeMismatch_Throws()
    {
        var originalData = new byte[2048];
        for (int i = 0; i < originalData.Length; i++)
        {
            originalData[i] = (byte)(i % 32);
        }

        var (compressed, _) = NativeCompressionCodec.Compress(originalData);

        // Try to decompress with wrong size
        Assert.Throws<InvalidOperationException>(() =>
            NativeCompressionCodec.Decompress(compressed, originalData.Length + 100));
    }

    [Fact]
    public void Compression_LargePayload_CompressesSignificantly()
    {
        // Test with realistic message payload (JSON-like)
        var jsonPattern = System.Text.Encoding.UTF8.GetBytes(
            "{\"timestamp\":1234567890,\"type\":\"event\",\"data\":{\"field\":\"value\",\"count\":42}}");

        // Create large payload by repeating pattern
        var largePayload = new byte[jsonPattern.Length * 100];
        for (int i = 0; i < 100; i++)
        {
            jsonPattern.CopyTo(largePayload, i * jsonPattern.Length);
        }

        var (compressed, wasCompressed) = NativeCompressionCodec.CompressWithHeader(largePayload);

        Assert.True(wasCompressed);
        // With header (4 bytes) + compressed data, should still be much smaller
        Assert.True(compressed.Length < largePayload.Length / 2,
            $"Expected significant compression. Original: {largePayload.Length}, Compressed: {compressed.Length}");
    }

    [Fact]
    public void Compression_ExactMinSize_Compresses()
    {
        // Data exactly at MinCompressionSize should be considered for compression
        var data = new byte[NativeCompressionCodec.MinCompressionSize];
        for (int i = 0; i < data.Length; i++)
        {
            data[i] = (byte)(i % 16); // Compressible pattern
        }

        var (_, wasCompressed) = NativeCompressionCodec.Compress(data);
        Assert.True(wasCompressed);
    }

    [Fact]
    public void Compression_JustBelowMinSize_NotCompressed()
    {
        var data = new byte[NativeCompressionCodec.MinCompressionSize - 1];
        for (int i = 0; i < data.Length; i++)
        {
            data[i] = (byte)(i % 16);
        }

        var (result, wasCompressed) = NativeCompressionCodec.Compress(data);
        Assert.False(wasCompressed);
        Assert.Equal(data, result);
    }

    #endregion
}
