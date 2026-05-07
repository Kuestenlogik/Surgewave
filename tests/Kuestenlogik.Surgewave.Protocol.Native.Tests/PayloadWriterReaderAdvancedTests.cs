using Kuestenlogik.Surgewave.Protocol.Native;
using System.Text;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Advanced tests for SurgewavePayloadWriter and SurgewavePayloadReader covering edge cases
/// </summary>
public sealed class PayloadWriterReaderAdvancedTests
{
    [Fact]
    public void Writer_WriteUInt8_And_Reader_ReadUInt8()
    {
        var buffer = new byte[10];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteUInt8(0);
        writer.WriteUInt8(128);
        writer.WriteUInt8(255);

        var reader = new SurgewavePayloadReader(buffer);
        Assert.Equal((byte)0, reader.ReadUInt8());
        Assert.Equal((byte)128, reader.ReadUInt8());
        Assert.Equal((byte)255, reader.ReadUInt8());
    }

    [Fact]
    public void Writer_WriteUInt16_And_Reader_ReadUInt16()
    {
        var buffer = new byte[10];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteUInt16(0);
        writer.WriteUInt16(32768);
        writer.WriteUInt16(ushort.MaxValue);

        var reader = new SurgewavePayloadReader(buffer);
        Assert.Equal((ushort)0, reader.ReadUInt16());
        Assert.Equal((ushort)32768, reader.ReadUInt16());
        Assert.Equal(ushort.MaxValue, reader.ReadUInt16());
    }

    [Fact]
    public void Writer_WriteUInt32_And_Reader_ReadUInt32()
    {
        var buffer = new byte[12];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteUInt32(0);
        writer.WriteUInt32(uint.MaxValue / 2);
        writer.WriteUInt32(uint.MaxValue);

        var reader = new SurgewavePayloadReader(buffer);
        Assert.Equal(0u, reader.ReadUInt32());
        Assert.Equal(uint.MaxValue / 2, reader.ReadUInt32());
        Assert.Equal(uint.MaxValue, reader.ReadUInt32());
    }

    [Fact]
    public void Writer_WriteUInt64_And_Reader_ReadUInt64()
    {
        var buffer = new byte[24];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteUInt64(0);
        writer.WriteUInt64(ulong.MaxValue / 2);
        writer.WriteUInt64(ulong.MaxValue);

        var reader = new SurgewavePayloadReader(buffer);
        Assert.Equal(0ul, reader.ReadUInt64());
        Assert.Equal(ulong.MaxValue / 2, reader.ReadUInt64());
        Assert.Equal(ulong.MaxValue, reader.ReadUInt64());
    }

    [Fact]
    public void Writer_WriteBoolean_And_Reader_ReadBoolean()
    {
        var buffer = new byte[5];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteBoolean(true);
        writer.WriteBoolean(false);
        writer.WriteBoolean(true);

        var reader = new SurgewavePayloadReader(buffer);
        Assert.True(reader.ReadBoolean());
        Assert.False(reader.ReadBoolean());
        Assert.True(reader.ReadBoolean());
    }

    [Fact]
    public void Writer_WriteNullableString_Null_ReadsBack_Null()
    {
        var buffer = new byte[50];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteNullableString(null);

        var reader = new SurgewavePayloadReader(buffer);
        var result = reader.ReadNullableString();
        Assert.Null(result);
    }

    [Fact]
    public void Writer_WriteNullableString_Value_ReadsBack_Correctly()
    {
        var buffer = new byte[50];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteNullableString("hello");

        var reader = new SurgewavePayloadReader(buffer);
        var result = reader.ReadNullableString();
        Assert.Equal("hello", result);
    }

    [Fact]
    public void Writer_WriteNullableString_EmptyString_ReadsBack_Empty()
    {
        var buffer = new byte[50];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteNullableString("");

        var reader = new SurgewavePayloadReader(buffer);
        var result = reader.ReadNullableString();
        Assert.Equal("", result);
    }

    [Fact]
    public void Writer_WriteRaw_And_Reader_ReadRaw()
    {
        var buffer = new byte[20];
        var data = new byte[] { 10, 20, 30, 40, 50 };
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteRaw(data);

        var reader = new SurgewavePayloadReader(buffer);
        var result = reader.ReadRaw(5);
        Assert.Equal(data, result.ToArray());
    }

    [Fact]
    public void Writer_Advance_UpdatesPosition()
    {
        var buffer = new byte[20];
        var writer = new SurgewavePayloadWriter(buffer);
        Assert.Equal(0, writer.Position);
        writer.Advance(5);
        Assert.Equal(5, writer.Position);
        writer.Advance(3);
        Assert.Equal(8, writer.Position);
    }

    [Fact]
    public void Writer_Written_ReturnsWrittenBytesOnly()
    {
        var buffer = new byte[20];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteInt32(42);
        var written = writer.Written;
        Assert.Equal(4, written.Length);
    }

    [Fact]
    public void Reader_Skip_MovesPosition()
    {
        var buffer = new byte[20];
        var reader = new SurgewavePayloadReader(buffer);
        Assert.Equal(0, reader.Position);
        reader.Skip(5);
        Assert.Equal(5, reader.Position);
        Assert.Equal(15, reader.Remaining);
    }

    [Fact]
    public void Writer_WriteString_Unicode_RoundTrips()
    {
        var buffer = new byte[200];
        var text = "こんにちは世界"; // "Hello World" in Japanese
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteString(text);

        var reader = new SurgewavePayloadReader(buffer);
        var result = reader.ReadString();
        Assert.Equal(text, result);
    }

    [Fact]
    public void Writer_WriteString_NullEncoded_AsNegativeOne()
    {
        var buffer = new byte[10];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteString(null);

        // Null string encoded as int16 = -1 (big-endian: 0xFF, 0xFF)
        Assert.Equal(0xFF, buffer[0]);
        Assert.Equal(0xFF, buffer[1]);
    }

    [Fact]
    public void Writer_WriteBytes_EmptySpan_WritesZeroLength()
    {
        var buffer = new byte[20];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteBytes(ReadOnlySpan<byte>.Empty);

        var reader = new SurgewavePayloadReader(buffer);
        var result = reader.ReadBytes();
        Assert.Equal(0, result.Length);
    }

    [Fact]
    public void Writer_BigEndian_Int32_ByteOrder()
    {
        var buffer = new byte[4];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteInt32(0x01020304);

        // Big-endian: most significant byte first
        Assert.Equal(0x01, buffer[0]);
        Assert.Equal(0x02, buffer[1]);
        Assert.Equal(0x03, buffer[2]);
        Assert.Equal(0x04, buffer[3]);
    }

    [Fact]
    public void Writer_BigEndian_Int16_ByteOrder()
    {
        var buffer = new byte[2];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteInt16(0x0102);

        Assert.Equal(0x01, buffer[0]);
        Assert.Equal(0x02, buffer[1]);
    }

    [Fact]
    public void Writer_BigEndian_Int64_ByteOrder()
    {
        var buffer = new byte[8];
        var writer = new SurgewavePayloadWriter(buffer);
        writer.WriteInt64(0x0102030405060708L);

        Assert.Equal(0x01, buffer[0]);
        Assert.Equal(0x02, buffer[1]);
        Assert.Equal(0x03, buffer[2]);
        Assert.Equal(0x04, buffer[3]);
        Assert.Equal(0x05, buffer[4]);
        Assert.Equal(0x06, buffer[5]);
        Assert.Equal(0x07, buffer[6]);
        Assert.Equal(0x08, buffer[7]);
    }

    [Fact]
    public void WriterReader_MixedTypes_RoundTrip()
    {
        var buffer = new byte[200];
        var writer = new SurgewavePayloadWriter(buffer);

        writer.WriteBoolean(true);
        writer.WriteInt8(-42);
        writer.WriteUInt8(200);
        writer.WriteInt16(-1000);
        writer.WriteUInt16(60000);
        writer.WriteInt32(-100000);
        writer.WriteUInt32(3000000000u);
        writer.WriteInt64(-9999999999L);
        writer.WriteUInt64(18000000000000ul);
        writer.WriteString("test-string");
        writer.WriteNullableString(null);
        writer.WriteNullableString("nullable");

        var reader = new SurgewavePayloadReader(buffer);
        Assert.True(reader.ReadBoolean());
        Assert.Equal(-42, reader.ReadInt8());
        Assert.Equal((byte)200, reader.ReadUInt8());
        Assert.Equal(-1000, reader.ReadInt16());
        Assert.Equal((ushort)60000, reader.ReadUInt16());
        Assert.Equal(-100000, reader.ReadInt32());
        Assert.Equal(3000000000u, reader.ReadUInt32());
        Assert.Equal(-9999999999L, reader.ReadInt64());
        Assert.Equal(18000000000000ul, reader.ReadUInt64());
        Assert.Equal("test-string", reader.ReadString());
        Assert.Null(reader.ReadNullableString());
        Assert.Equal("nullable", reader.ReadNullableString());
    }
}
