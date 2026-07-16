using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Pins the byte-order contract of <see cref="SurgewaveBufferExtensions"/>: values are
/// encoded big-endian (network byte order) at exactly the requested offset, round-trip
/// across the full value range, and out-of-range offsets fail fast instead of silently
/// touching adjacent memory.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SurgewaveBufferExtensionsTests
{
    [Fact]
    public void WriteInt32BigEndian_EncodesMostSignificantByteFirstAtOffset()
    {
        using ISurgewaveWritableBuffer buffer = new PooledSurgewaveBuffer(8);
        buffer.Span.Clear();

        buffer.WriteInt32BigEndian(2, 0x01020304);

        byte[] expected = [0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x00, 0x00];
        Assert.Equal(expected, buffer.ToArray());
    }

    [Fact]
    public void WriteInt64BigEndian_EncodesMostSignificantByteFirst()
    {
        using ISurgewaveWritableBuffer buffer = new PooledSurgewaveBuffer(8);

        buffer.WriteInt64BigEndian(0, 0x0102030405060708L);

        byte[] expected = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        Assert.Equal(expected, buffer.ToArray());
    }

    [Fact]
    public void ReadInt32BigEndian_DecodesMostSignificantByteFirst()
    {
        byte[] data = [0x80, 0x00, 0x00, 0x01];
        using var buffer = DefaultSurgewaveBufferPool.Shared.RentAndCopy(data);

        Assert.Equal(unchecked((int)0x80000001), buffer.ReadInt32BigEndian(0));
    }

    [Fact]
    public void ReadInt64BigEndian_DecodesSignBitAndMagnitude()
    {
        byte[] data = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A];
        using var buffer = DefaultSurgewaveBufferPool.Shared.RentAndCopy(data);

        Assert.Equal(-1L, buffer.ReadInt64BigEndian(0));
        Assert.Equal(42L, buffer.ReadInt64BigEndian(8));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(42)]
    [InlineData(0x01020304)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    public void Int32_RoundTripsAtAlignedAndUnalignedOffsets(int value)
    {
        using ISurgewaveWritableBuffer buffer = new PooledSurgewaveBuffer(12);
        int[] offsets = [0, 3, 8];

        foreach (var offset in offsets)
        {
            buffer.WriteInt32BigEndian(offset, value);
            Assert.Equal(value, buffer.ReadInt32BigEndian(offset));
        }
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(0x0102030405060708L)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void Int64_RoundTripsAtAlignedAndUnalignedOffsets(long value)
    {
        using ISurgewaveWritableBuffer buffer = new PooledSurgewaveBuffer(16);
        int[] offsets = [0, 5, 8];

        foreach (var offset in offsets)
        {
            buffer.WriteInt64BigEndian(offset, value);
            Assert.Equal(value, buffer.ReadInt64BigEndian(offset));
        }
    }

    [Fact]
    public void AdjacentInt32Writes_DoNotOverlap()
    {
        using ISurgewaveWritableBuffer buffer = new PooledSurgewaveBuffer(8);

        buffer.WriteInt32BigEndian(0, 0x11223344);
        buffer.WriteInt32BigEndian(4, unchecked((int)0xAABBCCDD));

        Assert.Equal(0x11223344, buffer.ReadInt32BigEndian(0));
        Assert.Equal(unchecked((int)0xAABBCCDD), buffer.ReadInt32BigEndian(4));
    }

    [Fact]
    public void ReadInt32BigEndian_OffsetTooCloseToEnd_Throws()
    {
        byte[] data = [1, 2, 3, 4];
        using var buffer = DefaultSurgewaveBufferPool.Shared.RentAndCopy(data);

        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = buffer.ReadInt32BigEndian(1); });
    }

    [Fact]
    public void ReadInt64BigEndian_NegativeOffset_Throws()
    {
        byte[] data = [1, 2, 3, 4, 5, 6, 7, 8];
        using var buffer = DefaultSurgewaveBufferPool.Shared.RentAndCopy(data);

        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = buffer.ReadInt64BigEndian(-1); });
    }

    [Fact]
    public void WriteInt64BigEndian_OffsetTooCloseToEnd_Throws()
    {
        using ISurgewaveWritableBuffer buffer = new PooledSurgewaveBuffer(8);

        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.WriteInt64BigEndian(1, 0L));
    }

    [Fact]
    public void WriteInt32BigEndian_NegativeOffset_Throws()
    {
        using ISurgewaveWritableBuffer buffer = new PooledSurgewaveBuffer(8);

        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.WriteInt32BigEndian(-1, 7));
    }
}
