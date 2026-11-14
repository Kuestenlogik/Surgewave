using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// A payload that does not hold what is read from it is said to be that (#290), by an exception of the reader's own
/// and not by whatever the runtime throws at the end of a buffer.
/// </summary>
public sealed class PayloadReaderBoundsTests
{
    [Fact]
    public void AReadPastTheEndIsAPayloadException()
    {
        Assert.Throws<SurgewavePayloadException>(() => new SurgewavePayloadReader([]).ReadUInt8());
        Assert.Throws<SurgewavePayloadException>(() => new SurgewavePayloadReader([]).ReadBoolean());
        Assert.Throws<SurgewavePayloadException>(() => new SurgewavePayloadReader([1]).ReadInt16());
        Assert.Throws<SurgewavePayloadException>(() => new SurgewavePayloadReader([1, 2, 3]).ReadInt32());
        Assert.Throws<SurgewavePayloadException>(() => new SurgewavePayloadReader([1, 2, 3, 4, 5, 6, 7]).ReadInt64());
        Assert.Throws<SurgewavePayloadException>(() => new SurgewavePayloadReader([1, 2]).Skip(3));
        Assert.Throws<SurgewavePayloadException>(() => new SurgewavePayloadReader([1, 2]).ReadRaw(3));
    }

    [Fact]
    public void AStringOrBytesLongerThanThePayloadIsAPayloadException()
    {
        // A string of five bytes, of which two are there.
        Assert.Throws<SurgewavePayloadException>(() => new SurgewavePayloadReader([0, 5, (byte)'a', (byte)'b']).ReadString());
        // Bytes of a length of 16, of which one is there.
        Assert.Throws<SurgewavePayloadException>(() => new SurgewavePayloadReader([0, 0, 0, 16, 1]).ReadBytes());
    }

    [Fact]
    public void ANegativeLengthIsAPayloadException()
    {
        Assert.Throws<SurgewavePayloadException>(() => new SurgewavePayloadReader([0xFF, 0xFF, 0xFF, 0xFE, 1]).ReadBytes());
        Assert.Throws<SurgewavePayloadException>(() => new SurgewavePayloadReader([1, 2]).ReadRaw(-1));
    }

    [Fact]
    public void WhatIsThereIsReadAndThePositionMovesBehindIt()
    {
        var reader = new SurgewavePayloadReader([0, 2, (byte)'o', (byte)'k', 0, 0, 0, 7, 1, 0xFF, 0xFF]);

        Assert.Equal("ok", reader.ReadString());
        Assert.Equal(7, reader.ReadInt32());
        Assert.True(reader.ReadBoolean());
        Assert.Null(reader.ReadString());
        Assert.Equal(0, reader.Remaining);
        Assert.Equal(11, reader.Position);
    }
}
