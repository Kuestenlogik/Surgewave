using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

[Trait("Category", TestCategories.Unit)]
public sealed class NativeRequestFrameWriterTests
{
    private sealed class CountingStream : MemoryStream
    {
        public int WriteCalls { get; private set; }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            WriteCalls++;
            return base.WriteAsync(buffer, cancellationToken);
        }
    }

    private static SurgewaveRequestHeader MakeHeader(int payloadLength) => new()
    {
        Flags = SurgewaveProtocolFlags.None,
        RequestId = 42u,
        OpCode = SurgewaveOpCode.Produce,
        PayloadLength = payloadLength
    };

    [Theory]
    [InlineData(1)]
    [InlineData(4096)]
    [InlineData(NativeRequestFrameWriter.MaxCoalescedPayloadBytes)] // boundary: still coalesced
    public async Task SmallPayload_CoalescesIntoSingleWrite(int payloadLength)
    {
        var payload = new byte[payloadLength];
        Random.Shared.NextBytes(payload);
        using var stream = new CountingStream();
        var scratch = new byte[SurgewaveNativeProtocol.HeaderSize];

        await NativeRequestFrameWriter.WriteAsync(stream, MakeHeader(payloadLength), payload, scratch, CancellationToken.None);

        Assert.Equal(1, stream.WriteCalls);
        var written = stream.ToArray();
        var expectedHeader = new byte[SurgewaveNativeProtocol.HeaderSize];
        MakeHeader(payloadLength).WriteTo(expectedHeader);
        Assert.Equal(expectedHeader, written[..SurgewaveNativeProtocol.HeaderSize]);
        Assert.Equal(payload, written[SurgewaveNativeProtocol.HeaderSize..]);
    }

    [Fact]
    public async Task LargePayload_UsesTwoWrites_SameBytesOnWire()
    {
        const int payloadLength = NativeRequestFrameWriter.MaxCoalescedPayloadBytes + 1;
        var payload = new byte[payloadLength];
        Random.Shared.NextBytes(payload);
        using var stream = new CountingStream();
        var scratch = new byte[SurgewaveNativeProtocol.HeaderSize];

        await NativeRequestFrameWriter.WriteAsync(stream, MakeHeader(payloadLength), payload, scratch, CancellationToken.None);

        Assert.Equal(2, stream.WriteCalls);
        var written = stream.ToArray();
        var expectedHeader = new byte[SurgewaveNativeProtocol.HeaderSize];
        MakeHeader(payloadLength).WriteTo(expectedHeader);
        Assert.Equal(expectedHeader, written[..SurgewaveNativeProtocol.HeaderSize]);
        Assert.Equal(payload, written[SurgewaveNativeProtocol.HeaderSize..]);
    }

    [Fact]
    public async Task EmptyPayload_WritesHeaderOnly()
    {
        using var stream = new CountingStream();
        var scratch = new byte[SurgewaveNativeProtocol.HeaderSize];

        await NativeRequestFrameWriter.WriteAsync(stream, MakeHeader(0), ReadOnlyMemory<byte>.Empty, scratch, CancellationToken.None);

        Assert.Equal(1, stream.WriteCalls);
        Assert.Equal(SurgewaveNativeProtocol.HeaderSize, stream.Length);
    }
}
