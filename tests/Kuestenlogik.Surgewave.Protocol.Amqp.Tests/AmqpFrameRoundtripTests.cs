using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Protocol.Amqp;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Amqp.Tests;

/// <summary>
/// Tests for AmqpFrameReader / AmqpFrameWriter round-trip.
/// </summary>
public sealed class AmqpFrameRoundtripTests
{
    private static (AmqpFrameReader Reader, AmqpFrameWriter Writer, MemoryStream Stream) CreatePair()
    {
        var ms = new MemoryStream();
        var writer = new AmqpFrameWriter(ms);
        // We'll rewind before reading
        return (new AmqpFrameReader(ms, maxFrameSize: 131_072), writer, ms);
    }

    private static async Task<AmqpFrame> WriteAndReadAsync(
        AmqpFrameWriter writer, AmqpFrameReader reader, MemoryStream ms,
        byte type, ushort channel, byte[] payload)
    {
        await writer.WriteFrameAsync(type, channel, payload);
        ms.Position = 0;
        return (await reader.ReadFrameAsync())!;
    }

    [Fact]
    public async Task MethodFrame_RoundTrips()
    {
        var (reader, writer, ms) = CreatePair();
        var payload = new byte[] { 0x00, 0x0A, 0x00, 0x0A, 0x01 };

        var frame = await WriteAndReadAsync(writer, reader, ms, AmqpFrameType.Method, 0, payload);

        Assert.Equal(AmqpFrameType.Method, frame.Type);
        Assert.Equal(0, frame.Channel);
        Assert.Equal(payload, frame.Payload);
    }

    [Fact]
    public async Task HeartbeatFrame_HasEmptyPayload()
    {
        var ms = new MemoryStream();
        var writer = new AmqpFrameWriter(ms);
        await writer.WriteHeartbeatAsync();
        ms.Position = 0;

        var reader = new AmqpFrameReader(ms, 131_072);
        var frame = await reader.ReadFrameAsync();

        Assert.NotNull(frame);
        Assert.Equal(AmqpFrameType.Heartbeat, frame.Type);
        Assert.Equal(0, frame.Channel);
        Assert.Empty(frame.Payload);
    }

    [Fact]
    public async Task BodyFrame_RoundTrips_WithChannelNumber()
    {
        var ms = new MemoryStream();
        var writer = new AmqpFrameWriter(ms);
        var body = System.Text.Encoding.UTF8.GetBytes("Hello Surgewave!");
        await writer.WriteFrameAsync(AmqpFrameType.Body, 3, body);
        ms.Position = 0;

        var reader = new AmqpFrameReader(ms, 131_072);
        var frame = await reader.ReadFrameAsync();

        Assert.NotNull(frame);
        Assert.Equal(AmqpFrameType.Body, frame.Type);
        Assert.Equal(3, frame.Channel);
        Assert.Equal(body, frame.Payload);
    }

    [Fact]
    public async Task MultipleFrames_ReadInOrder()
    {
        var ms = new MemoryStream();
        var writer = new AmqpFrameWriter(ms);
        var reader = new AmqpFrameReader(ms, 131_072);

        var payload1 = new byte[] { 1, 2, 3 };
        var payload2 = new byte[] { 4, 5, 6, 7 };

        await writer.WriteFrameAsync(AmqpFrameType.Method, 1, payload1);
        await writer.WriteFrameAsync(AmqpFrameType.Body, 2, payload2);
        ms.Position = 0;

        var frame1 = await reader.ReadFrameAsync();
        var frame2 = await reader.ReadFrameAsync();

        Assert.Equal(AmqpFrameType.Method, frame1!.Type);
        Assert.Equal(payload1, frame1.Payload);
        Assert.Equal(AmqpFrameType.Body, frame2!.Type);
        Assert.Equal(payload2, frame2.Payload);
    }

    [Fact]
    public async Task ReadFrame_EndOfStream_ReturnsNull()
    {
        var ms = new MemoryStream();
        var reader = new AmqpFrameReader(ms, 131_072);
        var frame = await reader.ReadFrameAsync();
        Assert.Null(frame);
    }

    [Fact]
    public async Task ConnectionStart_WritesValidMethodFrame()
    {
        var ms = new MemoryStream();
        var writer = new AmqpFrameWriter(ms);
        await writer.WriteConnectionStartAsync(0);
        ms.Position = 0;

        var reader = new AmqpFrameReader(ms, 131_072);
        var frame = await reader.ReadFrameAsync();

        Assert.NotNull(frame);
        Assert.Equal(AmqpFrameType.Method, frame.Type);
        // class-id = 10 (Connection), method-id = 10 (Start)
        Assert.Equal(10, BinaryPrimitives.ReadUInt16BigEndian(frame.Payload.AsSpan(0, 2)));
        Assert.Equal(10, BinaryPrimitives.ReadUInt16BigEndian(frame.Payload.AsSpan(2, 2)));
    }

    [Fact]
    public async Task ConnectionTune_ContainsCorrectValues()
    {
        var ms = new MemoryStream();
        var writer = new AmqpFrameWriter(ms);
        await writer.WriteConnectionTuneAsync(0, maxChannels: 256, maxFrameSize: 131072, heartbeat: 60);
        ms.Position = 0;

        var reader = new AmqpFrameReader(ms, 200_000);
        var frame = await reader.ReadFrameAsync();

        Assert.NotNull(frame);
        // class=10 method=30 (Tune)
        Assert.Equal(10, BinaryPrimitives.ReadUInt16BigEndian(frame.Payload.AsSpan(0, 2)));
        Assert.Equal(30, BinaryPrimitives.ReadUInt16BigEndian(frame.Payload.AsSpan(2, 2)));
        Assert.Equal(256, BinaryPrimitives.ReadUInt16BigEndian(frame.Payload.AsSpan(4, 2)));
    }
}
