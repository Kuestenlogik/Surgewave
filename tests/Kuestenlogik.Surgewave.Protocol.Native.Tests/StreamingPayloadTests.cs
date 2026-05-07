using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Streaming;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Tests for streaming payload serialization/deserialization roundtrips.
/// Covers SubscribePayload, SubscribeResponsePayload, UnsubscribePayload,
/// StreamRecordPayload, and StreamAckPayload.
/// </summary>
public sealed class StreamingPayloadTests
{
    // ── SubscribePayload ──────────────────────────────────────────────────────

    [Fact]
    public void Subscribe_RoundTrip_BasicFields()
    {
        var payload = new SubscribePayload
        {
            SubscriptionId = "sub-001",
            Topic = "orders",
            Partitions =
            [
                new PartitionOffset(0, 42L),
                new PartitionOffset(1, 42L),
                new PartitionOffset(2, 42L)
            ],
            MaxBytesPerPush = 512 * 1024
        };

        var buffer = new byte[payload.EstimateSize() + 32];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = SubscribePayload.Read(ref reader);

        Assert.Equal("sub-001", parsed.SubscriptionId);
        Assert.Equal("orders", parsed.Topic);
        Assert.Equal(3, parsed.Partitions.Length);
        Assert.Equal(0, parsed.Partitions[0].Partition);
        Assert.Equal(42L, parsed.Partitions[0].StartOffset);
        Assert.Equal(512 * 1024, parsed.MaxBytesPerPush);
    }

    [Fact]
    public void Subscribe_RoundTrip_EmptyPartitions()
    {
        var payload = new SubscribePayload
        {
            SubscriptionId = Guid.NewGuid().ToString("N"),
            Topic = "events",
            Partitions = [],
            MaxBytesPerPush = 1024 * 1024
        };

        var buffer = new byte[payload.EstimateSize() + 32];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = SubscribePayload.Read(ref reader);

        Assert.Empty(parsed.Partitions);
    }

    [Fact]
    public void Subscribe_RoundTrip_NegativeStartOffsets()
    {
        foreach (var startOffset in new long[] { -1L, -2L, 0L, long.MaxValue })
        {
            var payload = new SubscribePayload
            {
                SubscriptionId = "sub-x",
                Topic = "t",
                Partitions = [new PartitionOffset(0, startOffset)],
                MaxBytesPerPush = 64
            };

            var buffer = new byte[payload.EstimateSize() + 16];
            var writer = new SurgewavePayloadWriter(buffer);
            payload.Write(ref writer);

            var reader = new SurgewavePayloadReader(buffer);
            var parsed = SubscribePayload.Read(ref reader);

            Assert.Equal(startOffset, parsed.Partitions[0].StartOffset);
        }
    }

    [Fact]
    public void Subscribe_EstimateSize_IsNonNegative()
    {
        var payload = new SubscribePayload
        {
            SubscriptionId = "abc",
            Topic = "xyz",
            Partitions =
            [
                new PartitionOffset(1, 0),
                new PartitionOffset(2, 0),
                new PartitionOffset(3, 0),
                new PartitionOffset(4, 0)
            ],
            MaxBytesPerPush = 100
        };

        Assert.True(payload.EstimateSize() > 0);
    }

    [Fact]
    public void Subscribe_WriteTo_IPayloadWriter_RoundTrip()
    {
        var payload = new SubscribePayload
        {
            SubscriptionId = "iface-sub",
            Topic = "iface-topic",
            Partitions = [new PartitionOffset(7, 99L)],
            MaxBytesPerPush = 8192
        };

        // Write via IPayloadWriter (ListPayloadWriter)
        var listWriter = new ListPayloadWriter();
        payload.WriteTo(listWriter);
        var buffer = listWriter.ToArray();

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = SubscribePayload.Read(ref reader);

        Assert.Equal("iface-sub", parsed.SubscriptionId);
        Assert.Equal("iface-topic", parsed.Topic);
        Assert.Single(parsed.Partitions);
        Assert.Equal(7, parsed.Partitions[0].Partition);
        Assert.Equal(99L, parsed.Partitions[0].StartOffset);
        Assert.Equal(8192, parsed.MaxBytesPerPush);
    }

    // ── SubscribeResponsePayload ──────────────────────────────────────────────

    [Fact]
    public void SubscribeResponse_RoundTrip_MultiplePartitions()
    {
        var payload = new SubscribeResponsePayload
        {
            SubscriptionId = "resp-sub-123",
            Partitions = [0, 1, 2, 3]
        };

        var buffer = new byte[payload.EstimateSize() + 16];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = SubscribeResponsePayload.Read(ref reader);

        Assert.Equal("resp-sub-123", parsed.SubscriptionId);
        Assert.Equal([0, 1, 2, 3], parsed.Partitions);
    }

    [Fact]
    public void SubscribeResponse_RoundTrip_SinglePartition()
    {
        var payload = new SubscribeResponsePayload
        {
            SubscriptionId = "s",
            Partitions = [5]
        };

        var buffer = new byte[payload.EstimateSize() + 16];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = SubscribeResponsePayload.Read(ref reader);

        Assert.Single(parsed.Partitions);
        Assert.Equal(5, parsed.Partitions[0]);
    }

    [Fact]
    public void SubscribeResponse_EstimateSize_IsCorrect()
    {
        // subscriptionId "ab" = 2 + 2 bytes, 0 partitions = 4 bytes
        var payload = new SubscribeResponsePayload { SubscriptionId = "ab", Partitions = [] };
        var estimated = payload.EstimateSize();
        Assert.Equal(2 + 2 + 4, estimated); // len-prefix(2) + "ab"(2) + count(4)
    }

    // ── UnsubscribePayload ────────────────────────────────────────────────────

    [Fact]
    public void Unsubscribe_RoundTrip()
    {
        var payload = new UnsubscribePayload { SubscriptionId = "unsub-456" };

        var buffer = new byte[payload.EstimateSize() + 8];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = UnsubscribePayload.Read(ref reader);

        Assert.Equal("unsub-456", parsed.SubscriptionId);
    }

    [Fact]
    public void Unsubscribe_EstimateSize_IsCorrect()
    {
        // "hi" = 2 bytes + 2 prefix = 4
        var payload = new UnsubscribePayload { SubscriptionId = "hi" };
        Assert.Equal(4, payload.EstimateSize());
    }

    [Fact]
    public void Unsubscribe_WriteTo_IPayloadWriter_RoundTrip()
    {
        var payload = new UnsubscribePayload { SubscriptionId = "via-iface" };

        var listWriter = new ListPayloadWriter();
        payload.WriteTo(listWriter);
        var buffer = listWriter.ToArray();

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = UnsubscribePayload.Read(ref reader);

        Assert.Equal("via-iface", parsed.SubscriptionId);
    }

    // ── StreamAckPayload ──────────────────────────────────────────────────────

    [Fact]
    public void StreamAck_RoundTrip()
    {
        var payload = new StreamAckPayload
        {
            SubscriptionId = "ack-sub",
            AcknowledgedBytes = 1_048_576L
        };

        var buffer = new byte[payload.EstimateSize() + 8];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = StreamAckPayload.Read(ref reader);

        Assert.Equal("ack-sub", parsed.SubscriptionId);
        Assert.Equal(1_048_576L, parsed.AcknowledgedBytes);
    }

    [Fact]
    public void StreamAck_LargeAcknowledgedBytes_RoundTrips()
    {
        var payload = new StreamAckPayload
        {
            SubscriptionId = "large-ack",
            AcknowledgedBytes = long.MaxValue
        };

        var buffer = new byte[payload.EstimateSize() + 8];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = StreamAckPayload.Read(ref reader);

        Assert.Equal(long.MaxValue, parsed.AcknowledgedBytes);
    }

    [Fact]
    public void StreamAck_EstimateSize_IsCorrect()
    {
        // "x" = 1 byte + 2 prefix + 8 bytes for long = 11
        var payload = new StreamAckPayload { SubscriptionId = "x", AcknowledgedBytes = 0 };
        Assert.Equal(2 + 1 + 8, payload.EstimateSize());
    }

    // ── StreamRecordPayload ───────────────────────────────────────────────────

    [Fact]
    public void StreamRecord_RoundTrip_SingleMessage()
    {
        var value = System.Text.Encoding.UTF8.GetBytes("hello-world");
        var payload = new StreamRecordPayload
        {
            SubscriptionId = "rec-sub",
            Partition = 3,
            HighWatermark = 9999L,
            Messages = [new StreamMessage(100L, 1234567890L, null, value)]
        };

        var buffer = new byte[payload.EstimateSize() + 64];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = StreamRecordPayload.Read(ref reader);

        Assert.Equal("rec-sub", parsed.SubscriptionId);
        Assert.Equal(3, parsed.Partition);
        Assert.Equal(9999L, parsed.HighWatermark);
        Assert.Single(parsed.Messages);

        var msg = parsed.Messages[0];
        Assert.Equal(100L, msg.Offset);
        Assert.Equal(1234567890L, msg.Timestamp);
        Assert.Null(msg.Key);
        Assert.Equal(value, msg.Value);
    }

    [Fact]
    public void StreamRecord_RoundTrip_MessageWithKey()
    {
        var key = System.Text.Encoding.UTF8.GetBytes("order-key");
        var value = System.Text.Encoding.UTF8.GetBytes("order-value");

        var payload = new StreamRecordPayload
        {
            SubscriptionId = "k-sub",
            Partition = 0,
            HighWatermark = 500L,
            Messages = [new StreamMessage(1L, 100L, key, value)]
        };

        var buffer = new byte[payload.EstimateSize() + 64];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = StreamRecordPayload.Read(ref reader);

        var msg = parsed.Messages[0];
        Assert.NotNull(msg.Key);
        Assert.Equal(key, msg.Key);
        Assert.Equal(value, msg.Value);
    }

    [Fact]
    public void StreamRecord_RoundTrip_MultipleMixedMessages()
    {
        var messages = new StreamMessage[]
        {
            new(0L, 1000L, null, [1, 2, 3]),
            new(1L, 2000L, [0xAA, 0xBB], [4, 5, 6]),
            new(2L, 3000L, null, [7, 8, 9])
        };

        var payload = new StreamRecordPayload
        {
            SubscriptionId = "multi-sub",
            Partition = 2,
            HighWatermark = 3L,
            Messages = messages
        };

        var buffer = new byte[payload.EstimateSize() + 128];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = StreamRecordPayload.Read(ref reader);

        Assert.Equal(3, parsed.Messages.Length);
        Assert.Null(parsed.Messages[0].Key);
        Assert.NotNull(parsed.Messages[1].Key);
        Assert.Equal(new byte[] { 0xAA, 0xBB }, parsed.Messages[1].Key);
        Assert.Null(parsed.Messages[2].Key);
    }

    [Fact]
    public void StreamRecord_RoundTrip_EmptyMessages()
    {
        var payload = new StreamRecordPayload
        {
            SubscriptionId = "empty-sub",
            Partition = 0,
            HighWatermark = 0L,
            Messages = []
        };

        var buffer = new byte[payload.EstimateSize() + 32];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = StreamRecordPayload.Read(ref reader);

        Assert.Empty(parsed.Messages);
        Assert.Equal("empty-sub", parsed.SubscriptionId);
    }

    [Fact]
    public void StreamRecord_EstimateSize_IsNonNegative()
    {
        var payload = new StreamRecordPayload
        {
            SubscriptionId = "sz",
            Partition = 0,
            HighWatermark = 0,
            Messages = [new StreamMessage(0, 0, null, [1])]
        };

        Assert.True(payload.EstimateSize() > 0);
    }

    // ── OpCode and ErrorCode smoke tests ─────────────────────────────────────

    [Fact]
    public void SurgewaveOpCode_StreamAck_HasExpectedValue()
    {
        Assert.Equal((ushort)0x0206, (ushort)SurgewaveOpCode.StreamAck);
    }

    [Fact]
    public void SurgewaveErrorCode_SubscriptionAlreadyExists_HasExpectedValue()
    {
        Assert.Equal((ushort)110, (ushort)SurgewaveErrorCode.SubscriptionAlreadyExists);
    }

    [Fact]
    public void SurgewaveErrorCode_SubscriptionNotFound_HasExpectedValue()
    {
        Assert.Equal((ushort)111, (ushort)SurgewaveErrorCode.SubscriptionNotFound);
    }

    [Fact]
    public void SurgewaveErrorCode_MaxSubscriptionsExceeded_HasExpectedValue()
    {
        Assert.Equal((ushort)112, (ushort)SurgewaveErrorCode.MaxSubscriptionsExceeded);
    }
}

/// <summary>
/// Simple IPayloadWriter implementation backed by a growable list.
/// Used in tests to verify WriteTo(IPayloadWriter) paths without depending
/// on BigEndianWriter (which is internal to Kuestenlogik.Surgewave.Broker).
/// </summary>
file sealed class ListPayloadWriter : Kuestenlogik.Surgewave.Protocol.Native.Payloads.IPayloadWriter
{
    private readonly List<byte> _bytes = new();

    public byte[] ToArray() => [.. _bytes];

    public void WriteInt8(sbyte value) => _bytes.Add((byte)value);
    public void WriteUInt8(byte value) => _bytes.Add(value);

    public void WriteInt16(short value)
    {
        _bytes.Add((byte)(value >> 8));
        _bytes.Add((byte)value);
    }

    public void WriteUInt16(ushort value)
    {
        _bytes.Add((byte)(value >> 8));
        _bytes.Add((byte)value);
    }

    public void WriteInt32(int value)
    {
        _bytes.Add((byte)(value >> 24));
        _bytes.Add((byte)(value >> 16));
        _bytes.Add((byte)(value >> 8));
        _bytes.Add((byte)value);
    }

    public void WriteUInt32(uint value)
    {
        _bytes.Add((byte)(value >> 24));
        _bytes.Add((byte)(value >> 16));
        _bytes.Add((byte)(value >> 8));
        _bytes.Add((byte)value);
    }

    public void WriteInt64(long value)
    {
        _bytes.Add((byte)(value >> 56));
        _bytes.Add((byte)(value >> 48));
        _bytes.Add((byte)(value >> 40));
        _bytes.Add((byte)(value >> 32));
        _bytes.Add((byte)(value >> 24));
        _bytes.Add((byte)(value >> 16));
        _bytes.Add((byte)(value >> 8));
        _bytes.Add((byte)value);
    }

    public void WriteUInt64(ulong value)
    {
        _bytes.Add((byte)(value >> 56));
        _bytes.Add((byte)(value >> 48));
        _bytes.Add((byte)(value >> 40));
        _bytes.Add((byte)(value >> 32));
        _bytes.Add((byte)(value >> 24));
        _bytes.Add((byte)(value >> 16));
        _bytes.Add((byte)(value >> 8));
        _bytes.Add((byte)value);
    }

    public void WriteString(string? value)
    {
        if (value == null)
        {
            WriteInt16(-1);
            return;
        }
        var encoded = System.Text.Encoding.UTF8.GetBytes(value);
        WriteInt16((short)encoded.Length);
        _bytes.AddRange(encoded);
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        WriteInt32(value.Length);
        _bytes.AddRange(value.ToArray());
    }
}
