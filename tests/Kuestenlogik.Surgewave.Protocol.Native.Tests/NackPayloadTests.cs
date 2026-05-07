using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Tests for Nack payload serialization/deserialization
/// </summary>
public sealed class NackPayloadTests
{
    [Fact]
    public void NackRequest_RoundTrip()
    {
        var payload = new NackRequestPayload
        {
            Topic = "orders",
            Partition = 3,
            Offset = 12345L
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = NackRequestPayload.Read(ref reader);

        Assert.Equal("orders", parsed.Topic);
        Assert.Equal(3, parsed.Partition);
        Assert.Equal(12345L, parsed.Offset);
    }

    [Fact]
    public void NackRequest_PartitionZero_RoundTrips()
    {
        var payload = new NackRequestPayload
        {
            Topic = "events",
            Partition = 0,
            Offset = 0L
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = NackRequestPayload.Read(ref reader);

        Assert.Equal(0, parsed.Partition);
        Assert.Equal(0L, parsed.Offset);
    }

    [Fact]
    public void NackResponse_RoundTrip_RoutedToDlq()
    {
        var payload = new NackResponsePayload
        {
            RoutedToDlq = true,
            RetryCount = 5
        };

        var buffer = new byte[payload.EstimateSize() + 5];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = NackResponsePayload.Read(ref reader);

        Assert.True(parsed.RoutedToDlq);
        Assert.Equal(5, parsed.RetryCount);
    }

    [Fact]
    public void NackResponse_RoundTrip_Retry()
    {
        var payload = new NackResponsePayload
        {
            RoutedToDlq = false,
            RetryCount = 2
        };

        var buffer = new byte[payload.EstimateSize() + 5];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = NackResponsePayload.Read(ref reader);

        Assert.False(parsed.RoutedToDlq);
        Assert.Equal(2, parsed.RetryCount);
    }

    [Fact]
    public void NackRequest_EstimateSize_IsCorrect()
    {
        var payload = new NackRequestPayload
        {
            Topic = "test", // 4 bytes
            Partition = 0,
            Offset = 0L
        };

        var estimated = payload.EstimateSize();
        // 2 (len prefix) + 4 (topic) + 4 (partition) + 8 (offset) = 18
        Assert.Equal(18, estimated);
    }

    [Fact]
    public void NackResponse_EstimateSize_IsCorrect()
    {
        var payload = new NackResponsePayload { RoutedToDlq = false, RetryCount = 0 };
        // 1 (bool) + 4 (int) = 5
        Assert.Equal(5, payload.EstimateSize());
    }

    [Fact]
    public void NackRequest_LargeOffset_RoundTrips()
    {
        var payload = new NackRequestPayload
        {
            Topic = "high-volume-topic",
            Partition = 15,
            Offset = long.MaxValue
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = NackRequestPayload.Read(ref reader);

        Assert.Equal("high-volume-topic", parsed.Topic);
        Assert.Equal(15, parsed.Partition);
        Assert.Equal(long.MaxValue, parsed.Offset);
    }
}
