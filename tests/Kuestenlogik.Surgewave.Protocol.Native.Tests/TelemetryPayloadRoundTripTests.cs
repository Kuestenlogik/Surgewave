using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Telemetry;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — KIP-714 client-telemetry payloads. These back
/// the broker-driven metrics subscription / push protocol: clients first
/// call <c>GetTelemetrySubscriptions</c> to discover what to send, then
/// periodically <c>PushTelemetry</c> with OTLP-shaped metric blobs.
/// All four payloads sat at 0% on the latest report.
/// </summary>
public sealed class TelemetryPayloadRoundTripTests
{
    private static readonly Guid SampleInstanceId = new("a0a0a0a0-b1b1-c2c2-d3d3-e4e4e4e4e4e4");

    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // GetTelemetrySubscriptions
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void GetTelemetrySubscriptionsRequest_RoundTrip_PreservesClientInstanceId()
    {
        var original = new GetTelemetrySubscriptionsRequestPayload { ClientInstanceId = SampleInstanceId };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetTelemetrySubscriptionsRequestPayload.Read(ref r); });
        Assert.Equal(SampleInstanceId, parsed.ClientInstanceId);
    }

    [Fact]
    public void GetTelemetrySubscriptionsResponse_FullShape_RoundTrips()
    {
        var original = new GetTelemetrySubscriptionsResponsePayload
        {
            ThrottleTimeMs = 0,
            ErrorCode = 0,
            ClientInstanceId = SampleInstanceId,
            SubscriptionId = 42,
            // Compression types: zstd (4), gzip (1) — raw bytes for the
            // "negotiate this compression" hint.
            AcceptedCompressionTypes = new byte[] { 4, 1 },
            PushIntervalMs = 60_000,
            TelemetryMaxBytes = 1_048_576,
            DeltaTemporality = true,
            RequestedMetrics = new[]
            {
                "org.apache.kafka.producer.partition-count",
                "org.apache.kafka.producer.record-send-rate",
                "io.surgewave.broker.bytes-in-rate",
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetTelemetrySubscriptionsResponsePayload.Read(ref r); });

        Assert.Equal(SampleInstanceId, parsed.ClientInstanceId);
        Assert.Equal(42, parsed.SubscriptionId);
        Assert.Equal(new byte[] { 4, 1 }, parsed.AcceptedCompressionTypes);
        Assert.Equal(60_000, parsed.PushIntervalMs);
        Assert.Equal(1_048_576, parsed.TelemetryMaxBytes);
        Assert.True(parsed.DeltaTemporality);
        Assert.Equal(3, parsed.RequestedMetrics.Length);
        Assert.Contains("bytes-in-rate", parsed.RequestedMetrics[2]);
    }

    [Fact]
    public void GetTelemetrySubscriptionsResponse_EmptySubscription_RoundTrips()
    {
        // First request from an unknown client: broker assigns a fresh
        // subscription ID + empty metric list (nothing to push yet).
        var original = new GetTelemetrySubscriptionsResponsePayload
        {
            ThrottleTimeMs = 0,
            ErrorCode = 0,
            ClientInstanceId = SampleInstanceId,
            SubscriptionId = 1,
            AcceptedCompressionTypes = Array.Empty<byte>(),
            PushIntervalMs = 30_000,
            TelemetryMaxBytes = 524_288,
            DeltaTemporality = false,
            RequestedMetrics = Array.Empty<string>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return GetTelemetrySubscriptionsResponsePayload.Read(ref r); });

        Assert.Empty(parsed.AcceptedCompressionTypes);
        Assert.False(parsed.DeltaTemporality);
        Assert.Empty(parsed.RequestedMetrics);
    }

    // ───────────────────────────────────────────────────────────────
    // PushTelemetry
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void PushTelemetryRequest_RoundTrip_PreservesAllFields()
    {
        // 64-byte synthetic OTLP-shaped blob — production payloads are
        // larger but the round-trip framing is identical.
        var metricsBlob = new byte[64];
        for (int i = 0; i < metricsBlob.Length; i++) metricsBlob[i] = (byte)(i & 0xFF);

        var original = new PushTelemetryRequestPayload
        {
            ClientInstanceId = SampleInstanceId,
            SubscriptionId = 42,
            Terminating = false,
            CompressionType = 4, // zstd
            Metrics = metricsBlob,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return PushTelemetryRequestPayload.Read(ref r); });

        Assert.Equal(SampleInstanceId, parsed.ClientInstanceId);
        Assert.Equal(42, parsed.SubscriptionId);
        Assert.False(parsed.Terminating);
        Assert.Equal((byte)4, parsed.CompressionType);
        Assert.Equal(metricsBlob, parsed.Metrics);
    }

    [Fact]
    public void PushTelemetryRequest_TerminatingShape_RoundTrips()
    {
        // Last push from a client that's shutting down — empty metrics +
        // Terminating=true is the broker's signal to retire the subscription.
        var original = new PushTelemetryRequestPayload
        {
            ClientInstanceId = SampleInstanceId,
            SubscriptionId = 42,
            Terminating = true,
            CompressionType = 0, // no compression
            Metrics = Array.Empty<byte>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return PushTelemetryRequestPayload.Read(ref r); });

        Assert.True(parsed.Terminating);
        Assert.Equal((byte)0, parsed.CompressionType);
        Assert.Empty(parsed.Metrics);
    }

    [Theory]
    [InlineData((short)0)]   // OK
    [InlineData((short)42)]  // INVALID_REQUEST
    [InlineData((short)93)]  // UNKNOWN_SUBSCRIPTION_ID
    public void PushTelemetryResponse_RoundTrips_AllErrorCodes(short errorCode)
    {
        var original = new PushTelemetryResponsePayload { ThrottleTimeMs = 0, ErrorCode = errorCode };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return PushTelemetryResponsePayload.Read(ref r); });
        Assert.Equal(errorCode, parsed.ErrorCode);
    }
}
