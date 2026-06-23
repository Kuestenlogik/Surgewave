using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ShareGroups;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — KIP-932 ShareFetch + ShareAcknowledge native
/// payloads. These are the dominant data-path RPCs for share-consumers:
/// ShareFetch pulls records (with inline ack-batches piggy-backed),
/// ShareAcknowledge sends standalone acks for already-fetched records.
///
/// Covers <see cref="ShareFetchRequestPayload"/> + Response (with
/// nested ShareFetchTopic / ShareFetchPartition /
/// AcknowledgementBatch / ShareFetchForgottenTopic /
/// ShareFetchTopicResponse / ShareFetchPartitionResponse /
/// AcquiredRecord) and <see cref="ShareAcknowledgeRequestPayload"/> +
/// Response (with nested ShareAcknowledgeTopic /
/// ShareAcknowledgePartition / ShareAcknowledgeTopicResponse /
/// ShareAcknowledgePartitionResponse).
/// </summary>
public sealed class ShareFetchAckRoundTripTests
{
    private static readonly Guid TopicA = new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly Guid TopicB = new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // ShareFetch
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void FetchRequest_FullShape_RoundTrips()
    {
        var original = new ShareFetchRequestPayload
        {
            GroupId = "share-orders",
            MemberId = "consumer-1",
            MaxWaitMs = 500,
            MinBytes = 1,
            MaxBytes = 1_048_576,
            Topics = new[]
            {
                new ShareFetchTopic(TopicA, new[]
                {
                    new ShareFetchPartition
                    {
                        PartitionIndex = 0,
                        PartitionMaxBytes = 524_288,
                        AcknowledgementBatches = new[]
                        {
                            // Piggy-backed ack: Accept (1) the range [10, 12]
                            new AcknowledgementBatch(10, 12, new byte[] { 1, 1, 1 }),
                            // Renew (4) ack at 13
                            new AcknowledgementBatch(13, 13, new byte[] { 4 }),
                        },
                    },
                    new ShareFetchPartition
                    {
                        PartitionIndex = 1,
                        PartitionMaxBytes = 524_288,
                        AcknowledgementBatches = Array.Empty<AcknowledgementBatch>(),
                    },
                }),
            },
            ForgottenTopics = new[]
            {
                new ShareFetchForgottenTopic(TopicB, new[] { 0, 1 }),
            },
        };

        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareFetchRequestPayload.Read(ref r); });

        Assert.Equal("share-orders", parsed.GroupId);
        Assert.Equal("consumer-1", parsed.MemberId);
        Assert.Equal(500, parsed.MaxWaitMs);
        Assert.Equal(1_048_576, parsed.MaxBytes);

        Assert.Single(parsed.Topics);
        var topic = parsed.Topics[0];
        Assert.Equal(TopicA, topic.TopicId);
        Assert.Equal(2, topic.Partitions.Length);

        var p0 = topic.Partitions[0];
        Assert.Equal(0, p0.PartitionIndex);
        Assert.Equal(524_288, p0.PartitionMaxBytes);
        Assert.Equal(2, p0.AcknowledgementBatches.Length);
        Assert.Equal(10, p0.AcknowledgementBatches[0].FirstOffset);
        Assert.Equal(12, p0.AcknowledgementBatches[0].LastOffset);
        Assert.Equal(new byte[] { 1, 1, 1 }, p0.AcknowledgementBatches[0].AcknowledgeTypes);
        Assert.Equal(new byte[] { 4 }, p0.AcknowledgementBatches[1].AcknowledgeTypes);

        Assert.Empty(topic.Partitions[1].AcknowledgementBatches);

        Assert.Single(parsed.ForgottenTopics);
        Assert.Equal(TopicB, parsed.ForgottenTopics[0].TopicId);
        Assert.Equal(new[] { 0, 1 }, parsed.ForgottenTopics[0].Partitions);
    }

    [Fact]
    public void FetchRequest_NoTopicsNoForgotten_RoundTrips()
    {
        // Empty fetch — used for long-polling without subscription changes.
        var original = new ShareFetchRequestPayload
        {
            GroupId = "g",
            MemberId = "m",
            MaxWaitMs = 0,
            MinBytes = 0,
            MaxBytes = 0,
            Topics = Array.Empty<ShareFetchTopic>(),
            ForgottenTopics = Array.Empty<ShareFetchForgottenTopic>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareFetchRequestPayload.Read(ref r); });
        Assert.Empty(parsed.Topics);
        Assert.Empty(parsed.ForgottenTopics);
    }

    [Fact]
    public void FetchResponse_WithRecordsAndAcquired_RoundTrips()
    {
        // Synthetic 32-byte records blob — production payloads are RecordBatch
        // bytes, but the framing is identical.
        var recordBytes = new byte[32];
        for (var i = 0; i < recordBytes.Length; i++) recordBytes[i] = (byte)(i & 0xFF);

        var original = new ShareFetchResponsePayload
        {
            ThrottleTimeMs = 0,
            Responses = new[]
            {
                new ShareFetchTopicResponse(TopicA, new[]
                {
                    new ShareFetchPartitionResponse
                    {
                        PartitionIndex = 0,
                        ErrorCode = 0,
                        HighWatermark = 1_000_000L,
                        Records = recordBytes,
                        AcquiredRecords = new[]
                        {
                            new AcquiredRecord(100, 102, 1),  // first delivery
                            new AcquiredRecord(103, 103, 3),  // third delivery
                        },
                    },
                }),
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize() + recordBytes.Length, // EstimateSize doesn't add records length
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareFetchResponsePayload.Read(ref r); });

        Assert.Single(parsed.Responses);
        var p = parsed.Responses[0].Partitions[0];
        Assert.Equal(0, p.PartitionIndex);
        Assert.Equal(1_000_000L, p.HighWatermark);
        Assert.NotNull(p.Records);
        Assert.Equal(recordBytes, p.Records);
        Assert.Equal(2, p.AcquiredRecords.Length);
        Assert.Equal(100, p.AcquiredRecords[0].FirstOffset);
        Assert.Equal(102, p.AcquiredRecords[0].LastOffset);
        Assert.Equal(1, p.AcquiredRecords[0].DeliveryCount);
        Assert.Equal(3, p.AcquiredRecords[1].DeliveryCount); // third delivery of single offset 103
    }

    [Fact]
    public void FetchResponse_NullRecords_WireDistinctFromEmpty()
    {
        // Wire uses int32=-1 for null vs int32=0 for empty Records. Pin
        // both — Read normalises null to null and empty to non-null
        // empty array.
        var original = new ShareFetchResponsePayload
        {
            ThrottleTimeMs = 0,
            Responses = new[]
            {
                new ShareFetchTopicResponse(TopicA, new[]
                {
                    new ShareFetchPartitionResponse
                    {
                        PartitionIndex = 0,
                        ErrorCode = 0,
                        HighWatermark = 0,
                        Records = null, // no records yet (long-poll timed out)
                        AcquiredRecords = Array.Empty<AcquiredRecord>(),
                    },
                }),
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareFetchResponsePayload.Read(ref r); });

        Assert.Null(parsed.Responses[0].Partitions[0].Records);
        Assert.Empty(parsed.Responses[0].Partitions[0].AcquiredRecords);
    }

    // ───────────────────────────────────────────────────────────────
    // ShareAcknowledge
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void AckRequest_FullShape_RoundTrips()
    {
        var original = new ShareAcknowledgeRequestPayload
        {
            GroupId = "share-orders",
            MemberId = "consumer-1",
            Topics = new[]
            {
                new ShareAcknowledgeTopic(TopicA, new[]
                {
                    new ShareAcknowledgePartition(0, new[]
                    {
                        new AcknowledgementBatch(10, 14, new byte[] { 1, 1, 2, 3, 4 }),
                        new AcknowledgementBatch(20, 20, new byte[] { 1 }),
                    }),
                }),
                new ShareAcknowledgeTopic(TopicB, new[]
                {
                    new ShareAcknowledgePartition(0, new[]
                    {
                        new AcknowledgementBatch(5, 5, new byte[] { 1 }),
                    }),
                }),
            },
        };

        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareAcknowledgeRequestPayload.Read(ref r); });

        Assert.Equal("share-orders", parsed.GroupId);
        Assert.Equal(2, parsed.Topics.Length);

        var t0 = parsed.Topics[0];
        Assert.Equal(TopicA, t0.TopicId);
        Assert.Single(t0.Partitions);
        Assert.Equal(2, t0.Partitions[0].AcknowledgementBatches.Length);
        Assert.Equal(10, t0.Partitions[0].AcknowledgementBatches[0].FirstOffset);
        Assert.Equal(14, t0.Partitions[0].AcknowledgementBatches[0].LastOffset);
        // KIP-932 ack types: 1=Accept, 2=Release, 3=Reject, 4=Renew (KIP-1222)
        Assert.Equal(new byte[] { 1, 1, 2, 3, 4 }, t0.Partitions[0].AcknowledgementBatches[0].AcknowledgeTypes);

        var t1 = parsed.Topics[1];
        Assert.Equal(TopicB, t1.TopicId);
        Assert.Equal(new byte[] { 1 }, t1.Partitions[0].AcknowledgementBatches[0].AcknowledgeTypes);
    }

    [Fact]
    public void AckRequest_EmptyTopics_RoundTrips()
    {
        var original = new ShareAcknowledgeRequestPayload
        {
            GroupId = "g",
            MemberId = "m",
            Topics = Array.Empty<ShareAcknowledgeTopic>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareAcknowledgeRequestPayload.Read(ref r); });
        Assert.Empty(parsed.Topics);
    }

    [Fact]
    public void AckResponse_FullShape_RoundTrips()
    {
        var original = new ShareAcknowledgeResponsePayload
        {
            ThrottleTimeMs = 0,
            Responses = new[]
            {
                new ShareAcknowledgeTopicResponse(TopicA, new[]
                {
                    new ShareAcknowledgePartitionResponse(0, 0),     // OK
                    new ShareAcknowledgePartitionResponse(1, 42),    // INVALID_REQUEST (e.g. KIP-1240 renew gate closed)
                }),
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ShareAcknowledgeResponsePayload.Read(ref r); });

        Assert.Single(parsed.Responses);
        var partitions = parsed.Responses[0].Partitions;
        Assert.Equal(2, partitions.Length);
        Assert.Equal((short)0, partitions[0].ErrorCode);
        Assert.Equal((short)42, partitions[1].ErrorCode);
    }
}
