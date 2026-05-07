using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Wire-level conformance for the KIP-932 Share Group RPCs (Heartbeat, Fetch,
/// Acknowledge). Round-trips each shape through the same parser librdkafka would
/// drive, so the on-the-wire bytes match the Kafka spec.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip932WireTests
{
    [Fact]
    public void ShareGroupHeartbeatRequest_Roundtrips_WithSubscription()
    {
        var original = new ShareGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ShareGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 11,
            ClientId = "share-c1",
            GroupId = "share-g",
            MemberId = "share-m",
            MemberEpoch = 0,
            RackId = "rack-a",
            SubscribedTopicNames = ["queue-a", "queue-b"],
        };

        using var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();

        var reader = new KafkaProtocolReader(bytes);
        Assert.Equal((short)ApiKey.ShareGroupHeartbeat, reader.ReadInt16());
        Assert.Equal(0, reader.ReadInt16());
        Assert.Equal(11, reader.ReadInt32());
        Assert.Equal("share-c1", reader.ReadCompactString());
        reader.SkipTaggedFields();

        var parsed = ShareGroupHeartbeatRequest.ReadFrom(reader, apiVersion: 0, correlationId: 11, clientId: "share-c1");

        Assert.Equal("share-g", parsed.GroupId);
        Assert.Equal("share-m", parsed.MemberId);
        Assert.Equal(0, parsed.MemberEpoch);
        Assert.Equal("rack-a", parsed.RackId);
        Assert.NotNull(parsed.SubscribedTopicNames);
        Assert.Equal(["queue-a", "queue-b"], parsed.SubscribedTopicNames);
    }

    [Fact]
    public void ShareGroupHeartbeatRequest_Roundtrips_LeaveSignal()
    {
        // KIP-932: MemberEpoch = -1 means "I'm leaving".
        var original = new ShareGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ShareGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "c",
            GroupId = "g",
            MemberId = "m",
            MemberEpoch = -1,
            RackId = null,
            SubscribedTopicNames = null,
        };

        using var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();

        var reader = new KafkaProtocolReader(bytes);
        _ = reader.ReadInt16(); _ = reader.ReadInt16(); _ = reader.ReadInt32();
        _ = reader.ReadCompactString(); reader.SkipTaggedFields();

        var parsed = ShareGroupHeartbeatRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "c");

        Assert.Equal(-1, parsed.MemberEpoch);
        Assert.Null(parsed.RackId);
        Assert.Null(parsed.SubscribedTopicNames);
    }

    [Fact]
    public void ShareFetchRequest_Roundtrips_WithInlineAcknowledgements()
    {
        // The interesting part of KIP-932 is that ShareFetch can carry an inline
        // acknowledgement batch — the client doesn't need a separate
        // ShareAcknowledge round-trip in the steady state.
        var topicId = Guid.NewGuid();
        var original = new ShareFetchRequest
        {
            ApiKey = ApiKey.ShareFetch,
            ApiVersion = 1, // v1+ — needed so MaxRecords/BatchSize round-trip
            CorrelationId = 5,
            ClientId = "share-fetch",
            GroupId = "g",
            MemberId = "m",
            ShareSessionEpoch = 0,
            MaxWaitMs = 500,
            MinBytes = 1,
            MaxBytes = 1_048_576,
            MaxRecords = 100,
            BatchSize = 100,
            Topics =
            [
                new ShareFetchRequest.FetchTopic
                {
                    TopicId = topicId,
                    Partitions =
                    [
                        new ShareFetchRequest.FetchPartition
                        {
                            PartitionIndex = 0,
                            AcknowledgementBatches =
                            [
                                new ShareFetchRequest.AcknowledgementBatch
                                {
                                    FirstOffset = 10,
                                    LastOffset = 12,
                                    AcknowledgeTypes = [1, 1, 2], // Accept, Accept, Release
                                }
                            ],
                        }
                    ],
                }
            ],
            ForgottenTopicsData = [],
        };

        using var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();

        var reader = new KafkaProtocolReader(bytes);
        _ = reader.ReadInt16(); _ = reader.ReadInt16(); _ = reader.ReadInt32();
        _ = reader.ReadCompactString(); reader.SkipTaggedFields();

        var parsed = ShareFetchRequest.ReadFrom(reader, apiVersion: 1, correlationId: 5, clientId: "share-fetch");

        Assert.Equal("g", parsed.GroupId);
        Assert.Equal("m", parsed.MemberId);
        Assert.Equal(100, parsed.MaxRecords);
        var topic = Assert.Single(parsed.Topics);
        Assert.Equal(topicId, topic.TopicId);
        var partition = Assert.Single(topic.Partitions);
        Assert.Equal(0, partition.PartitionIndex);
        var ackBatch = Assert.Single(partition.AcknowledgementBatches);
        Assert.Equal(10, ackBatch.FirstOffset);
        Assert.Equal(12, ackBatch.LastOffset);
        Assert.Equal<sbyte>([1, 1, 2], ackBatch.AcknowledgeTypes);
    }

    [Fact]
    public void ShareAcknowledgeRequest_Roundtrips_WithRenewType()
    {
        // KIP-932 AcknowledgeType=4 (Renew) — Surgewave now extends the visibility
        // timeout in place rather than nack+requeue. Wire shape must round-trip.
        var topicId = Guid.NewGuid();
        var original = new ShareAcknowledgeRequest
        {
            ApiKey = ApiKey.ShareAcknowledge,
            ApiVersion = 0,
            CorrelationId = 3,
            ClientId = "share-ack",
            GroupId = "g",
            MemberId = "m",
            ShareSessionEpoch = 1,
            Topics =
            [
                new ShareAcknowledgeRequest.AcknowledgeTopic
                {
                    TopicId = topicId,
                    Partitions =
                    [
                        new ShareAcknowledgeRequest.AcknowledgePartition
                        {
                            PartitionIndex = 1,
                            AcknowledgementBatches =
                            [
                                new ShareAcknowledgeRequest.AcknowledgementBatch
                                {
                                    FirstOffset = 100,
                                    LastOffset = 100,
                                    AcknowledgeTypes = [4], // Renew (KIP-932)
                                }
                            ],
                        }
                    ],
                }
            ],
        };

        using var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();

        var reader = new KafkaProtocolReader(bytes);
        _ = reader.ReadInt16(); _ = reader.ReadInt16(); _ = reader.ReadInt32();
        _ = reader.ReadCompactString(); reader.SkipTaggedFields();

        var parsed = ShareAcknowledgeRequest.ReadFrom(reader, apiVersion: 0, correlationId: 3, clientId: "share-ack");

        var topic = Assert.Single(parsed.Topics);
        var partition = Assert.Single(topic.Partitions);
        var batch = Assert.Single(partition.AcknowledgementBatches);
        Assert.Equal<sbyte>([4], batch.AcknowledgeTypes);
    }
}
