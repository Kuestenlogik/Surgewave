using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// KIP-1319 — TxnOffsetCommit v6 replaces the topic <c>Name</c> string with a
/// <c>TopicId</c> uuid in both the request and response. The model now carries
/// both fields and the wire methods pick one or the other based on
/// <c>ApiVersion</c>. The broker-side handler resolves TopicId → Name via the
/// log manager (and returns <c>UnknownTopicId</c> for unresolvable IDs) so the
/// pending-offset store stays name-keyed regardless of which version came in
/// on the wire.
///
/// These tests pin the framing for both v5 (Name-on-the-wire) and v6
/// (TopicId-on-the-wire) so a future refactor can't silently break one of
/// the two paths.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1319TxnOffsetCommitV6Tests
{
    private static readonly Guid SampleTopicId = new("aaaabbbb-cccc-dddd-eeee-ffff00112233");

    private static TxnOffsetCommitRequest BuildRequest(short apiVersion, string? name, Guid topicId) =>
        new()
        {
            ApiKey = ApiKey.TxnOffsetCommit,
            ApiVersion = apiVersion,
            CorrelationId = 11,
            ClientId = "kip1319-test",
            TransactionalId = "txn-1",
            GroupId = "grp-1",
            ProducerId = 42,
            ProducerEpoch = 0,
            GenerationId = 7,
            MemberId = "m-1",
            GroupInstanceId = null,
            Topics =
            [
                new TxnOffsetCommitRequest.TxnOffsetCommitTopic
                {
                    Name = name,
                    TopicId = topicId,
                    Partitions =
                    [
                        new TxnOffsetCommitRequest.TxnOffsetCommitPartition
                        {
                            Partition = 0,
                            CommittedOffset = 100,
                            CommittedLeaderEpoch = 4,
                            Metadata = "test-metadata",
                        },
                    ],
                },
            ],
        };

    [Fact]
    public void Request_V6_RoundTrip_UsesTopicIdOnTheWire()
    {
        var original = BuildRequest(apiVersion: 6, name: null, topicId: SampleTopicId);

        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        // Skip the header (ApiKey/ApiVersion/CorrelationId/ClientId/tagged
        // fields) — TxnOffsetCommitRequest.ReadFrom takes a BinaryReader
        // positioned at the body. The body layout matches the body the
        // protocol handler hands to ReadFrom in production.
        var bytes = writer.ToArray();
        using var ms = new MemoryStream(bytes);
        using var br = new BinaryReader(ms);
        br.ReadInt16(); // ApiKey
        br.ReadInt16(); // ApiVersion
        br.ReadInt32(); // CorrelationId
        // ClientId is a non-compact string in the request header for the
        // surgewave protocol (length-prefixed int16 + UTF-8 body).
        var clientIdLen = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(br.ReadInt16());
        br.ReadBytes(clientIdLen);
        var parsed = TxnOffsetCommitRequest.ReadFrom(br, apiVersion: 6, correlationId: 11, clientId: "kip1319-test");

        Assert.Single(parsed.Topics);
        var topic = parsed.Topics[0];
        Assert.Equal(SampleTopicId, topic.TopicId);
        Assert.Null(topic.Name); // v6 wire carries TopicId only
        Assert.Single(topic.Partitions);
        Assert.Equal(100, topic.Partitions[0].CommittedOffset);
    }

    [Fact]
    public void Request_V5_RoundTrip_UsesNameOnTheWire()
    {
        var original = BuildRequest(apiVersion: 5, name: "orders", topicId: Guid.Empty);

        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();
        using var ms = new MemoryStream(bytes);
        using var br = new BinaryReader(ms);
        br.ReadInt16(); br.ReadInt16(); br.ReadInt32();
        var clientIdLen = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(br.ReadInt16());
        br.ReadBytes(clientIdLen);
        var parsed = TxnOffsetCommitRequest.ReadFrom(br, apiVersion: 5, correlationId: 11, clientId: "kip1319-test");

        var topic = parsed.Topics[0];
        Assert.Equal("orders", topic.Name);
        Assert.Equal(Guid.Empty, topic.TopicId); // v5 wire carries Name only
    }

    [Fact]
    public void Response_V6_RoundTrip_UsesTopicIdOnTheWire()
    {
        var response = new TxnOffsetCommitResponse
        {
            ApiVersion = 6,
            CorrelationId = 11,
            ThrottleTimeMs = 0,
            Topics =
            [
                new TxnOffsetCommitResponse.TxnOffsetCommitTopicResult
                {
                    Name = null,                  // v6 — Name not written
                    TopicId = SampleTopicId,
                    Partitions =
                    [
                        new TxnOffsetCommitResponse.TxnOffsetCommitPartitionResult
                        {
                            Partition = 0,
                            ErrorCode = ErrorCode.None,
                        },
                    ],
                },
            ],
        };

        var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        // Response.WriteTo prefixes CorrelationId (4 bytes). The body parser
        // here re-reads the header-tagged-fields varint inline, so we slice
        // past CorrelationId and use a KafkaProtocolReader for the body.
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        reader.SkipTaggedFields(); // response header tagged fields
        var throttle = reader.ReadInt32();
        var topicCount = reader.ReadVarInt() - 1;
        Assert.Equal(0, throttle);
        Assert.Equal(1, topicCount);

        var topicId = reader.ReadUuid();
        Assert.Equal(SampleTopicId, topicId);
    }

    [Fact]
    public void Response_V5_RoundTrip_UsesNameOnTheWire()
    {
        var response = new TxnOffsetCommitResponse
        {
            ApiVersion = 5,
            CorrelationId = 11,
            ThrottleTimeMs = 0,
            Topics =
            [
                new TxnOffsetCommitResponse.TxnOffsetCommitTopicResult
                {
                    Name = "orders",
                    TopicId = Guid.Empty,
                    Partitions =
                    [
                        new TxnOffsetCommitResponse.TxnOffsetCommitPartitionResult
                        {
                            Partition = 0,
                            ErrorCode = ErrorCode.None,
                        },
                    ],
                },
            ],
        };

        var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        reader.SkipTaggedFields(); // header tagged
        reader.ReadInt32(); // throttle
        reader.ReadVarInt(); // topic count
        var name = reader.ReadCompactString();
        Assert.Equal("orders", name);
    }
}
