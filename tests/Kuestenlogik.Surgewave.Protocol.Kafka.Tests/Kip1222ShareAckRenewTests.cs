using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// KIP-1222 — ShareAcknowledge v2 introduces:
/// (1) request-side: <c>IsRenewAck</c> bool after <c>ShareSessionEpoch</c>,
///     and a new <c>AcknowledgeType = 4: Renew</c> in the int8 array.
/// (2) response-side: <c>AcquisitionLockTimeoutMs</c> int32 after the
///     top-level <c>ErrorMessage</c>.
///
/// The wire fields and v2 gating already live in
/// <c>ShareGroupRequests.cs</c> — these tests pin the framing so a future
/// refactor can't silently drop one of the new fields, and the v1 path
/// stays a strict subset (no v2 bytes on the wire even if the field is set
/// on the model).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1222ShareAckRenewTests
{
    private static readonly Guid SampleTopicId = new("11111111-1111-1111-1111-111111111111");

    [Fact]
    public void V2_Request_RoundTrip_PreservesIsRenewAck_AndRenewAckType()
    {
        var original = new ShareAcknowledgeRequest
        {
            ApiKey = ApiKey.ShareAcknowledge,
            ApiVersion = 2,
            CorrelationId = 17,
            ClientId = "kip1222-client",
            GroupId = "share-grp-1",
            MemberId = "m-1",
            ShareSessionEpoch = 3,
            IsRenewAck = true,
            Topics =
            [
                new ShareAcknowledgeRequest.AcknowledgeTopic
                {
                    TopicId = SampleTopicId,
                    Partitions =
                    [
                        new ShareAcknowledgeRequest.AcknowledgePartition
                        {
                            PartitionIndex = 0,
                            AcknowledgementBatches =
                            [
                                new ShareAcknowledgeRequest.AcknowledgementBatch
                                {
                                    FirstOffset = 100,
                                    LastOffset = 199,
                                    AcknowledgeTypes = [(sbyte)4], // 4 = Renew (KIP-1222)
                                },
                            ],
                        },
                    ],
                },
            ],
        };

        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        // ShareAcknowledgeRequest.WriteTo emits a full header (ApiKey + ApiVersion
        // + CorrelationId + ClientId compact-string + tagged-fields varint).
        // ReadFrom expects the reader positioned at the body. The header is
        // 2+2+4 = 8 bytes of fixed prefix + the ClientId compact-string +
        // 1-byte tagged-fields varint; easier to drive ReadFrom off a fresh
        // KafkaProtocolHandler-style framing, but for a focused unit test we
        // walk past the header by parsing it inline.
        var reader = new KafkaProtocolReader(writer.ToArray());
        reader.ReadInt16(); // ApiKey
        reader.ReadInt16(); // ApiVersion
        reader.ReadInt32(); // CorrelationId
        reader.ReadCompactString(); // ClientId
        reader.SkipTaggedFields(); // header tagged fields
        var parsed = ShareAcknowledgeRequest.ReadFrom(reader, apiVersion: 2, correlationId: 17, clientId: "kip1222-client");

        Assert.True(parsed.IsRenewAck);
        Assert.Equal("share-grp-1", parsed.GroupId);
        var batch = parsed.Topics[0].Partitions[0].AcknowledgementBatches[0];
        Assert.Equal(100, batch.FirstOffset);
        Assert.Equal(199, batch.LastOffset);
        Assert.Equal([(sbyte)4], batch.AcknowledgeTypes); // Renew survives
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void V1_Request_DoesNotEmitIsRenewAck()
    {
        // Regression guard: at v1 the wire MUST NOT include IsRenewAck,
        // otherwise the topic-array varint that follows mis-aligns.
        var original = new ShareAcknowledgeRequest
        {
            ApiKey = ApiKey.ShareAcknowledge,
            ApiVersion = 1,
            CorrelationId = 17,
            ClientId = "kip1222-client",
            GroupId = "share-grp-1",
            MemberId = "m-1",
            ShareSessionEpoch = 3,
            IsRenewAck = true, // should be IGNORED at v1
            Topics =
            [
                new ShareAcknowledgeRequest.AcknowledgeTopic
                {
                    TopicId = SampleTopicId,
                    Partitions =
                    [
                        new ShareAcknowledgeRequest.AcknowledgePartition
                        {
                            PartitionIndex = 0,
                            AcknowledgementBatches =
                            [
                                new ShareAcknowledgeRequest.AcknowledgementBatch
                                {
                                    FirstOffset = 0,
                                    LastOffset = 9,
                                    AcknowledgeTypes = [(sbyte)1], // Accept
                                },
                            ],
                        },
                    ],
                },
            ],
        };

        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray());
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadCompactString(); reader.SkipTaggedFields();
        var parsed = ShareAcknowledgeRequest.ReadFrom(reader, apiVersion: 1, correlationId: 17, clientId: "kip1222-client");

        Assert.False(parsed.IsRenewAck); // default — never on the v1 wire
        Assert.Equal("share-grp-1", parsed.GroupId);
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void V2_Response_RoundTrip_PreservesAcquisitionLockTimeoutMs()
    {
        var response = new ShareAcknowledgeResponse
        {
            ApiVersion = 2,
            CorrelationId = 17,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            AcquisitionLockTimeoutMs = 30000, // 30s lease
            Responses = [],
            NodeEndpoints = [],
        };

        var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        // WriteTo emits CorrelationId ahead of the body; ReadFrom expects
        // the reader at the response-header tagged-fields varint that follows.
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = ShareAcknowledgeResponse.ReadFrom(reader, apiVersion: 2, correlationId: 17);

        Assert.Equal(30000, parsed.AcquisitionLockTimeoutMs);
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void V1_Response_DoesNotEmitAcquisitionLockTimeoutMs()
    {
        var response = new ShareAcknowledgeResponse
        {
            ApiVersion = 1,
            CorrelationId = 17,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            AcquisitionLockTimeoutMs = 30000, // should be IGNORED at v1
            Responses = [],
            NodeEndpoints = [],
        };

        var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = ShareAcknowledgeResponse.ReadFrom(reader, apiVersion: 1, correlationId: 17);

        Assert.Equal(0, parsed.AcquisitionLockTimeoutMs); // default — no v2 bytes on the wire
        Assert.Equal(0, reader.Remaining);
    }
}
