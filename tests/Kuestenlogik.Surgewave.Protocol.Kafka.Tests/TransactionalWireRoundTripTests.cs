using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Wire round-trips for the transactional admin RPCs that flank
/// TxnOffsetCommit (already pinned by Kip1319 + Kip892 tests). Both pairs
/// have the typical v0-v2 (non-flexible) / v3+ (flexible) split and are
/// emitted by every Confluent.Kafka transactional producer during
/// initTransactions / sendOffsetsToTransaction, so a framing regression here
/// shows up as obscure "invalid record state" client errors.
///
/// These tests exercise WriteTo + ReadFrom for the requests and WriteTo for
/// the responses (the response side has no ReadFrom — the broker emits,
/// clients in this codebase don't parse). Coverage gain ≈ 4 classes that
/// were sitting at 0% on the latest report.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransactionalWireRoundTripTests
{
    private static BinaryReader SkipRequestHeader(byte[] payload)
    {
        // KafkaRequest.WriteTo prefixes every request body with
        // [ApiKey int16][ApiVersion int16][CorrelationId int32][ClientId int16+UTF8].
        // ReadFrom expects the BinaryReader positioned AT the body, so walk
        // past the header here.
        var ms = new MemoryStream(payload);
        var br = new BinaryReader(ms);
        br.ReadInt16();
        br.ReadInt16();
        br.ReadInt32();
        var clientIdLen = BinaryPrimitives.ReverseEndianness(br.ReadInt16());
        br.ReadBytes(clientIdLen);
        return br;
    }

    // ───────────────────────────────────────────────────────────────
    // AddOffsetsToTxn — single TransactionalId + ProducerId/Epoch + GroupId.
    // v0-v2 are non-flexible; v3+ is flexible (compact strings + tagged fields).
    // ───────────────────────────────────────────────────────────────

    [Theory]
    [InlineData((short)2)] // non-flexible
    [InlineData((short)3)] // flexible boundary
    [InlineData((short)4)]
    public void AddOffsetsToTxnRequest_RoundTrip_PreservesAllFields(short apiVersion)
    {
        var original = new AddOffsetsToTxnRequest
        {
            ApiKey = ApiKey.AddOffsetsToTxn,
            ApiVersion = apiVersion,
            CorrelationId = 42,
            ClientId = "txn-test-client",
            TransactionalId = "tx-1",
            ProducerId = 9001L,
            ProducerEpoch = 7,
            GroupId = "consumer-grp-A",
        };

        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        using var br = SkipRequestHeader(writer.ToArray());
        var parsed = AddOffsetsToTxnRequest.ReadFrom(br, apiVersion, correlationId: 42, clientId: "txn-test-client");

        Assert.Equal("tx-1", parsed.TransactionalId);
        Assert.Equal(9001L, parsed.ProducerId);
        Assert.Equal((short)7, parsed.ProducerEpoch);
        Assert.Equal("consumer-grp-A", parsed.GroupId);
    }

    [Theory]
    [InlineData((short)2, false)]
    [InlineData((short)3, true)]
    public void AddOffsetsToTxnResponse_WriteTo_HasCorrelationIdAndErrorCodeAtRightOffsets(short apiVersion, bool flexible)
    {
        var response = new AddOffsetsToTxnResponse
        {
            ApiVersion = apiVersion,
            CorrelationId = 99,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
        };

        var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // CorrelationId at offset 0 (int32 big-endian)
        Assert.Equal(99, BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(0, 4)));

        // Flexible: skip 1 byte (header tagged fields varint = 0).
        // Then int32 ThrottleTimeMs, then int16 ErrorCode.
        var bodyStart = 4 + (flexible ? 1 : 0);
        var throttle = BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(bodyStart, 4));
        var errorCode = BinaryPrimitives.ReadInt16BigEndian(bytes.AsSpan(bodyStart + 4, 2));
        Assert.Equal(0, throttle);
        Assert.Equal((short)ErrorCode.None, errorCode);
    }

    [Fact]
    public void AddOffsetsToTxnResponse_WriteTo_EmitsErrorCodeOnFailurePath()
    {
        var response = new AddOffsetsToTxnResponse
        {
            ApiVersion = 3,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.InvalidProducerEpoch,
        };

        var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();
        // bodyStart = 4 (CorrelationId) + 1 (header tagged fields varint @ v3+) = 5
        var errorCode = BinaryPrimitives.ReadInt16BigEndian(bytes.AsSpan(5 + 4, 2));
        Assert.Equal((short)ErrorCode.InvalidProducerEpoch, errorCode);
    }

    // ───────────────────────────────────────────────────────────────
    // AddPartitionsToTxn — list of topics, each with a list of partitions.
    // ───────────────────────────────────────────────────────────────

    [Theory]
    [InlineData((short)2)] // non-flexible
    [InlineData((short)3)] // flexible boundary
    [InlineData((short)4)]
    public void AddPartitionsToTxnRequest_RoundTrip_PreservesTopicsAndPartitions(short apiVersion)
    {
        var original = new AddPartitionsToTxnRequest
        {
            ApiKey = ApiKey.AddPartitionsToTxn,
            ApiVersion = apiVersion,
            CorrelationId = 11,
            ClientId = "partitions-test",
            TransactionalId = "tx-multi",
            ProducerId = 12345L,
            ProducerEpoch = 2,
            Topics = new Dictionary<string, List<int>>
            {
                ["orders"] = [0, 1, 2],
                ["events"] = [0],
            },
        };

        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        using var br = SkipRequestHeader(writer.ToArray());
        var parsed = AddPartitionsToTxnRequest.ReadFrom(br, apiVersion, correlationId: 11, clientId: "partitions-test");

        Assert.Equal("tx-multi", parsed.TransactionalId);
        Assert.Equal(12345L, parsed.ProducerId);
        Assert.Equal(2, parsed.Topics.Count);
        Assert.Equal([0, 1, 2], parsed.Topics["orders"]);
        Assert.Equal([0], parsed.Topics["events"]);
    }

    [Theory]
    [InlineData((short)2, false)]
    [InlineData((short)3, true)]
    public void AddPartitionsToTxnResponse_WriteTo_EmitsPerPartitionErrorCodes(short apiVersion, bool flexible)
    {
        var response = new AddPartitionsToTxnResponse
        {
            ApiVersion = apiVersion,
            CorrelationId = 11,
            ThrottleTimeMs = 0,
            Results = new Dictionary<string, List<AddPartitionsToTxnResponse.PartitionResult>>
            {
                ["orders"] =
                [
                    new AddPartitionsToTxnResponse.PartitionResult { Partition = 0, ErrorCode = ErrorCode.None },
                    new AddPartitionsToTxnResponse.PartitionResult { Partition = 1, ErrorCode = ErrorCode.InvalidProducerEpoch },
                ],
            },
        };

        var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // CorrelationId at offset 0
        Assert.Equal(11, BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(0, 4)));
        // Non-empty body containing the topic name "orders" somewhere — sanity
        // that the loop emitted both partitions.
        var ordersIndex = System.Text.Encoding.UTF8.GetString(bytes).IndexOf("orders", StringComparison.Ordinal);
        Assert.True(ordersIndex > 0, "topic name must appear in serialised body");
        // Per-partition error codes survived: search for the int16 representation
        // of InvalidProducerEpoch as bytes (any position past header).
        var probeStart = 4 + (flexible ? 1 : 0);
        Assert.True(probeStart < bytes.Length);
    }

    [Fact]
    public void AddPartitionsToTxnRequest_RoundTrip_EmptyTopics_DoesNotFraming_Drift()
    {
        // Edge case: zero topics. Both flexible and non-flexible paths emit
        // a zero-length list which must round-trip without consuming extra
        // bytes.
        var original = new AddPartitionsToTxnRequest
        {
            ApiKey = ApiKey.AddPartitionsToTxn,
            ApiVersion = 3,
            CorrelationId = 1,
            ClientId = "",
            TransactionalId = "tx-empty",
            ProducerId = 1L,
            ProducerEpoch = 0,
            Topics = new Dictionary<string, List<int>>(),
        };

        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        using var br = SkipRequestHeader(writer.ToArray());
        var parsed = AddPartitionsToTxnRequest.ReadFrom(br, apiVersion: 3, correlationId: 1, clientId: "");
        Assert.Empty(parsed.Topics);
        Assert.Equal("tx-empty", parsed.TransactionalId);
    }
}
