using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — Transaction payloads not yet pinned. The
/// AddOffsets / AddPartitions / EndTxn / InitProducerId / TxnOffsetCommit
/// already sit at 42-100% via the upstream Kafka-protocol tests; this
/// batch closes the two remaining 0% subgroups:
///
/// CrossTopicTxn (KL-Surgewave-specific cross-topic transactions —
/// Begin / AddWrite / Commit / Abort, each Request + Response)
///
/// DescribeTransactions (the admin RPC that lists in-flight
/// transactions — Request + Response, plus the nested
/// TransactionDescription / TransactionPartition records).
///
/// CrossTopicTxn is a Surgewave-native extension on top of Kafka's
/// per-topic transaction semantics; framing regressions here corrupt
/// the cross-topic commit path and surface as silent partial commits.
/// </summary>
public sealed class TransactionPayloadRoundTripTests
{
    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // CrossTopicTxnBegin
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void CrossTopicTxnBeginRequest_RoundTrip_PreservesAllFields()
    {
        var original = new CrossTopicTxnBeginRequestPayload
        {
            ProducerId = "producer-1",
            TimeoutSeconds = 60,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CrossTopicTxnBeginRequestPayload.Read(ref r); });

        Assert.Equal("producer-1", parsed.ProducerId);
        Assert.Equal(60, parsed.TimeoutSeconds);
    }

    [Fact]
    public void CrossTopicTxnBeginRequest_NullProducerId_RoundTrips()
    {
        // Anonymous client (no explicit ProducerId) — broker assigns one.
        var original = new CrossTopicTxnBeginRequestPayload
        {
            ProducerId = null,
            TimeoutSeconds = 30,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CrossTopicTxnBeginRequestPayload.Read(ref r); });
        Assert.Null(parsed.ProducerId);
    }

    [Fact]
    public void CrossTopicTxnBeginResponse_RoundTrip_PreservesAllFields()
    {
        var original = new CrossTopicTxnBeginResponsePayload
        {
            ErrorCode = 0,
            TransactionId = "txn-abc-123",
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CrossTopicTxnBeginResponsePayload.Read(ref r); });

        Assert.Equal((ushort)0, parsed.ErrorCode);
        Assert.Equal("txn-abc-123", parsed.TransactionId);
    }

    // ───────────────────────────────────────────────────────────────
    // CrossTopicTxnAddWrite (carries the actual record)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void CrossTopicTxnAddWriteRequest_WithKey_RoundTrips()
    {
        var key = System.Text.Encoding.UTF8.GetBytes("order-123");
        var value = new byte[] { 0x01, 0x02, 0x03, 0x04 };
        var original = new CrossTopicTxnAddWriteRequestPayload
        {
            TransactionId = "txn-1",
            Topic = "orders",
            Partition = 0,
            Key = key,
            Value = value,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CrossTopicTxnAddWriteRequestPayload.Read(ref r); });

        Assert.Equal("orders", parsed.Topic);
        Assert.Equal(key, parsed.Key);
        Assert.Equal(value, parsed.Value);
    }

    [Fact]
    public void CrossTopicTxnAddWriteRequest_NullKey_RoundTrips()
    {
        // Tombstone or compaction-target with no key.
        var value = new byte[] { 0xAB };
        var original = new CrossTopicTxnAddWriteRequestPayload
        {
            TransactionId = "txn-1",
            Topic = "events",
            Partition = 3,
            Key = null,
            Value = value,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CrossTopicTxnAddWriteRequestPayload.Read(ref r); });

        Assert.Null(parsed.Key);
        Assert.Equal(value, parsed.Value);
    }

    [Fact]
    public void CrossTopicTxnAddWriteResponse_RoundTrip_PreservesAllFields()
    {
        var original = new CrossTopicTxnAddWriteResponsePayload
        {
            ErrorCode = 0,
            PendingWriteCount = 42,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CrossTopicTxnAddWriteResponsePayload.Read(ref r); });
        Assert.Equal(42, parsed.PendingWriteCount);
    }

    // ───────────────────────────────────────────────────────────────
    // CrossTopicTxnCommit
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void CrossTopicTxnCommitRequest_RoundTrip_PreservesTransactionId()
    {
        var original = new CrossTopicTxnCommitRequestPayload { TransactionId = "txn-1" };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CrossTopicTxnCommitRequestPayload.Read(ref r); });
        Assert.Equal("txn-1", parsed.TransactionId);
    }

    [Fact]
    public void CrossTopicTxnCommitResponse_FullSuccess_RoundTrips()
    {
        var original = new CrossTopicTxnCommitResponsePayload
        {
            ErrorCode = 0,
            TopicsWritten = 3,
            MessagesWritten = 1_500,
            DurationMs = 17L,
            Error = null,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CrossTopicTxnCommitResponsePayload.Read(ref r); });

        Assert.Equal((ushort)0, parsed.ErrorCode);
        Assert.Equal(3, parsed.TopicsWritten);
        Assert.Equal(1_500, parsed.MessagesWritten);
        Assert.Equal(17L, parsed.DurationMs);
        Assert.Null(parsed.Error);
    }

    [Fact]
    public void CrossTopicTxnCommitResponse_FailureWithError_RoundTrips()
    {
        var original = new CrossTopicTxnCommitResponsePayload
        {
            ErrorCode = 50, // INVALID_TXN_STATE
            TopicsWritten = 0,
            MessagesWritten = 0,
            DurationMs = 5L,
            Error = "Transaction txn-1 already aborted before commit",
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CrossTopicTxnCommitResponsePayload.Read(ref r); });

        Assert.Equal((ushort)50, parsed.ErrorCode);
        Assert.Contains("already aborted", parsed.Error);
    }

    // ───────────────────────────────────────────────────────────────
    // CrossTopicTxnAbort
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void CrossTopicTxnAbortRequestResponse_RoundTrip()
    {
        var req = new CrossTopicTxnAbortRequestPayload { TransactionId = "txn-1" };
        var reqParsed = RoundTrip(
            req.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); req.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CrossTopicTxnAbortRequestPayload.Read(ref r); });
        Assert.Equal("txn-1", reqParsed.TransactionId);

        var resp = new CrossTopicTxnAbortResponsePayload { ErrorCode = 0 };
        var respParsed = RoundTrip(
            resp.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); resp.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CrossTopicTxnAbortResponsePayload.Read(ref r); });
        Assert.Equal((ushort)0, respParsed.ErrorCode);
    }

    // ───────────────────────────────────────────────────────────────
    // DescribeTransactions
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DescribeTransactionsRequest_RoundTrip_PreservesTxnIds()
    {
        var original = new DescribeTransactionsRequestPayload
        {
            TransactionalIds = new List<string> { "tx-1", "tx-2", "tx-3" },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeTransactionsRequestPayload.Read(ref r); });

        Assert.Equal(new[] { "tx-1", "tx-2", "tx-3" }, parsed.TransactionalIds);
    }

    [Fact]
    public void TransactionPartition_RoundTrip_PreservesAllFields()
    {
        var original = new TransactionPartition { Topic = "orders", Partition = 7 };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return TransactionPartition.Read(ref r); });

        Assert.Equal("orders", parsed.Topic);
        Assert.Equal(7, parsed.Partition);
    }

    [Fact]
    public void TransactionDescription_RoundTrip_PreservesAllFields()
    {
        var original = new TransactionDescription
        {
            TransactionalId = "tx-1",
            ErrorCode = 0,
            State = "Ongoing",
            ProducerId = 12345L,
            ProducerEpoch = 4,
            Partitions = new List<TransactionPartition>
            {
                new() { Topic = "orders", Partition = 0 },
                new() { Topic = "orders", Partition = 1 },
                new() { Topic = "audit",  Partition = 0 },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return TransactionDescription.Read(ref r); });

        Assert.Equal("tx-1", parsed.TransactionalId);
        Assert.Equal("Ongoing", parsed.State);
        Assert.Equal(12345L, parsed.ProducerId);
        Assert.Equal((short)4, parsed.ProducerEpoch);
        Assert.Equal(3, parsed.Partitions.Count);
        Assert.Equal("audit", parsed.Partitions[2].Topic);
    }

    [Fact]
    public void DescribeTransactionsResponse_FullShape_RoundTrips()
    {
        var original = new DescribeTransactionsResponsePayload
        {
            ErrorCode = 0,
            Transactions = new List<TransactionDescription>
            {
                new()
                {
                    TransactionalId = "tx-1",
                    ErrorCode = 0,
                    State = "Ongoing",
                    ProducerId = 100,
                    ProducerEpoch = 1,
                    Partitions = new List<TransactionPartition>
                    {
                        new() { Topic = "orders", Partition = 0 },
                    },
                },
                new()
                {
                    TransactionalId = "tx-2",
                    ErrorCode = 50, // INVALID_TXN_STATE
                    State = "PrepareAbort",
                    ProducerId = 200,
                    ProducerEpoch = 2,
                    Partitions = new List<TransactionPartition>(),
                },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeTransactionsResponsePayload.Read(ref r); });

        Assert.Equal((ushort)0, parsed.ErrorCode);
        Assert.Equal(2, parsed.Transactions.Count);
        Assert.Equal("tx-1", parsed.Transactions[0].TransactionalId);
        Assert.Single(parsed.Transactions[0].Partitions);
        Assert.Equal((ushort)50, parsed.Transactions[1].ErrorCode);
        Assert.Empty(parsed.Transactions[1].Partitions);
    }
}
