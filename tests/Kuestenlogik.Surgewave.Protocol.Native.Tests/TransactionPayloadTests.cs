using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Tests for transaction payload serialization/deserialization
/// </summary>
public sealed class TransactionPayloadTests
{
    [Fact]
    public void InitProducerIdRequest_RoundTrip_WithTransactionalId()
    {
        var payload = new InitProducerIdRequestPayload
        {
            TransactionalId = "my-txn-id",
            TransactionTimeoutMs = 60000,
            ProducerId = 42L,
            ProducerEpoch = 3
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = InitProducerIdRequestPayload.Read(ref reader);

        Assert.Equal("my-txn-id", parsed.TransactionalId);
        Assert.Equal(60000, parsed.TransactionTimeoutMs);
        Assert.Equal(42L, parsed.ProducerId);
        Assert.Equal(3, parsed.ProducerEpoch);
    }

    [Fact]
    public void InitProducerIdRequest_RoundTrip_NullTransactionalId()
    {
        var payload = new InitProducerIdRequestPayload
        {
            TransactionalId = null,
            TransactionTimeoutMs = 30000,
            ProducerId = -1L,
            ProducerEpoch = 0
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = InitProducerIdRequestPayload.Read(ref reader);

        Assert.Null(parsed.TransactionalId);
        Assert.Equal(30000, parsed.TransactionTimeoutMs);
        Assert.Equal(-1L, parsed.ProducerId);
        Assert.Equal(0, parsed.ProducerEpoch);
    }

    [Fact]
    public void InitProducerIdResponse_RoundTrip()
    {
        var payload = new InitProducerIdResponsePayload
        {
            ErrorCode = 0,
            ProducerId = 12345L,
            ProducerEpoch = 7
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = InitProducerIdResponsePayload.Read(ref reader);

        Assert.Equal((ushort)0, parsed.ErrorCode);
        Assert.Equal(12345L, parsed.ProducerId);
        Assert.Equal(7, parsed.ProducerEpoch);
    }

    [Fact]
    public void EndTxnRequest_RoundTrip_Committed()
    {
        var payload = new EndTxnRequestPayload
        {
            TransactionalId = "txn-abc",
            ProducerId = 99L,
            ProducerEpoch = 2,
            Committed = true
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = EndTxnRequestPayload.Read(ref reader);

        Assert.Equal("txn-abc", parsed.TransactionalId);
        Assert.Equal(99L, parsed.ProducerId);
        Assert.Equal(2, parsed.ProducerEpoch);
        Assert.True(parsed.Committed);
    }

    [Fact]
    public void EndTxnRequest_RoundTrip_Aborted()
    {
        var payload = new EndTxnRequestPayload
        {
            TransactionalId = "txn-xyz",
            ProducerId = 50L,
            ProducerEpoch = 1,
            Committed = false
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = EndTxnRequestPayload.Read(ref reader);

        Assert.Equal("txn-xyz", parsed.TransactionalId);
        Assert.False(parsed.Committed);
    }

    [Fact]
    public void EndTxnResponse_RoundTrip()
    {
        var payload = new EndTxnResponsePayload { ErrorCode = 32 }; // InvalidTxnState

        var buffer = new byte[payload.EstimateSize() + 5];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = EndTxnResponsePayload.Read(ref reader);

        Assert.Equal((ushort)32, parsed.ErrorCode);
    }

    [Fact]
    public void InitProducerIdResponse_EstimateSize_IsCorrect()
    {
        var payload = new InitProducerIdResponsePayload
        {
            ErrorCode = 0,
            ProducerId = 1L,
            ProducerEpoch = 0
        };

        // 2 (ErrorCode) + 8 (ProducerId) + 2 (ProducerEpoch) = 12
        Assert.Equal(12, payload.EstimateSize());
    }

    [Fact]
    public void EndTxnResponse_EstimateSize_IsCorrect()
    {
        var payload = new EndTxnResponsePayload { ErrorCode = 0 };
        Assert.Equal(2, payload.EstimateSize());
    }
}
