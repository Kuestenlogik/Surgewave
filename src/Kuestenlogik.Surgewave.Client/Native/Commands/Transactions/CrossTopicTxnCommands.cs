using Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Transactions;

/// <summary>
/// Command to begin a cross-topic transaction.
/// </summary>
public sealed class CrossTopicTxnBeginCommand : ISurgewaveCommand<CrossTopicTxnBeginResponse>
{
    private readonly CrossTopicTxnBeginRequestPayload _request;

    public CrossTopicTxnBeginCommand(string? producerId, int timeoutSeconds)
    {
        _request = new CrossTopicTxnBeginRequestPayload
        {
            ProducerId = producerId,
            TimeoutSeconds = timeoutSeconds
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CrossTopicTxnBegin;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public CrossTopicTxnBeginResponse ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = CrossTopicTxnBeginResponsePayload.Read(ref reader);
        return new CrossTopicTxnBeginResponse((SurgewaveErrorCode)response.ErrorCode, response.TransactionId);
    }
}

/// <summary>
/// Command to add a write to a cross-topic transaction.
/// </summary>
public sealed class CrossTopicTxnAddWriteCommand : ISurgewaveCommand<CrossTopicTxnAddWriteResponse>
{
    private readonly CrossTopicTxnAddWriteRequestPayload _request;

    public CrossTopicTxnAddWriteCommand(string transactionId, string topic, int partition, byte[]? key, byte[] value)
    {
        _request = new CrossTopicTxnAddWriteRequestPayload
        {
            TransactionId = transactionId,
            Topic = topic,
            Partition = partition,
            Key = key,
            Value = value
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CrossTopicTxnAddWrite;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public CrossTopicTxnAddWriteResponse ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = CrossTopicTxnAddWriteResponsePayload.Read(ref reader);
        return new CrossTopicTxnAddWriteResponse((SurgewaveErrorCode)response.ErrorCode, response.PendingWriteCount);
    }
}

/// <summary>
/// Command to commit a cross-topic transaction.
/// </summary>
public sealed class CrossTopicTxnCommitCommand : ISurgewaveCommand<CrossTopicTxnCommitResponse>
{
    private readonly CrossTopicTxnCommitRequestPayload _request;

    public CrossTopicTxnCommitCommand(string transactionId)
    {
        _request = new CrossTopicTxnCommitRequestPayload
        {
            TransactionId = transactionId
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CrossTopicTxnCommit;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public CrossTopicTxnCommitResponse ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = CrossTopicTxnCommitResponsePayload.Read(ref reader);
        return new CrossTopicTxnCommitResponse(
            (SurgewaveErrorCode)response.ErrorCode,
            response.TopicsWritten,
            response.MessagesWritten,
            response.DurationMs,
            response.Error);
    }
}

/// <summary>
/// Command to abort a cross-topic transaction.
/// </summary>
public sealed class CrossTopicTxnAbortCommand : ISurgewaveCommand<SurgewaveErrorCode>
{
    private readonly CrossTopicTxnAbortRequestPayload _request;

    public CrossTopicTxnAbortCommand(string transactionId)
    {
        _request = new CrossTopicTxnAbortRequestPayload
        {
            TransactionId = transactionId
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CrossTopicTxnAbort;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public SurgewaveErrorCode ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = CrossTopicTxnAbortResponsePayload.Read(ref reader);
        return (SurgewaveErrorCode)response.ErrorCode;
    }
}
