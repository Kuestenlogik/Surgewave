using Kuestenlogik.Surgewave.Broker.Native.Handlers;
using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

namespace Kuestenlogik.Surgewave.Broker.Native.Operations.Transactions;

// --- Begin ---

/// <summary>
/// Result for cross-topic transaction begin operation.
/// </summary>
public readonly record struct CrossTopicTxnBeginResult : IOperationResult
{
    public required CrossTopicTxnBeginResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to begin a cross-topic transaction.
/// </summary>
public sealed class CrossTopicTxnBeginOperation : IOperationHandler<CrossTopicTxnBeginRequestPayload, CrossTopicTxnBeginResult>
{
    private readonly CrossTopicTransactionManager _manager;

    public CrossTopicTxnBeginOperation(CrossTopicTransactionManager manager)
    {
        _manager = manager;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CrossTopicTxnBegin;

    public CrossTopicTxnBeginRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => CrossTopicTxnBeginRequestPayload.Read(ref reader);

    public void ValidateRequest(in CrossTopicTxnBeginRequestPayload request) { }

    public Task<CrossTopicTxnBeginResult> ExecuteAsync(CrossTopicTxnBeginRequestPayload request, CancellationToken cancellationToken)
    {
        TimeSpan? timeout = request.TimeoutSeconds > 0 ? TimeSpan.FromSeconds(request.TimeoutSeconds) : null;
        var txn = _manager.Begin(request.ProducerId, timeout);

        var response = new CrossTopicTxnBeginResponsePayload
        {
            ErrorCode = (ushort)SurgewaveErrorCode.None,
            TransactionId = txn.TransactionId
        };

        return Task.FromResult(new CrossTopicTxnBeginResult { Response = response, ErrorCode = SurgewaveErrorCode.None });
    }

    public void WriteResponse(IPayloadWriter writer, in CrossTopicTxnBeginResult response)
        => response.Response.WriteTo(writer);
}

// --- AddWrite ---

/// <summary>
/// Result for cross-topic transaction add write operation.
/// </summary>
public readonly record struct CrossTopicTxnAddWriteResult : IOperationResult
{
    public required CrossTopicTxnAddWriteResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to add a write to a cross-topic transaction.
/// </summary>
public sealed class CrossTopicTxnAddWriteOperation : IOperationHandler<CrossTopicTxnAddWriteRequestPayload, CrossTopicTxnAddWriteResult>
{
    private readonly CrossTopicTransactionManager _manager;

    public CrossTopicTxnAddWriteOperation(CrossTopicTransactionManager manager)
    {
        _manager = manager;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CrossTopicTxnAddWrite;

    public CrossTopicTxnAddWriteRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => CrossTopicTxnAddWriteRequestPayload.Read(ref reader);

    public void ValidateRequest(in CrossTopicTxnAddWriteRequestPayload request) { }

    public Task<CrossTopicTxnAddWriteResult> ExecuteAsync(CrossTopicTxnAddWriteRequestPayload request, CancellationToken cancellationToken)
    {
        try
        {
            _manager.AddWrite(request.TransactionId, request.Topic, request.Partition, request.Key, request.Value);
            var txn = _manager.GetTransaction(request.TransactionId);

            var response = new CrossTopicTxnAddWriteResponsePayload
            {
                ErrorCode = (ushort)SurgewaveErrorCode.None,
                PendingWriteCount = txn?.PendingWrites.Count ?? 0
            };

            return Task.FromResult(new CrossTopicTxnAddWriteResult { Response = response, ErrorCode = SurgewaveErrorCode.None });
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("not found", StringComparison.OrdinalIgnoreCase))
        {
            var response = new CrossTopicTxnAddWriteResponsePayload
            {
                ErrorCode = (ushort)SurgewaveErrorCode.CrossTopicTxnNotFound,
                PendingWriteCount = 0
            };
            return Task.FromResult(new CrossTopicTxnAddWriteResult { Response = response, ErrorCode = SurgewaveErrorCode.CrossTopicTxnNotFound });
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("max pending", StringComparison.OrdinalIgnoreCase))
        {
            var response = new CrossTopicTxnAddWriteResponsePayload
            {
                ErrorCode = (ushort)SurgewaveErrorCode.CrossTopicTxnMaxWritesExceeded,
                PendingWriteCount = 0
            };
            return Task.FromResult(new CrossTopicTxnAddWriteResult { Response = response, ErrorCode = SurgewaveErrorCode.CrossTopicTxnMaxWritesExceeded });
        }
    }

    public void WriteResponse(IPayloadWriter writer, in CrossTopicTxnAddWriteResult response)
        => response.Response.WriteTo(writer);
}

// --- Commit ---

/// <summary>
/// Result for cross-topic transaction commit operation.
/// </summary>
public readonly record struct CrossTopicTxnCommitResult : IOperationResult
{
    public required CrossTopicTxnCommitResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to commit a cross-topic transaction.
/// </summary>
public sealed class CrossTopicTxnCommitOperation : IOperationHandler<CrossTopicTxnCommitRequestPayload, CrossTopicTxnCommitResult>
{
    private readonly CrossTopicTransactionManager _manager;

    public CrossTopicTxnCommitOperation(CrossTopicTransactionManager manager)
    {
        _manager = manager;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CrossTopicTxnCommit;

    public CrossTopicTxnCommitRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => CrossTopicTxnCommitRequestPayload.Read(ref reader);

    public void ValidateRequest(in CrossTopicTxnCommitRequestPayload request) { }

    public async Task<CrossTopicTxnCommitResult> ExecuteAsync(CrossTopicTxnCommitRequestPayload request, CancellationToken cancellationToken)
    {
        var result = await _manager.CommitAsync(request.TransactionId, cancellationToken);

        var errorCode = result.Success ? SurgewaveErrorCode.None : SurgewaveErrorCode.CrossTopicTxnCommitFailed;
        var response = new CrossTopicTxnCommitResponsePayload
        {
            ErrorCode = (ushort)errorCode,
            TopicsWritten = result.TopicsWritten,
            MessagesWritten = result.MessagesWritten,
            DurationMs = (long)result.Duration.TotalMilliseconds,
            Error = result.Error
        };

        return new CrossTopicTxnCommitResult { Response = response, ErrorCode = errorCode };
    }

    public void WriteResponse(IPayloadWriter writer, in CrossTopicTxnCommitResult response)
        => response.Response.WriteTo(writer);
}

// --- Abort ---

/// <summary>
/// Result for cross-topic transaction abort operation.
/// </summary>
public readonly record struct CrossTopicTxnAbortResult : IOperationResult
{
    public required CrossTopicTxnAbortResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to abort a cross-topic transaction.
/// </summary>
public sealed class CrossTopicTxnAbortOperation : IOperationHandler<CrossTopicTxnAbortRequestPayload, CrossTopicTxnAbortResult>
{
    private readonly CrossTopicTransactionManager _manager;

    public CrossTopicTxnAbortOperation(CrossTopicTransactionManager manager)
    {
        _manager = manager;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CrossTopicTxnAbort;

    public CrossTopicTxnAbortRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => CrossTopicTxnAbortRequestPayload.Read(ref reader);

    public void ValidateRequest(in CrossTopicTxnAbortRequestPayload request) { }

    public async Task<CrossTopicTxnAbortResult> ExecuteAsync(CrossTopicTxnAbortRequestPayload request, CancellationToken cancellationToken)
    {
        await _manager.AbortAsync(request.TransactionId, cancellationToken);

        var response = new CrossTopicTxnAbortResponsePayload
        {
            ErrorCode = (ushort)SurgewaveErrorCode.None
        };

        return new CrossTopicTxnAbortResult { Response = response, ErrorCode = SurgewaveErrorCode.None };
    }

    public void WriteResponse(IPayloadWriter writer, in CrossTopicTxnAbortResult response)
        => response.Response.WriteTo(writer);
}
