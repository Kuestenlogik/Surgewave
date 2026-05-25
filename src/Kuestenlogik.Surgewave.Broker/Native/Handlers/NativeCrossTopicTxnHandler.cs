using Kuestenlogik.Surgewave.Broker.Native.Operations.Transactions;
using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol cross-topic transaction operations.
/// </summary>
public sealed class NativeCrossTopicTxnHandler : NativeHandlerBase
{
    private readonly CrossTopicTransactionManager? _manager;

    public NativeCrossTopicTxnHandler(CrossTopicTransactionManager? manager)
    {
        _manager = manager;

        Register<CrossTopicTxnBeginRequestPayload, CrossTopicTxnBeginResult>(
            SurgewaveOpCode.CrossTopicTxnBegin, _ => new CrossTopicTxnBeginOperation(manager!));
        Register<CrossTopicTxnAddWriteRequestPayload, CrossTopicTxnAddWriteResult>(
            SurgewaveOpCode.CrossTopicTxnAddWrite, _ => new CrossTopicTxnAddWriteOperation(manager!));
        Register<CrossTopicTxnCommitRequestPayload, CrossTopicTxnCommitResult>(
            SurgewaveOpCode.CrossTopicTxnCommit, _ => new CrossTopicTxnCommitOperation(manager!));
        Register<CrossTopicTxnAbortRequestPayload, CrossTopicTxnAbortResult>(
            SurgewaveOpCode.CrossTopicTxnAbort, _ => new CrossTopicTxnAbortOperation(manager!));
    }

    protected override Task? PreExecuteCheck(NativeRequestContext context, CancellationToken cancellationToken)
    {
        if (_manager == null)
        {
            return context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.CrossTopicTxnDisabled, "Cross-topic transactions not enabled", cancellationToken);
        }
        return null;
    }
}
