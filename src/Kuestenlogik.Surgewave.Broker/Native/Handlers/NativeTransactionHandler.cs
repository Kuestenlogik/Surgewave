using Kuestenlogik.Surgewave.Broker.Native.Operations.Transactions;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol transaction operations.
/// </summary>
public sealed class NativeTransactionHandler : NativeHandlerBase
{
    private readonly TransactionCoordinator? _transactionCoordinator;

    public NativeTransactionHandler(TransactionCoordinator? transactionCoordinator)
    {
        _transactionCoordinator = transactionCoordinator;

        // Only register operations if coordinator is available (checked in PreExecuteCheck)
        Register<InitProducerIdRequestPayload, InitProducerIdResult>(
            SurgewaveOpCode.InitProducerId, ctx => new InitProducerIdOperation(transactionCoordinator!, ctx.Header.RequestId));
        Register<AddPartitionsToTxnRequestPayload, AddPartitionsToTxnResult>(
            SurgewaveOpCode.AddPartitionsToTxn, ctx => new AddPartitionsToTxnOperation(transactionCoordinator!, ctx.Header.RequestId));
        Register<AddOffsetsToTxnRequest, AddOffsetsToTxnResult>(
            SurgewaveOpCode.AddOffsetsToTxn, ctx => new AddOffsetsToTxnOperation(transactionCoordinator!, ctx.Header.RequestId));
        Register<TxnOffsetCommitRequestPayload, TxnOffsetCommitResult>(
            SurgewaveOpCode.TxnOffsetCommit, ctx => new TxnOffsetCommitOperation(transactionCoordinator!, ctx.Header.RequestId));
        Register<EndTxnRequestPayload, EndTxnResult>(
            SurgewaveOpCode.EndTxn, ctx => new EndTxnOperation(transactionCoordinator!, ctx.Header.RequestId));
        RegisterNoRequest<ListTransactionsResult>(SurgewaveOpCode.ListTransactions, _ => new ListTransactionsOperation());
        Register<DescribeTransactionsRequestPayload, DescribeTransactionsResult>(
            SurgewaveOpCode.DescribeTransactions, _ => new DescribeTransactionsOperation());
    }

    protected override Task? PreExecuteCheck(NativeRequestContext context, CancellationToken cancellationToken)
    {
        if (_transactionCoordinator == null)
        {
            return context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.NotCoordinator, "Transaction coordinator not available", cancellationToken);
        }
        return null;
    }
}
