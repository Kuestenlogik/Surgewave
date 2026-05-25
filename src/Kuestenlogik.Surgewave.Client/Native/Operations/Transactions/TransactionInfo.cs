namespace Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;

/// <summary>
/// Transaction information.
/// </summary>
public record TransactionInfo(string TransactionalId, string State, long ProducerId, short ProducerEpoch);
