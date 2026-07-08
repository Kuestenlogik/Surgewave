namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// Protocol-neutral transaction listing projection (ListTransactions surface). Shared by the
/// Kafka adapter, the gRPC admin service and the native path (#59).
/// </summary>
public record TransactionListing(string TransactionalId, long ProducerId, string State);
