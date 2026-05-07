using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;

/// <summary>
/// Result of adding a partition to a transaction.
/// </summary>
public record PartitionTxnResult(int Partition, SurgewaveErrorCode ErrorCode);
