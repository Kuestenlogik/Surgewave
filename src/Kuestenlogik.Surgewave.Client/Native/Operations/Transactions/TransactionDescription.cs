using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;

/// <summary>
/// Transaction description.
/// </summary>
public record TransactionDescription(
    string TransactionalId,
    SurgewaveErrorCode ErrorCode,
    string State,
    long ProducerId,
    short ProducerEpoch,
    List<(string Topic, int Partition)> Partitions);
