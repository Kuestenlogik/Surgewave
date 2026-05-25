using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;

/// <summary>
/// Response from beginning a cross-topic transaction.
/// </summary>
public sealed record CrossTopicTxnBeginResponse(SurgewaveErrorCode ErrorCode, string TransactionId);

/// <summary>
/// Response from adding a write to a cross-topic transaction.
/// </summary>
public sealed record CrossTopicTxnAddWriteResponse(SurgewaveErrorCode ErrorCode, int PendingWriteCount);

/// <summary>
/// Response from committing a cross-topic transaction.
/// </summary>
public sealed record CrossTopicTxnCommitResponse(
    SurgewaveErrorCode ErrorCode,
    int TopicsWritten,
    int MessagesWritten,
    long DurationMs,
    string? Error);
