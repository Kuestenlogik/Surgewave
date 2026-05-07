using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Core.Pipeline;

/// <summary>
/// Request to read messages from a partition
/// </summary>
public sealed record ReadRequest(
    TopicPartition TopicPartition,
    long StartOffset,
    int MaxMessages,
    TaskCompletionSource<List<Message>> CompletionSource,
    CancellationToken CancellationToken);
