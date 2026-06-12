using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Routing;

/// <summary>
/// Adapter that turns a plain <see cref="Func{T1,T2,T3,T4,TResult}"/>
/// (the LogManager.AppendBatchAsync signature) into an
/// <see cref="IPartitionAppender"/>. Lets the broker wire up the
/// default replicated-mode appender without making the Disaggregated
/// project depend on Core's LogManager type.
/// </summary>
public sealed class DelegatingPartitionAppender : IPartitionAppender
{
    private readonly Func<TopicPartition, ReadOnlyMemory<byte>, int, CancellationToken, Task<long>> _append;

    public DelegatingPartitionAppender(
        Func<TopicPartition, ReadOnlyMemory<byte>, int, CancellationToken, Task<long>> append)
    {
        _append = append;
    }

    public Task<long> AppendBatchAsync(
        TopicPartition partition,
        ReadOnlyMemory<byte> recordBatch,
        int recordCount,
        CancellationToken cancellationToken = default)
        => _append(partition, recordBatch, recordCount, cancellationToken);
}
