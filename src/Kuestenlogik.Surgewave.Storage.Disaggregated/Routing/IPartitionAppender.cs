using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Routing;

/// <summary>
/// Single-method facade over the partition write path. The broker's
/// Produce handlers (Kafka + Native) call this instead of LogManager
/// directly so the disaggregated routing can intercept without
/// touching the handlers each time a new mode lands.
///
/// The default impl is a thin wrapper around <c>LogManager.AppendBatchAsync</c>;
/// when disaggregated storage is enabled, a routing decorator
/// (<see cref="RoutingPartitionAppender"/>) sits in front and dispatches
/// stateless-mode topics to the <see cref="Stateless.StatelessAgent"/>.
/// </summary>
public interface IPartitionAppender
{
    /// <summary>
    /// Append <paramref name="recordBatch"/> to <paramref name="partition"/>
    /// and return the assigned base offset. Semantics are mode-specific:
    /// for <c>replicated</c> and <c>disaggregated-wal</c> the call returns
    /// after the local append (background flusher carries the disaggregated
    /// part); for <c>disaggregated-stateless</c> the call returns only
    /// after the batch is durable in the object store and the manifest
    /// commit is accepted.
    /// </summary>
    Task<long> AppendBatchAsync(
        TopicPartition partition,
        ReadOnlyMemory<byte> recordBatch,
        int recordCount,
        CancellationToken cancellationToken = default);
}
