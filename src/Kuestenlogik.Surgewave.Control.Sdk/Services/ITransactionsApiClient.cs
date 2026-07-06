using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for the broker's transaction inspection APIs:
/// Kafka transactions via /v3/transactions and Surgewave cross-topic
/// transactions via /api/transactions.
/// </summary>
public interface ITransactionsApiClient
{
    /// <summary>List all known Kafka transactions (transactional id, producer id, state).</summary>
    Task<IReadOnlyList<TxnListingModel>> ListKafkaTransactionsAsync(CancellationToken cancellationToken = default);

    /// <summary>Describe Kafka transactions in detail (epoch, timeout, start time, partitions).</summary>
    Task<IReadOnlyList<TxnDescriptionModel>> DescribeKafkaTransactionsAsync(
        IReadOnlyList<string> transactionalIds, CancellationToken cancellationToken = default);

    /// <summary>List active Surgewave cross-topic transactions.</summary>
    Task<IReadOnlyList<CrossTopicTransactionModel>> ListCrossTopicTransactionsAsync(CancellationToken cancellationToken = default);
}
