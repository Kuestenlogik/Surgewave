namespace Kuestenlogik.Surgewave.Core.Observability;

/// <summary>
/// Kind of broker-side event surfaced via
/// <see cref="ISurgewaveBrokerObservability"/>. Each kind maps to a
/// specific pipeline stage inside the broker:
/// <list type="bullet">
///   <item><see cref="Produced"/> — a record was successfully
///   appended to the log (post-acknowledge, pre-replication for the
///   interested observers).</item>
///   <item><see cref="Consumed"/> — a record was dispatched to one
///   or more consumer groups; <see cref="SurgewaveBrokerEvent.Consumers"/>
///   names them.</item>
///   <item><see cref="Rejected"/> — the broker refused the record.
///   <see cref="SurgewaveBrokerEvent.RejectReason"/> carries the
///   human-readable cause (ACL deny, schema mismatch, payload too
///   large, quota exceeded).</item>
///   <item><see cref="Rebalanced"/> — a consumer group finished
///   rebalancing. Partition ownership after rebalance is surfaced
///   via subsequent <see cref="Consumed"/> events; this kind is a
///   signal only.</item>
/// </list>
/// </summary>
public enum SurgewaveBrokerEventKind
{
    /// <summary>A record was successfully produced to a partition.</summary>
    Produced,

    /// <summary>A record was delivered to one or more consumer groups.</summary>
    Consumed,

    /// <summary>The broker rejected a produce or fetch request.</summary>
    Rejected,

    /// <summary>A consumer group finished rebalancing.</summary>
    Rebalanced,
}

/// <summary>
/// Snapshot of one event as observed inside the broker pipeline.
/// Immutable; emitted by <see cref="ISurgewaveBrokerObservability"/> to
/// observers that hang off the broker in-process (the Bowire
/// <c>surgewave://embedded</c> tap is the reference consumer).
/// </summary>
/// <param name="Kind">Which pipeline stage emitted the event.</param>
/// <param name="Topic">Target topic.</param>
/// <param name="Partition">Target partition; -1 when the event is
/// cluster-scoped (e.g. <see cref="SurgewaveBrokerEventKind.Rebalanced"/>).</param>
/// <param name="Offset">Log offset. <c>null</c> for rejected produces
/// (nothing made it to the log) and rebalance events.</param>
/// <param name="Principal">Auth subject of the producing / consuming
/// client. <c>null</c> on anonymous / internal paths.</param>
/// <param name="RejectReason">Populated only on
/// <see cref="SurgewaveBrokerEventKind.Rejected"/>.</param>
/// <param name="Consumers">Populated on
/// <see cref="SurgewaveBrokerEventKind.Consumed"/> — the consumer-group
/// ids that received this record. Multiple groups land in a single
/// event when fan-out delivers the record to each simultaneously.
/// Also populated on <see cref="SurgewaveBrokerEventKind.Rebalanced"/>
/// with the single group id whose ownership just changed.</param>
/// <param name="Key">Message key bytes; <c>null</c> for
/// keyless-produce or events without a record.</param>
/// <param name="Value">Message value bytes; <c>null</c> for tombstones
/// and rebalance events.</param>
/// <param name="Timestamp">When the broker observed the event.</param>
public sealed record SurgewaveBrokerEvent(
    SurgewaveBrokerEventKind Kind,
    string Topic,
    int Partition,
    long? Offset,
    string? Principal,
    string? RejectReason,
    IReadOnlyList<string>? Consumers,
    byte[]? Key,
    byte[]? Value,
    DateTimeOffset Timestamp);
