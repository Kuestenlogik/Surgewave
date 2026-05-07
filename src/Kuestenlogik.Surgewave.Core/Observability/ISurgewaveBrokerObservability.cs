namespace Kuestenlogik.Surgewave.Core.Observability;

/// <summary>
/// Broker-side observability hook. Lets in-process consumers — the
/// Bowire workbench's <c>surgewave://embedded</c> tap is the primary
/// reference consumer — subscribe to every <see cref="SurgewaveBrokerEvent"/>
/// the broker emits, including produce rejections, consumer-group
/// deliveries, and rebalance notifications that an external client
/// can never see.
/// </summary>
/// <remarks>
/// <para>
/// The broker registers exactly one implementation in DI when it
/// boots; <see cref="ObserveAsync"/> multiplexes the event stream so
/// several observers can subscribe without each getting their own
/// copy of the hot path. Implementations use a bounded channel with
/// a drop-policy so a slow observer can't back-pressure
/// production — dropped events log a warning but never halt the
/// broker.
/// </para>
/// <para>
/// Payload semantics: events carry the raw <c>Key</c> / <c>Value</c>
/// bytes as seen on the wire; schema-registry decoding is the
/// consumer's concern. Auth principals are surfaced so tools can
/// correlate who sent a record that later got rejected or consumed
/// by which group.
/// </para>
/// </remarks>
public interface ISurgewaveBrokerObservability
{
    /// <summary>
    /// <c>true</c> when at least one observer is currently subscribed via
    /// <see cref="ObserveAsync"/>. Broker hot-path code must check this
    /// before constructing a <see cref="SurgewaveBrokerEvent"/> so unused
    /// observability paths pay zero allocation cost.
    /// </summary>
    /// <remarks>
    /// The read is lock-free and safe to call from any thread. Values
    /// may lag the actual subscriber-count by a few nanoseconds —
    /// callers that see an outdated <c>true</c> just allocate an event
    /// that gets dropped in <see cref="SurgewaveBrokerObservability"/>'s
    /// empty-subscribers fast path; callers that see an outdated
    /// <c>false</c> miss an event for a single publish, which is
    /// acceptable under the tap's at-most-once contract.
    /// </remarks>
    bool HasSubscribers { get; }

    /// <summary>
    /// Subscribe to the live stream of broker events. Each invocation
    /// returns a fresh async-enumerable; cancellation of the passed
    /// token terminates the enumeration cleanly without affecting
    /// other observers.
    /// </summary>
    /// <param name="ct">Token that ends the subscription.</param>
    /// <returns>Asynchronous stream of events.</returns>
    IAsyncEnumerable<SurgewaveBrokerEvent> ObserveAsync(CancellationToken ct = default);
}
