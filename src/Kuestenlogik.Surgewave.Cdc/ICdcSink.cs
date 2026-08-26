namespace Kuestenlogik.Surgewave.Cdc;

/// <summary>
/// Where captured change events go (#144).
/// </summary>
/// <remarks>
/// <para>
/// Capture used to end in a trace log: the loop computed the topic name,
/// serialised the event and the key, incremented the counter — and dropped all
/// three. Nothing ever appended a CDC event to a topic, so the feature reported
/// healthy sources and rising event counts while producing nothing.
/// </para>
/// <para>
/// The sink is a constructor dependency of <see cref="CdcService"/> rather than
/// an optional one, so a host cannot enable capture without saying where the
/// events go. That is the whole of the defect: not a broken sink, an absent one
/// that nothing forced anybody to supply.
/// </para>
/// <para>
/// Deliberately narrow, and deliberately not the Surgewave client. The broker
/// hosts CDC in-process and can append straight to its own log, so routing the
/// events through a loopback socket would buy nothing; an out-of-process host
/// can implement this over the client instead. The dead
/// <c>Cdc → Client</c> project reference goes with it.
/// </para>
/// </remarks>
public interface ICdcSink
{
    /// <summary>
    /// Appends one change event.
    /// </summary>
    /// <param name="topic">Topic the event belongs to, per the source's naming rules.</param>
    /// <param name="key">Serialised primary key, or <c>null</c> when the row carried none.</param>
    /// <param name="value">The serialised event.</param>
    /// <param name="cancellationToken">Cancels the append.</param>
    /// <remarks>
    /// Implementations decide what a failed append means. Throwing faults the
    /// capture loop for that source and marks it as such, which is the right
    /// answer when the events cannot be replaced; swallowing keeps capture
    /// running and loses the event, which is the right answer only when
    /// something else already holds it.
    /// </remarks>
    ValueTask WriteAsync(
        string topic,
        ReadOnlyMemory<byte>? key,
        ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken = default);
}
