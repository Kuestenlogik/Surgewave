namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// A source task with exactly-once delivery guarantees.
/// Instead of implementing <see cref="SourceTask.PollAsync"/>, subclasses implement
/// <see cref="PollWithOffsetAsync"/> which receives the last committed offset and returns
/// records with explicit source partition/offset metadata.
///
/// The runtime commits source offsets atomically with the produced messages using
/// cross-topic transactions, ensuring no duplicates on crash/restart.
/// </summary>
/// <example>
/// <code>
/// public class MyExactlyOnceTask : ExactlyOnceSourceTask
/// {
///     public override string Version => "1.0.0";
///
///     public override Task&lt;IReadOnlyList&lt;ExactlyOnceSourceRecord&gt;&gt; PollWithOffsetAsync(
///         Dictionary&lt;string, string&gt;? lastOffset, CancellationToken ct)
///     {
///         var position = lastOffset?.GetValueOrDefault("position") is string pos
///             ? long.Parse(pos) : 0L;
///         // Read from position, return records with new offsets
///     }
/// }
/// </code>
/// </example>
public abstract class ExactlyOnceSourceTask : SourceTask
{
    /// <summary>
    /// Gets or sets the offset store used for exactly-once offset tracking.
    /// Set by the runtime before <see cref="Start"/> is called.
    /// </summary>
    public ISourceOffsetStore? OffsetStore { get; internal set; }

    /// <summary>
    /// Gets or sets the connector name for offset tracking.
    /// Set by the runtime before <see cref="Start"/> is called.
    /// </summary>
    public string? ConnectorName { get; internal set; }

    /// <summary>
    /// Gets or sets the source partition identifier for this task.
    /// Defaults to the task ID string if not set explicitly.
    /// </summary>
    public string SourcePartition { get; internal set; } = "default";

    /// <summary>
    /// Poll for new records with offset tracking.
    /// The runtime provides the last committed offset so the task can resume
    /// from the correct position after a crash.
    /// </summary>
    /// <param name="lastOffset">The last committed offset for this partition, or null if starting fresh.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A list of records with source offset metadata.</returns>
    public abstract Task<IReadOnlyList<ExactlyOnceSourceRecord>> PollWithOffsetAsync(
        Dictionary<string, string>? lastOffset,
        CancellationToken ct = default);

    /// <summary>
    /// Standard PollAsync delegates to PollWithOffsetAsync with offset lookup.
    /// This allows ExactlyOnceSourceTask to be used with the standard TaskRunner
    /// as a fallback (at-least-once), but the ExactlyOnceSourcePipeline provides
    /// the full exactly-once guarantees.
    /// </summary>
    public sealed override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        Dictionary<string, string>? lastOffset = null;

        if (OffsetStore != null && ConnectorName != null)
        {
            lastOffset = await OffsetStore.GetOffsetAsync(ConnectorName, SourcePartition, cancellationToken);
        }

        var eosRecords = await PollWithOffsetAsync(lastOffset, cancellationToken);
        if (eosRecords.Count == 0)
        {
            return [];
        }

        // Convert to standard SourceRecords for backward compatibility
        var records = new List<SourceRecord>(eosRecords.Count);
        foreach (var eosRecord in eosRecords)
        {
            records.Add(new SourceRecord
            {
                Topic = eosRecord.Topic,
                Partition = eosRecord.Partition,
                Key = eosRecord.Key,
                Value = eosRecord.Value,
                Timestamp = eosRecord.Timestamp,
                Headers = eosRecord.Headers,
                SourcePartition = new Dictionary<string, object>
                {
                    ["partition"] = eosRecord.SourcePartition
                },
                SourceOffset = eosRecord.SourceOffset.ToDictionary(
                    kvp => kvp.Key, kvp => (object)kvp.Value)
            });
        }

        return records;
    }
}
