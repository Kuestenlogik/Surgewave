using Kuestenlogik.Surgewave.Control.Models.Timeline;

namespace Kuestenlogik.Surgewave.Control.Services.Timeline;

/// <summary>
/// Provides timeline-based message flow visualization across topics.
/// </summary>
public interface ITimelineService
{
    /// <summary>
    /// Fetches a unified timeline snapshot of messages across multiple topics within a time range.
    /// </summary>
    Task<TimelineSnapshot> GetSnapshotAsync(
        IReadOnlyList<string> topics,
        DateTimeOffset from,
        DateTimeOffset to,
        int maxMessagesPerTopic = 100);

    /// <summary>
    /// Traces a message through downstream topics using correlation headers.
    /// </summary>
    Task<MessageTrace> TraceMessageAsync(
        string topic, int partition, long offset);

    /// <summary>
    /// Creates a replay session with pre-loaded events for client-side playback.
    /// </summary>
    Task<TimelineReplaySession> StartReplayAsync(
        IReadOnlyList<string> topics,
        DateTimeOffset from,
        DateTimeOffset to,
        double speed = 1.0);
}
