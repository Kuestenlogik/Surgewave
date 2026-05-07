namespace Kuestenlogik.Surgewave.Control.Models.Timeline;

/// <summary>
/// A snapshot of timeline events across multiple topics within a time range.
/// </summary>
public sealed class TimelineSnapshot
{
    /// <summary>Start of the time range.</summary>
    public DateTimeOffset From { get; init; }

    /// <summary>End of the time range.</summary>
    public DateTimeOffset To { get; init; }

    /// <summary>All events in chronological order.</summary>
    public List<TimelineEvent> Events { get; init; } = [];

    /// <summary>Topics included in this snapshot.</summary>
    public List<string> Topics { get; init; } = [];
}

/// <summary>
/// A single message event on the timeline.
/// </summary>
public sealed class TimelineEvent
{
    /// <summary>Topic the message belongs to.</summary>
    public required string Topic { get; init; }

    /// <summary>Partition within the topic.</summary>
    public int Partition { get; init; }

    /// <summary>Offset within the partition.</summary>
    public long Offset { get; init; }

    /// <summary>Timestamp of the message.</summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>Message key, if present.</summary>
    public string? Key { get; init; }

    /// <summary>First 200 characters of the message value.</summary>
    public string? ValuePreview { get; init; }

    /// <summary>Total size of the message value in bytes.</summary>
    public int ValueSize { get; init; }

    /// <summary>Correlation ID extracted from headers, if present.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>All message headers.</summary>
    public Dictionary<string, string>? Headers { get; init; }
}

/// <summary>
/// Trace of a message through multiple topics via correlation.
/// </summary>
public sealed class MessageTrace
{
    /// <summary>The original message that was traced.</summary>
    public required TimelineEvent Origin { get; init; }

    /// <summary>Subsequent hops the message took through topics.</summary>
    public List<TraceHop> Hops { get; init; } = [];
}

/// <summary>
/// A single hop in a message trace.
/// </summary>
public sealed class TraceHop
{
    /// <summary>The message event at this hop.</summary>
    public required TimelineEvent Event { get; init; }

    /// <summary>Type of transformation (pipeline, streams, connector).</summary>
    public string? TransformationType { get; init; }

    /// <summary>Latency between this hop and the origin.</summary>
    public TimeSpan Latency { get; init; }
}

/// <summary>
/// Client-side replay session state.
/// </summary>
public sealed class TimelineReplaySession
{
    /// <summary>Unique session identifier.</summary>
    public string Id { get; init; } = Guid.NewGuid().ToString("N");

    /// <summary>Playback speed multiplier.</summary>
    public double Speed { get; set; } = 1.0;

    /// <summary>Whether playback is currently paused.</summary>
    public bool IsPaused { get; set; }

    /// <summary>Current playhead position in time.</summary>
    public DateTimeOffset CurrentPosition { get; set; }

    /// <summary>All events for this replay, sorted chronologically.</summary>
    public List<TimelineEvent> Events { get; init; } = [];
}
