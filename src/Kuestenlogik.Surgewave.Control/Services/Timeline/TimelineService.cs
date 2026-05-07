using Kuestenlogik.Surgewave.Control.Models.Timeline;

namespace Kuestenlogik.Surgewave.Control.Services.Timeline;

/// <summary>
/// Implements timeline-based message flow visualization by aggregating messages
/// from multiple topics and correlating them via headers.
/// </summary>
public sealed class TimelineService : ITimelineService
{
    private const string CorrelationIdHeader = "surgewave-correlation-id";
    private const string TraceIdHeader = "surgewave-trace-id";
    private const int MaxValuePreviewLength = 200;

    private readonly ISurgewaveApiClient _apiClient;

    public TimelineService(ISurgewaveApiClient apiClient)
    {
        _apiClient = apiClient;
    }

    public async Task<TimelineSnapshot> GetSnapshotAsync(
        IReadOnlyList<string> topics,
        DateTimeOffset from,
        DateTimeOffset to,
        int maxMessagesPerTopic = 100)
    {
        var allEvents = new List<TimelineEvent>();

        // Fetch messages from each topic in parallel
        var tasks = topics.Select(topic => FetchTopicEventsAsync(topic, from, to, maxMessagesPerTopic));
        var results = await Task.WhenAll(tasks);

        foreach (var events in results)
        {
            allEvents.AddRange(events);
        }

        // Sort all events chronologically
        allEvents.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));

        return new TimelineSnapshot
        {
            From = from,
            To = to,
            Events = allEvents,
            Topics = [.. topics]
        };
    }

    public async Task<MessageTrace> TraceMessageAsync(string topic, int partition, long offset)
    {
        // Fetch the origin message
        var detail = await _apiClient.GetMessageAsync(topic, partition, offset);
        if (detail is null)
        {
            return new MessageTrace
            {
                Origin = new TimelineEvent
                {
                    Topic = topic,
                    Partition = partition,
                    Offset = offset,
                    Timestamp = DateTimeOffset.UtcNow
                }
            };
        }

        var origin = ToTimelineEvent(topic, partition, detail);
        var correlationId = origin.CorrelationId;

        var trace = new MessageTrace { Origin = origin };

        if (string.IsNullOrEmpty(correlationId))
        {
            return trace;
        }

        // Search all available topics for messages with the same correlation ID
        var allTopics = await _apiClient.ListTopicsAsync(includeInternal: false);
        var searchTasks = allTopics
            .Where(t => t.Name != topic) // Skip origin topic
            .Select(t => FindCorrelatedMessagesAsync(t.Name, correlationId, origin.Timestamp));

        var correlated = await Task.WhenAll(searchTasks);

        foreach (var events in correlated)
        {
            foreach (var evt in events)
            {
                var latency = evt.Timestamp - origin.Timestamp;
                trace.Hops.Add(new TraceHop
                {
                    Event = evt,
                    TransformationType = DetermineTransformationType(evt.Headers),
                    Latency = latency
                });
            }
        }

        // Sort hops by timestamp
        trace.Hops.Sort((a, b) => a.Event.Timestamp.CompareTo(b.Event.Timestamp));

        return trace;
    }

    public async Task<TimelineReplaySession> StartReplayAsync(
        IReadOnlyList<string> topics,
        DateTimeOffset from,
        DateTimeOffset to,
        double speed = 1.0)
    {
        var snapshot = await GetSnapshotAsync(topics, from, to);

        return new TimelineReplaySession
        {
            Speed = speed,
            IsPaused = false,
            CurrentPosition = from,
            Events = snapshot.Events
        };
    }

    private async Task<List<TimelineEvent>> FetchTopicEventsAsync(
        string topic, DateTimeOffset from, DateTimeOffset to, int maxMessages)
    {
        var events = new List<TimelineEvent>();

        try
        {
            // Get topic description to know partition count
            var desc = await _apiClient.DescribeTopicAsync(topic);
            if (desc is null) return events;

            var perPartitionLimit = Math.Max(1, maxMessages / Math.Max(1, desc.PartitionCount));

            // Fetch from each partition in parallel
            var partitionTasks = desc.Partitions.Select(async p =>
            {
                var partEvents = new List<TimelineEvent>();

                // Find the starting offset for the 'from' timestamp
                var startOffset = await _apiClient.GetOffsetForTimestampAsync(topic, p.PartitionId, from);
                if (startOffset is null) return partEvents;

                // Fetch messages from that offset
                var result = await _apiClient.GetMessagesAsync(topic, p.PartitionId, startOffset.Value, perPartitionLimit);
                if (result is null) return partEvents;

                foreach (var msg in result.Messages)
                {
                    // Filter to time range
                    if (msg.Timestamp < from || msg.Timestamp > to)
                        continue;

                    partEvents.Add(ToTimelineEvent(topic, p.PartitionId, msg));
                }

                return partEvents;
            });

            var partitionResults = await Task.WhenAll(partitionTasks);
            foreach (var partEvents in partitionResults)
            {
                events.AddRange(partEvents);
            }
        }
        catch
        {
            // Topic may be unavailable; return empty
        }

        return events;
    }

    private async Task<List<TimelineEvent>> FindCorrelatedMessagesAsync(
        string topic, string correlationId, DateTimeOffset originTimestamp)
    {
        var events = new List<TimelineEvent>();

        try
        {
            var desc = await _apiClient.DescribeTopicAsync(topic);
            if (desc is null) return events;

            // Search a window around the origin timestamp (up to 5 minutes after)
            var searchFrom = originTimestamp;
            var searchTo = originTimestamp.AddMinutes(5);

            foreach (var p in desc.Partitions)
            {
                var startOffset = await _apiClient.GetOffsetForTimestampAsync(topic, p.PartitionId, searchFrom);
                if (startOffset is null) continue;

                // Fetch a batch of messages to search through
                var result = await _apiClient.GetMessagesAsync(topic, p.PartitionId, startOffset.Value, 50);
                if (result is null) continue;

                foreach (var msg in result.Messages)
                {
                    if (msg.Timestamp > searchTo) break;

                    var msgCorrelation = ExtractCorrelationId(msg.Headers);
                    if (msgCorrelation == correlationId)
                    {
                        events.Add(ToTimelineEvent(topic, p.PartitionId, msg));
                    }
                }
            }
        }
        catch
        {
            // Skip unavailable topics
        }

        return events;
    }

    private static TimelineEvent ToTimelineEvent(string topic, int partition, Models.MessageDetail detail)
    {
        var headers = detail.Headers is not null
            ? new Dictionary<string, string>(detail.Headers)
            : null;

        return new TimelineEvent
        {
            Topic = topic,
            Partition = partition,
            Offset = detail.Offset,
            Timestamp = detail.Timestamp,
            Key = detail.Key,
            ValuePreview = detail.Value is not null
                ? (detail.Value.Length > MaxValuePreviewLength
                    ? detail.Value[..MaxValuePreviewLength]
                    : detail.Value)
                : null,
            ValueSize = detail.ValueSizeBytes,
            CorrelationId = ExtractCorrelationId(detail.Headers),
            Headers = headers
        };
    }

    private static string? ExtractCorrelationId(IReadOnlyDictionary<string, string>? headers)
    {
        if (headers is null) return null;

        if (headers.TryGetValue(CorrelationIdHeader, out var correlationId))
            return correlationId;

        if (headers.TryGetValue(TraceIdHeader, out var traceId))
            return traceId;

        return null;
    }

    private static string? DetermineTransformationType(Dictionary<string, string>? headers)
    {
        if (headers is null) return null;

        if (headers.ContainsKey("_provenance_path"))
            return "pipeline";

        if (headers.ContainsKey("surgewave-streams-app"))
            return "streams";

        if (headers.ContainsKey("surgewave-connector"))
            return "connector";

        return null;
    }
}
