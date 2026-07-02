using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Monitoring;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// REST API for consumer-group lag inspection and offset resets.
/// Consumed by the Control UI (ConsumerLagDashboard, Lag-Alerts, Offset-Reset)
/// and available to any HTTP tooling. Lag data covers Kafka-protocol and
/// native-protocol consumers alike — both commit into the shared OffsetStore.
/// </summary>
public static class ConsumerLagRestApi
{
    public static IEndpointRouteBuilder MapConsumerLag(
        this IEndpointRouteBuilder app,
        ILagCalculator lagCalculator,
        OffsetStore offsetStore,
        LogManager logManager,
        Func<string, int> getActiveMemberCount)
    {
        var group = app.MapGroup("/api/consumer-groups")
            .WithTags("Consumer Lag");

        group.MapGet("/lag", () => GetAllLags(lagCalculator))
            .WithName("GetAllConsumerLags")
            .WithSummary("Get lag for all consumer groups")
            .Produces<AllLagsResponse>();

        group.MapGet("/{groupId}/lag", (string groupId) => GetGroupLag(lagCalculator, groupId))
            .WithName("GetConsumerGroupLag")
            .WithSummary("Get lag for a single consumer group")
            .Produces<GroupLagResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/{groupId}/offsets", (string groupId, ResetOffsetsRequest request) =>
                ResetOffsets(offsetStore, logManager, getActiveMemberCount, groupId, request))
            .WithName("ResetConsumerGroupOffsets")
            .WithSummary("Reset committed offsets of an (empty) consumer group for one topic")
            .Produces<ResetOffsetsResponse>()
            .ProducesProblem(StatusCodes.Status400BadRequest)
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status409Conflict);

        return app;
    }

    private static IResult GetAllLags(ILagCalculator lagCalculator)
    {
        var summary = lagCalculator.GetLagSummary();
        return Results.Ok(new AllLagsResponse(
            [.. summary.Groups.Select(ToResponse)]));
    }

    private static IResult GetGroupLag(ILagCalculator lagCalculator, string groupId)
    {
        var info = lagCalculator.GetGroupLag(groupId);
        return info is null
            ? Results.NotFound(new { message = $"Consumer group '{groupId}' has no committed offsets" })
            : Results.Ok(ToResponse(info));
    }

    private static IResult ResetOffsets(
        OffsetStore offsetStore,
        LogManager logManager,
        Func<string, int> getActiveMemberCount,
        string groupId,
        ResetOffsetsRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Topic))
        {
            return Results.BadRequest(new { message = "topic is required" });
        }

        // Wie bei Kafka: Offsets nur fuer Gruppen ohne aktive Member zuruecksetzen,
        // sonst ueberschreibt der naechste Commit des laufenden Consumers den Reset.
        if (getActiveMemberCount(groupId) > 0)
        {
            return Results.Conflict(new { message = $"Consumer group '{groupId}' has active members — stop consumers before resetting offsets" });
        }

        // Zielpartitionen: die Partitionen mit committeten Offsets fuer das Topic;
        // ohne Commits alle Partitionen laut Topic-Metadaten.
        var committed = offsetStore.GetAllOffsets(groupId);
        var partitions = committed.Keys
            .Select(key => key.Split(':'))
            .Where(parts => parts.Length == 2 && parts[0] == request.Topic && int.TryParse(parts[1], out _))
            .Select(parts => int.Parse(parts[1]))
            .ToList();

        if (partitions.Count == 0)
        {
            partitions = [.. logManager.GetTopicPartitions(request.Topic)];
            if (partitions.Count == 0)
            {
                return Results.NotFound(new { message = $"Topic '{request.Topic}' not found and group '{groupId}' has no committed offsets for it" });
            }
        }

        var strategy = request.Strategy?.Trim().ToLowerInvariant();
        var resetPartitions = new List<ResetPartitionResult>(partitions.Count);

        foreach (var partition in partitions)
        {
            var log = logManager.GetLog(new TopicPartition { Topic = request.Topic, Partition = partition });

            long newOffset;
            switch (strategy)
            {
                case "earliest" or "beginning":
                    newOffset = log?.LogStartOffset ?? 0;
                    break;
                case "latest" or "end":
                    newOffset = log?.NextOffset ?? 0;
                    break;
                case "timestamp":
                    if (request.Timestamp is not long ts)
                    {
                        return Results.BadRequest(new { message = "timestamp (unix ms) is required for strategy 'timestamp'" });
                    }
                    newOffset = log?.FindOffsetByTimestamp(ts) ?? log?.NextOffset ?? 0;
                    break;
                case "offset" or "specific":
                    if (request.Offset is not long target || target < 0)
                    {
                        return Results.BadRequest(new { message = "offset (>= 0) is required for strategy 'offset'" });
                    }
                    newOffset = target;
                    break;
                default:
                    return Results.BadRequest(new { message = $"Unknown reset strategy '{request.Strategy}' — expected earliest|latest|timestamp|offset" });
            }

            offsetStore.CommitOffset(groupId, request.Topic, partition, newOffset);
            resetPartitions.Add(new ResetPartitionResult(partition, newOffset));
        }

        return Results.Ok(new ResetOffsetsResponse(groupId, request.Topic, resetPartitions));
    }

    private static GroupLagResponse ToResponse(ConsumerGroupLagInfo info) => new(
        info.GroupId,
        info.State,
        info.TotalLag,
        [.. info.Topics.SelectMany(t => t.Partitions.Select(p => new PartitionLagResponse(
            t.Topic, p.Partition, p.CommittedOffset, p.HighWatermark, p.Lag, p.AssignedConsumer)))]);
}

/// <summary>Response listing lag for all consumer groups.</summary>
public sealed record AllLagsResponse(IReadOnlyList<GroupLagResponse> Groups);

/// <summary>Lag of one consumer group, partitions flattened across topics.</summary>
public sealed record GroupLagResponse(
    string GroupId,
    string State,
    long TotalLag,
    IReadOnlyList<PartitionLagResponse> Partitions);

/// <summary>Lag of one topic partition within a consumer group.</summary>
public sealed record PartitionLagResponse(
    string Topic,
    int Partition,
    long CurrentOffset,
    long EndOffset,
    long Lag,
    string? ConsumerId);

/// <summary>Request to reset committed offsets of a consumer group for one topic.</summary>
public sealed record ResetOffsetsRequest(string Topic, string Strategy, long? Timestamp, long? Offset);

/// <summary>Result of an offset reset.</summary>
public sealed record ResetOffsetsResponse(string GroupId, string Topic, IReadOnlyList<ResetPartitionResult> Partitions);

/// <summary>New committed offset of one partition after a reset.</summary>
public sealed record ResetPartitionResult(int Partition, long Offset);
