namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Configuration for automatic pipeline scheduling via CRON expressions.
/// </summary>
public sealed record ScheduleConfig
{
    public string? CronExpression { get; init; }
    public string Timezone { get; init; } = "UTC";
    public bool Enabled { get; init; }
    public int? MaxRunDurationMinutes { get; init; }
    public DateTimeOffset? LastRunAt { get; init; }
    public DateTimeOffset? NextRunAt { get; init; }
    public DateTimeOffset? LastCompletedAt { get; init; }
}
