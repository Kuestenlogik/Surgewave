using Kuestenlogik.Surgewave.Connect.Nodes.Trigger;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Background service that checks pipeline schedules and starts/stops pipelines accordingly.
/// </summary>
public sealed class PipelineScheduler : BackgroundService
{
    private readonly PipelineOrchestrator _orchestrator;
    private readonly PipelineStore _store;
    private readonly ILogger<PipelineScheduler> _logger;
    private static readonly TimeSpan CheckInterval = TimeSpan.FromSeconds(15);

    public PipelineScheduler(
        PipelineOrchestrator orchestrator,
        PipelineStore store,
        ILogger<PipelineScheduler> logger)
    {
        _orchestrator = orchestrator;
        _store = store;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Pipeline scheduler started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CheckSchedulesAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error checking pipeline schedules");
            }

            await Task.Delay(CheckInterval, stoppingToken);
        }

        _logger.LogInformation("Pipeline scheduler stopped");
    }

    private async Task CheckSchedulesAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;

        foreach (var pipeline in _orchestrator.GetAll())
        {
            if (pipeline.Schedule is not { Enabled: true } schedule)
                continue;

            if (string.IsNullOrEmpty(schedule.CronExpression))
                continue;

            try
            {
                // Check if pipeline should be started
                if (pipeline.Status != PipelineStatus.Running &&
                    schedule.NextRunAt.HasValue &&
                    now >= schedule.NextRunAt.Value)
                {
                    _logger.LogInformation("Schedule triggered: starting pipeline {Id} ({Name})",
                        pipeline.Id, pipeline.Name);

                    await _orchestrator.StartAsync(pipeline.Id, cancellationToken: cancellationToken);

                    // Compute next run and update schedule
                    var cron = CronSchedule.Parse(schedule.CronExpression);
                    var tz = GetTimezone(schedule.Timezone);
                    var nextRun = cron.GetNextOccurrence(now, tz);

                    await _store.UpdateScheduleAsync(pipeline.Id, schedule with
                    {
                        LastRunAt = now,
                        NextRunAt = nextRun
                    }, cancellationToken);
                }

                // Check if running pipeline has exceeded max run duration
                if (pipeline.Status == PipelineStatus.Running &&
                    schedule.MaxRunDurationMinutes.HasValue &&
                    schedule.LastRunAt.HasValue)
                {
                    var elapsed = now - schedule.LastRunAt.Value;
                    if (elapsed.TotalMinutes > schedule.MaxRunDurationMinutes.Value)
                    {
                        _logger.LogInformation(
                            "Max run duration exceeded: stopping pipeline {Id} after {Minutes:F0}min",
                            pipeline.Id, elapsed.TotalMinutes);

                        await _orchestrator.StopAsync(pipeline.Id, cancellationToken);

                        await _store.UpdateScheduleAsync(pipeline.Id, schedule with
                        {
                            LastCompletedAt = now
                        }, cancellationToken);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing schedule for pipeline {Id}", pipeline.Id);
            }
        }
    }

    private static TimeZoneInfo GetTimezone(string timezone)
    {
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(timezone);
        }
        catch
        {
            return TimeZoneInfo.Utc;
        }
    }
}
