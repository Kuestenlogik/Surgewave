namespace Kuestenlogik.Surgewave.Connect.Nodes.Trigger;

using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Schedule-based trigger that emits events on a cron schedule.
/// </summary>
[ConnectorMetadata(
    Name = "ScheduleTrigger",
    Description = "Cron-based event source",
    Tags = "trigger,schedule,cron,timer")]
public sealed class ScheduleTrigger : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(ScheduleTriggerTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("topic", ConfigType.String, "", Importance.High,
            "Output topic for schedule events")
        .Define("cron", ConfigType.String, "*/5 * * * *", Importance.High,
            "Cron expression")
        .Define("payload", ConfigType.String, "", Importance.Low,
            "Static JSON payload")
        .Define("timezone", ConfigType.String, "UTC", Importance.Low,
            "Timezone for cron");

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}

internal sealed class ScheduleTriggerTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private string _cronExpression = "";
    private string _payload = "";
    private TimeZoneInfo _timezone = TimeZoneInfo.Utc;
    private DateTimeOffset _nextRun = DateTimeOffset.MinValue;
    private CronSchedule? _schedule;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config.TryGetValue("topic", out var t) ? t : "";
        _cronExpression = config.TryGetValue("cron", out var c) ? c : "*/5 * * * *";
        _payload = config.TryGetValue("payload", out var p) ? p : "";

        var tzId = config.TryGetValue("timezone", out var tz) ? tz : "UTC";
        try
        {
            _timezone = TimeZoneInfo.FindSystemTimeZoneById(tzId);
        }
        catch
        {
            _timezone = TimeZoneInfo.Utc;
        }

        _schedule = CronSchedule.Parse(_cronExpression);
        _nextRun = _schedule.GetNextOccurrence(DateTimeOffset.UtcNow, _timezone);
    }

    public override void Stop()
    {
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        if (string.IsNullOrEmpty(_topic) || _schedule is null)
            return Task.FromResult<IReadOnlyList<SourceRecord>>(records);

        var now = DateTimeOffset.UtcNow;

        if (now >= _nextRun)
        {
            var payload = string.IsNullOrEmpty(_payload)
                ? JsonSerializer.Serialize(new
                {
                    trigger = "schedule",
                    cron = _cronExpression,
                    scheduled_time = _nextRun.ToString("O"),
                    actual_time = now.ToString("O")
                })
                : _payload;

            var record = new SourceRecord
            {
                SourcePartition = new Dictionary<string, object> { ["schedule"] = _cronExpression },
                SourceOffset = new Dictionary<string, object> { ["timestamp"] = now.ToUnixTimeMilliseconds() },
                Topic = _topic,
                Key = System.Text.Encoding.UTF8.GetBytes(_cronExpression),
                Value = System.Text.Encoding.UTF8.GetBytes(payload),
                Headers = new Dictionary<string, byte[]>
                {
                    ["_trigger_type"] = System.Text.Encoding.UTF8.GetBytes("schedule"),
                    ["_cron"] = System.Text.Encoding.UTF8.GetBytes(_cronExpression),
                    ["_scheduled_time"] = System.Text.Encoding.UTF8.GetBytes(_nextRun.ToString("O"))
                }
            };

            records.Add(record);
            _nextRun = _schedule.GetNextOccurrence(now, _timezone);
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }
}

/// <summary>
/// Simple cron schedule parser supporting standard 5-field cron expressions.
/// </summary>
public sealed class CronSchedule
{
    private readonly int[] _minutes;
    private readonly int[] _hours;
    private readonly int[] _daysOfMonth;
    private readonly int[] _months;
    private readonly int[] _daysOfWeek;

    private CronSchedule(int[] minutes, int[] hours, int[] daysOfMonth, int[] months, int[] daysOfWeek)
    {
        _minutes = minutes;
        _hours = hours;
        _daysOfMonth = daysOfMonth;
        _months = months;
        _daysOfWeek = daysOfWeek;
    }

    public static CronSchedule Parse(string expression)
    {
        var parts = expression.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 5)
            throw new ArgumentException("Invalid cron expression");

        return new CronSchedule(
            ParseField(parts[0], 0, 59),
            ParseField(parts[1], 0, 23),
            ParseField(parts[2], 1, 31),
            ParseField(parts[3], 1, 12),
            ParseField(parts[4], 0, 6));
    }

    public DateTimeOffset GetNextOccurrence(DateTimeOffset from, TimeZoneInfo timezone)
    {
        var local = TimeZoneInfo.ConvertTime(from, timezone);
        var candidate = new DateTimeOffset(
            local.Year, local.Month, local.Day,
            local.Hour, local.Minute, 0, local.Offset);

        candidate = candidate.AddMinutes(1);

        for (var i = 0; i < 366 * 24 * 60; i++)
        {
            if (Matches(candidate))
            {
                return candidate.ToUniversalTime();
            }
            candidate = candidate.AddMinutes(1);
        }

        return from.AddMinutes(1);
    }

    private bool Matches(DateTimeOffset dt)
    {
        return _minutes.Contains(dt.Minute)
            && _hours.Contains(dt.Hour)
            && _daysOfMonth.Contains(dt.Day)
            && _months.Contains(dt.Month)
            && _daysOfWeek.Contains((int)dt.DayOfWeek);
    }

    private static int[] ParseField(string field, int min, int max)
    {
        var values = new HashSet<int>();

        foreach (var part in field.Split(','))
        {
            if (part == "*")
            {
                for (var i = min; i <= max; i++)
                    values.Add(i);
            }
            else if (part.Contains('/'))
            {
                var stepParts = part.Split('/');
                var range = stepParts[0];
                var step = int.Parse(stepParts[1]);

                int start, end;
                if (range == "*")
                {
                    start = min;
                    end = max;
                }
                else if (range.Contains('-'))
                {
                    var rangeParts = range.Split('-');
                    start = int.Parse(rangeParts[0]);
                    end = int.Parse(rangeParts[1]);
                }
                else
                {
                    start = int.Parse(range);
                    end = max;
                }

                for (var i = start; i <= end; i += step)
                    values.Add(i);
            }
            else if (part.Contains('-'))
            {
                var rangeParts = part.Split('-');
                var start = int.Parse(rangeParts[0]);
                var end = int.Parse(rangeParts[1]);

                for (var i = start; i <= end; i++)
                    values.Add(i);
            }
            else
            {
                values.Add(int.Parse(part));
            }
        }

        return values.Order().ToArray();
    }
}
