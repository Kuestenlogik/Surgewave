using Kuestenlogik.Surgewave.Control.Models.Timeline;
using Kuestenlogik.Surgewave.Control.Services;
using Kuestenlogik.Surgewave.Control.Services.Timeline;
using Kuestenlogik.Surgewave.Control.State;
using Microsoft.AspNetCore.Components;
using MudBlazor;

namespace Kuestenlogik.Surgewave.Control.Components.Pages.Debug;

public sealed partial class TimelineDebugger : IDisposable
{
    private static readonly System.Text.Json.JsonSerializerOptions IndentedJsonOptions = new()
    {
        WriteIndented = true
    };

    private static readonly string[] TopicColors =
    [
        "#1976D2", "#388E3C", "#F57C00", "#7B1FA2",
        "#C62828", "#00838F", "#4E342E", "#283593",
        "#AD1457", "#558B2F", "#EF6C00", "#6A1B9A"
    ];

    // State
    private bool _loadingTopics = true;
    private bool _loading;
    private bool _tracing;
    private bool _isPlaying;
    private double _replaySpeed = 1.0;

    // Topic selection
    private List<string> _availableTopics = [];
    private IEnumerable<string> _selectedTopics = [];

    // Time range
    private DateTime? _fromDate = DateTime.Today;
    private TimeSpan? _fromTime = DateTime.Now.TimeOfDay.Subtract(TimeSpan.FromMinutes(5));
    private DateTime? _toDate = DateTime.Today;
    private TimeSpan? _toTime = DateTime.Now.TimeOfDay;

    // Timeline data
    private TimelineSnapshot? _snapshot;
    private TimelineEvent? _selectedEvent;
    private MessageTrace? _traceResult;
    private DateTimeOffset _playheadPosition;
    private List<TimeAxisTick> _timeAxisTicks = [];

    // Replay timer
    private System.Threading.Timer? _replayTimer;
    private DateTime _lastTickTime;

    // Element reference
    private ElementReference _timelineContainer;

    private readonly List<BreadcrumbItem> _breadcrumbs =
    [
        new("Dashboard", "/"),
        new("Timeline Debugger", null, true)
    ];

    protected override async Task OnInitializedAsync()
    {
        await LoadTopicsAsync();
    }

    private async Task LoadTopicsAsync()
    {
        _loadingTopics = true;
        try
        {
            var topics = await ApiClient.ListTopicsAsync(ClusterState.SelectedClusterId, includeInternal: false);
            _availableTopics = topics.Select(t => t.Name).OrderBy(n => n).ToList();
        }
        catch (Exception ex)
        {
            Notification.ShowError($"Failed to load topics: {ex.Message}");
        }
        finally
        {
            _loadingTopics = false;
        }
    }

    private void ApplyPreset(TimeSpan lookback)
    {
        var now = DateTime.Now;
        var from = now - lookback;
        _fromDate = from.Date;
        _fromTime = from.TimeOfDay;
        _toDate = now.Date;
        _toTime = now.TimeOfDay;
    }

    private DateTimeOffset BuildDateTimeOffset(DateTime? date, TimeSpan? time)
    {
        var d = date ?? DateTime.Today;
        var t = time ?? TimeSpan.Zero;
        return new DateTimeOffset(d.Add(t), DateTimeOffset.Now.Offset);
    }

    private async Task LoadTimelineAsync()
    {
        if (!_selectedTopics.Any()) return;

        _loading = true;
        _selectedEvent = null;
        _traceResult = null;
        StopReplay();
        StateHasChanged();

        try
        {
            var from = BuildDateTimeOffset(_fromDate, _fromTime);
            var to = BuildDateTimeOffset(_toDate, _toTime);

            if (to <= from)
            {
                Notification.ShowWarning("'To' must be after 'From'");
                return;
            }

            _snapshot = await TimelineService.GetSnapshotAsync(_selectedTopics.ToList(), from, to);
            _playheadPosition = from;
            GenerateTimeAxisTicks(from, to);

            if (_snapshot.Events.Count == 0)
            {
                Notification.ShowInfo("No messages found in the selected time range.");
            }
            else
            {
                Notification.ShowSuccess($"Loaded {_snapshot.Events.Count} events across {_snapshot.Topics.Count} topics");
            }
        }
        catch (Exception ex)
        {
            Notification.ShowError($"Failed to load timeline: {ex.Message}");
        }
        finally
        {
            _loading = false;
            StateHasChanged();
        }
    }

    private void GenerateTimeAxisTicks(DateTimeOffset from, DateTimeOffset to)
    {
        _timeAxisTicks = [];
        var totalMs = (to - from).TotalMilliseconds;
        if (totalMs <= 0) return;

        // Target approximately 8-12 ticks
        var tickCount = 10;
        var intervalMs = totalMs / tickCount;

        // Round to a nice interval
        intervalMs = RoundToNiceInterval(intervalMs);

        var current = from;
        while (current <= to)
        {
            var pct = ((current - from).TotalMilliseconds / totalMs) * 100.0;
            _timeAxisTicks.Add(new TimeAxisTick(pct, FormatTickLabel(current, totalMs)));
            current = current.AddMilliseconds(intervalMs);
        }
    }

    private static double RoundToNiceInterval(double ms)
    {
        double[] niceIntervals = [1000, 2000, 5000, 10000, 15000, 30000, 60000, 120000, 300000, 600000, 900000, 1800000, 3600000];
        foreach (var interval in niceIntervals)
        {
            if (ms <= interval) return interval;
        }
        return 3600000; // 1 hour
    }

    private static string FormatTickLabel(DateTimeOffset dt, double totalRangeMs)
    {
        if (totalRangeMs <= 60000) // Under 1 minute: show seconds.ms
            return dt.LocalDateTime.ToString("HH:mm:ss.f");
        if (totalRangeMs <= 3600000) // Under 1 hour: show mm:ss
            return dt.LocalDateTime.ToString("HH:mm:ss");
        // Over 1 hour: show HH:mm
        return dt.LocalDateTime.ToString("HH:mm");
    }

    private double GetEventPercent(TimelineEvent evt)
    {
        if (_snapshot is null) return 0;
        var totalMs = (_snapshot.To - _snapshot.From).TotalMilliseconds;
        if (totalMs <= 0) return 0;
        return ((evt.Timestamp - _snapshot.From).TotalMilliseconds / totalMs) * 100.0;
    }

    private double GetPlayheadPercent()
    {
        if (_snapshot is null) return 0;
        var totalMs = (_snapshot.To - _snapshot.From).TotalMilliseconds;
        if (totalMs <= 0) return 0;
        return ((_playheadPosition - _snapshot.From).TotalMilliseconds / totalMs) * 100.0;
    }

    private void SelectEvent(TimelineEvent evt)
    {
        _selectedEvent = evt;
        _traceResult = null;
    }

    private void ClearSelection()
    {
        _selectedEvent = null;
        _traceResult = null;
    }

    private bool IsHighlighted(TimelineEvent evt)
    {
        if (_traceResult is null) return false;

        if (evt == _traceResult.Origin) return true;

        return _traceResult.Hops.Any(h => h.Event.Topic == evt.Topic
            && h.Event.Partition == evt.Partition
            && h.Event.Offset == evt.Offset);
    }

    private async Task TraceSelectedMessageAsync()
    {
        if (_selectedEvent is null) return;

        _tracing = true;
        StateHasChanged();

        try
        {
            _traceResult = await TimelineService.TraceMessageAsync(
                _selectedEvent.Topic, _selectedEvent.Partition, _selectedEvent.Offset);

            if (_traceResult.Hops.Count == 0)
            {
                Notification.ShowInfo("No correlated messages found in other topics.");
            }
            else
            {
                Notification.ShowSuccess($"Found {_traceResult.Hops.Count} correlated message(s)");
            }
        }
        catch (Exception ex)
        {
            Notification.ShowError($"Trace failed: {ex.Message}");
        }
        finally
        {
            _tracing = false;
            StateHasChanged();
        }
    }

    // Replay controls

    private void SetSpeed(double speed)
    {
        _replaySpeed = speed;
    }

    private void TogglePlayPause()
    {
        if (_isPlaying)
        {
            PauseReplay();
        }
        else
        {
            StartReplay();
        }
    }

    private void StartReplay()
    {
        if (_snapshot is null) return;

        _isPlaying = true;
        _playheadPosition = _snapshot.From;
        _lastTickTime = DateTime.UtcNow;

        _replayTimer?.Dispose();
        _replayTimer = new System.Threading.Timer(ReplayTick, null, 0, 50); // 20 fps
    }

    private void PauseReplay()
    {
        _isPlaying = false;
        _replayTimer?.Dispose();
        _replayTimer = null;
    }

    private void StopReplay()
    {
        _isPlaying = false;
        _replayTimer?.Dispose();
        _replayTimer = null;

        if (_snapshot is not null)
        {
            _playheadPosition = _snapshot.From;
        }
    }

    private void ReplayTick(object? state)
    {
        if (_snapshot is null || !_isPlaying) return;

        var now = DateTime.UtcNow;
        var elapsed = now - _lastTickTime;
        _lastTickTime = now;

        // Advance playhead by elapsed * speed
        var advance = TimeSpan.FromMilliseconds(elapsed.TotalMilliseconds * _replaySpeed);
        _playheadPosition = _playheadPosition.Add(advance);

        // Check if we've reached the end
        if (_playheadPosition >= _snapshot.To)
        {
            _playheadPosition = _snapshot.To;
            _isPlaying = false;
            _replayTimer?.Dispose();
            _replayTimer = null;
        }

        InvokeAsync(StateHasChanged);
    }

    // Formatting helpers

    private static string FormatBytes(int bytes)
    {
        return bytes switch
        {
            < 1024 => $"{bytes} B",
            < 1048576 => $"{bytes / 1024.0:F1} KB",
            _ => $"{bytes / 1048576.0:F1} MB"
        };
    }

    private static string FormatLatency(TimeSpan latency)
    {
        if (latency.TotalMilliseconds < 1)
            return $"{latency.TotalMicroseconds:F0} us";
        if (latency.TotalMilliseconds < 1000)
            return $"{latency.TotalMilliseconds:F1} ms";
        return $"{latency.TotalSeconds:F2} s";
    }

    private static string FormatJsonPreview(string value)
    {
        try
        {
            var doc = System.Text.Json.JsonDocument.Parse(value);
            return System.Text.Json.JsonSerializer.Serialize(doc, IndentedJsonOptions);
        }
        catch
        {
            return value;
        }
    }

    public void Dispose()
    {
        _replayTimer?.Dispose();
        GC.SuppressFinalize(this);
    }

    // Helper record for time axis ticks
    private sealed record TimeAxisTick(double Position, string Label);
}
