using System.Text.Json;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services.Alerting;

/// <summary>
/// Default <see cref="IAlertingService"/> implementation. Singleton: all Blazor
/// circuits and the background evaluation worker share one state instance,
/// persisted through <see cref="AlertingStore"/> on every mutation.
/// </summary>
public sealed class AlertingService : IAlertingService
{
    private const int MaxHistoryEntries = 500;

    private readonly Lock _gate = new();
    private readonly AlertingStore _store;
    private readonly AlertNotificationDispatcher _dispatcher;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<AlertingService>? _logger;
    private readonly AlertingStateDocument _state;

    // Rate-based rules (throughput, error rate) diff successive snapshots. Only
    // the single worker thread calls EvaluateAsync, so this needs no lock.
    private MetricsSnapshot? _previousMetrics;

    public AlertingService(
        AlertingStore store,
        AlertNotificationDispatcher dispatcher,
        TimeProvider? timeProvider = null,
        ILogger<AlertingService>? logger = null)
    {
        _store = store;
        _dispatcher = dispatcher;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger;
        _state = store.Load();
    }

    public event Action? Changed;

    public DateTime? LastEvaluatedAt { get; private set; }

    public bool HasRules
    {
        get
        {
            lock (_gate) return _state.Rules.Count > 0;
        }
    }

    public IReadOnlyList<AlertRule> GetRules()
    {
        lock (_gate) return DeepClone(_state.Rules);
    }

    public IReadOnlyList<NotificationChannel> GetChannels()
    {
        lock (_gate) return DeepClone(_state.Channels);
    }

    public IReadOnlyList<AlertEvent> GetEvents()
    {
        lock (_gate) return DeepClone(_state.Events);
    }

    public void SaveRules(IEnumerable<AlertRule> rules)
    {
        lock (_gate)
        {
            _state.Rules = DeepClone(rules.ToList());
            _store.Save(_state);
        }
        Changed?.Invoke();
    }

    public void SaveChannels(IEnumerable<NotificationChannel> channels)
    {
        lock (_gate)
        {
            _state.Channels = DeepClone(channels.ToList());
            _store.Save(_state);
        }
        Changed?.Invoke();
    }

    public bool Acknowledge(string eventId, string acknowledgedBy)
    {
        lock (_gate)
        {
            var alert = _state.Events.FirstOrDefault(e => e.Id == eventId);
            if (alert is null || alert.AcknowledgedAt is not null)
                return false;

            alert.AcknowledgedAt = _timeProvider.GetUtcNow().UtcDateTime;
            alert.AcknowledgedBy = acknowledgedBy;
            _store.Save(_state);
        }
        Changed?.Invoke();
        return true;
    }

    public int AcknowledgeAll(string acknowledgedBy)
    {
        int acknowledged;
        lock (_gate)
        {
            var pending = _state.Events.Where(e => !e.IsResolved && e.AcknowledgedAt is null).ToList();
            foreach (var alert in pending)
            {
                alert.AcknowledgedAt = _timeProvider.GetUtcNow().UtcDateTime;
                alert.AcknowledgedBy = acknowledgedBy;
            }
            acknowledged = pending.Count;
            if (acknowledged > 0)
                _store.Save(_state);
        }
        if (acknowledged > 0)
            Changed?.Invoke();
        return acknowledged;
    }

    public bool Resolve(string eventId)
    {
        lock (_gate)
        {
            var alert = _state.Events.FirstOrDefault(e => e.Id == eventId);
            if (alert is null || alert.IsResolved)
                return false;

            alert.IsResolved = true;
            alert.ResolvedAt = _timeProvider.GetUtcNow().UtcDateTime;
            _store.Save(_state);
        }
        Changed?.Invoke();
        return true;
    }

    public int ClearResolvedHistory()
    {
        int removed;
        lock (_gate)
        {
            removed = _state.Events.RemoveAll(e => e.IsResolved);
            if (removed > 0)
                _store.Save(_state);
        }
        if (removed > 0)
            Changed?.Invoke();
        return removed;
    }

    public async Task EvaluateAsync(
        MetricsSnapshot metrics,
        IReadOnlyList<ConsumerGroupLag> lags,
        bool brokerReachable,
        CancellationToken cancellationToken = default)
    {
        var rates = MetricsRates.Between(_previousMetrics, metrics, brokerReachable);
        if (brokerReachable)
            _previousMetrics = metrics;

        var now = _timeProvider.GetUtcNow().UtcDateTime;
        List<(AlertEvent Alert, List<string> ChannelIds)> fired = [];
        List<NotificationChannel> channels;

        lock (_gate)
        {
            LastEvaluatedAt = now;
            channels = DeepClone(_state.Channels);

            foreach (var rule in _state.Rules.Where(r => r.Enabled))
            {
                var result = AlertRuleEvaluator.Evaluate(rule, metrics, rates, lags, brokerReachable);
                if (!result.Triggered)
                    continue;

                var lastFired = _state.Events
                    .Where(e => e.RuleId == rule.Id)
                    .OrderByDescending(e => e.FiredAt)
                    .FirstOrDefault();
                if (lastFired is not null && (now - lastFired.FiredAt).TotalMinutes < rule.CooldownMinutes)
                    continue;

                var alert = new AlertEvent
                {
                    RuleId = rule.Id,
                    RuleName = rule.Name,
                    Type = rule.Type,
                    Severity = rule.Severity,
                    Message = result.Message,
                    CurrentValue = result.CurrentValue,
                    Threshold = rule.Threshold,
                    FiredAt = now,
                };
                _state.Events.Add(alert);
                fired.Add((alert, rule.NotificationChannels));
            }

            if (fired.Count > 0)
            {
                TrimHistory();
                _store.Save(_state);
            }
        }

        foreach (var (alert, channelIds) in fired)
        {
            _logger?.LogWarning("Alert fired: [{Severity}] {Rule} — {Message}",
                alert.Severity, alert.RuleName, alert.Message);

            // Rules without explicit channel selection notify all enabled channels.
            var targets = channelIds.Count == 0
                ? channels
                : channels.Where(c => channelIds.Contains(c.Id)).ToList();
            await _dispatcher.DispatchAsync(alert, targets, cancellationToken);
        }

        // Raised every cycle (not only when an alert fires) so subscribers can
        // reflect the fresh LastEvaluatedAt and any newly persisted events.
        Changed?.Invoke();
    }

    public Task<bool> SendTestAsync(NotificationChannel channel, CancellationToken cancellationToken = default)
        => _dispatcher.SendTestAsync(channel, cancellationToken);

    private void TrimHistory()
    {
        var excess = _state.Events.Count - MaxHistoryEntries;
        if (excess <= 0)
            return;

        foreach (var oldest in _state.Events.OrderBy(e => e.FiredAt).Take(excess).ToList())
            _state.Events.Remove(oldest);
    }

    private static T DeepClone<T>(T value)
        => JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(value))!;
}
