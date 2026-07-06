using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services.Alerting;

/// <summary>
/// Server-side alerting engine: holds rules, channels and the alert history,
/// evaluates rules against broker state and dispatches notifications. State is
/// persisted on the Control host, so alerts fire without an open browser.
/// </summary>
public interface IAlertingService
{
    /// <summary>Raised after any state change (rule/channel edits, fired/acked/resolved alerts).</summary>
    event Action? Changed;

    /// <summary>When the evaluation loop last ran; null before the first run.</summary>
    DateTime? LastEvaluatedAt { get; }

    /// <summary>Whether any rules exist — lets the evaluation worker skip idle cycles.</summary>
    bool HasRules { get; }

    IReadOnlyList<AlertRule> GetRules();
    IReadOnlyList<NotificationChannel> GetChannels();
    IReadOnlyList<AlertEvent> GetEvents();

    void SaveRules(IEnumerable<AlertRule> rules);
    void SaveChannels(IEnumerable<NotificationChannel> channels);

    bool Acknowledge(string eventId, string acknowledgedBy);
    int AcknowledgeAll(string acknowledgedBy);
    bool Resolve(string eventId);

    /// <summary>Remove resolved events from the history; returns how many were removed.</summary>
    int ClearResolvedHistory();

    /// <summary>
    /// Evaluate all enabled rules against the given broker state, persist any
    /// fired alerts and dispatch notifications.
    /// </summary>
    Task EvaluateAsync(
        MetricsSnapshot metrics,
        IReadOnlyList<ConsumerGroupLag> lags,
        bool brokerReachable,
        CancellationToken cancellationToken = default);

    /// <summary>Send a test notification to a channel; returns whether delivery succeeded.</summary>
    Task<bool> SendTestAsync(NotificationChannel channel, CancellationToken cancellationToken = default);
}
