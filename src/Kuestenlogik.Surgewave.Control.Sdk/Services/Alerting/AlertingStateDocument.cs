using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services.Alerting;

/// <summary>
/// Persistent state of the server-side alerting engine: rules, notification
/// channels and the alert event history, serialized as one JSON document.
/// </summary>
public sealed class AlertingStateDocument
{
    public List<AlertRule> Rules { get; set; } = [];
    public List<NotificationChannel> Channels { get; set; } = [];
    public List<AlertEvent> Events { get; set; } = [];
}
