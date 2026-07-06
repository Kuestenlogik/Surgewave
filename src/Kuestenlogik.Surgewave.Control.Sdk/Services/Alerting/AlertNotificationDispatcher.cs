using System.Net.Http.Json;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services.Alerting;

/// <summary>
/// Delivers alert notifications to the configured channels. Slack, Teams and
/// generic webhooks are plain HTTP POSTs; PagerDuty uses the Events API v2.
/// Email is not supported server-side yet and is logged as a warning.
/// </summary>
public sealed class AlertNotificationDispatcher
{
    public const string HttpClientName = "AlertNotifications";
    private const string PagerDutyEventsUrl = "https://events.pagerduty.com/v2/enqueue";

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<AlertNotificationDispatcher>? _logger;

    public AlertNotificationDispatcher(IHttpClientFactory httpClientFactory, ILogger<AlertNotificationDispatcher>? logger = null)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    /// <summary>
    /// Send an alert to all given channels. Failures are logged per channel and
    /// never propagate — one broken webhook must not block the others.
    /// </summary>
    public async Task DispatchAsync(AlertEvent alert, IReadOnlyList<NotificationChannel> channels, CancellationToken cancellationToken = default)
    {
        var summary = $"[{alert.Severity}] {alert.RuleName}: {alert.Message}";
        foreach (var channel in channels.Where(c => c.Enabled))
        {
            var delivered = await SendAsync(channel, summary, alert, cancellationToken);
            if (!delivered)
            {
                _logger?.LogWarning("Alert notification to channel {Channel} ({Type}) failed",
                    channel.Name, channel.Type);
            }
        }
    }

    /// <summary>Send a test message to a single channel; returns whether delivery succeeded.</summary>
    public Task<bool> SendTestAsync(NotificationChannel channel, CancellationToken cancellationToken = default)
        => SendAsync(channel, $"Surgewave Control test notification for channel '{channel.Name}'", alert: null, cancellationToken);

    private async Task<bool> SendAsync(NotificationChannel channel, string summary, AlertEvent? alert, CancellationToken cancellationToken)
    {
        var url = channel.Config.GetValueOrDefault("url", "");
        if (string.IsNullOrWhiteSpace(url))
        {
            _logger?.LogWarning("Notification channel {Channel} ({Type}) has no URL configured", channel.Name, channel.Type);
            return false;
        }

        try
        {
            var client = _httpClientFactory.CreateClient(HttpClientName);
            using var response = channel.Type switch
            {
                NotificationChannelType.Slack or NotificationChannelType.MicrosoftTeams =>
                    await client.PostAsJsonAsync(url, new { text = summary }, cancellationToken),
                NotificationChannelType.Webhook =>
                    await client.PostAsJsonAsync(url, BuildWebhookPayload(summary, alert), cancellationToken),
                NotificationChannelType.PagerDuty =>
                    await client.PostAsJsonAsync(PagerDutyEventsUrl, BuildPagerDutyPayload(url, summary, alert), cancellationToken),
                _ => null,
            };

            if (response is null)
            {
                _logger?.LogWarning("Notification channel type {Type} is not supported server-side yet", channel.Type);
                return false;
            }

            return response.IsSuccessStatusCode;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or InvalidOperationException or UriFormatException)
        {
            _logger?.LogWarning(ex, "Notification delivery to {Channel} ({Type}) failed", channel.Name, channel.Type);
            return false;
        }
    }

    private static object BuildWebhookPayload(string summary, AlertEvent? alert) => alert is null
        ? new { source = "surgewave-control", summary, test = true }
        : new
        {
            source = "surgewave-control",
            summary,
            alert.Id,
            alert.RuleId,
            alert.RuleName,
            type = alert.Type.ToString(),
            severity = alert.Severity.ToString(),
            alert.Message,
            alert.CurrentValue,
            alert.Threshold,
            alert.FiredAt,
        };

    private static object BuildPagerDutyPayload(string routingKey, string summary, AlertEvent? alert) => new
    {
        routing_key = routingKey,
        event_action = "trigger",
        dedup_key = alert?.Id,
        payload = new
        {
            summary,
            source = "surgewave-control",
            severity = alert?.Severity switch
            {
                AlertSeverity.Critical => "critical",
                AlertSeverity.Warning => "warning",
                _ => "info",
            },
        },
    };
}
