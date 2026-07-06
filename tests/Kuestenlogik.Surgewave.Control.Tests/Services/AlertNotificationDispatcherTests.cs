using System.Net;
using System.Text.Json;
using Kuestenlogik.Surgewave.Control.Models;
using Kuestenlogik.Surgewave.Control.Services.Alerting;

namespace Kuestenlogik.Surgewave.Control.Tests.Services;

/// <summary>
/// Wire-shape tests for alert notification delivery: Slack/Teams get a
/// {"text": ...} payload at the configured webhook, PagerDuty gets an Events
/// API v2 envelope with the integration key, and failures never throw.
/// </summary>
public sealed class AlertNotificationDispatcherTests
{
    private static AlertEvent SampleAlert() => new()
    {
        RuleName = "High Consumer Lag",
        Type = AlertRuleType.ConsumerLag,
        Severity = AlertSeverity.Critical,
        Message = "Max consumer lag 50,000 exceeds threshold 10,000",
        CurrentValue = 50000,
        Threshold = 10000,
    };

    private static NotificationChannel Channel(NotificationChannelType type, string url) => new()
    {
        Name = "test-channel",
        Type = type,
        Enabled = true,
        Config = { ["url"] = url },
    };

    private static (AlertNotificationDispatcher Dispatcher, RecordingHandler Handler) CreateDispatcher(
        HttpStatusCode statusCode = HttpStatusCode.OK)
    {
        var handler = new RecordingHandler(statusCode);
        return (new AlertNotificationDispatcher(new StubHttpClientFactory(handler)), handler);
    }

    [Fact]
    public async Task Slack_PostsTextPayloadToWebhookUrl()
    {
        var (dispatcher, handler) = CreateDispatcher();
        var channel = Channel(NotificationChannelType.Slack, "https://hooks.slack.test/services/abc");

        await dispatcher.DispatchAsync(SampleAlert(), [channel]);

        var request = Assert.Single(handler.Requests);
        Assert.Equal("https://hooks.slack.test/services/abc", request.Url);
        using var payload = JsonDocument.Parse(request.Body);
        Assert.Contains("High Consumer Lag", payload.RootElement.GetProperty("text").GetString());
        Assert.Contains("Critical", payload.RootElement.GetProperty("text").GetString());
    }

    [Fact]
    public async Task PagerDuty_UsesEventsApiWithRoutingKey()
    {
        var (dispatcher, handler) = CreateDispatcher();
        var channel = Channel(NotificationChannelType.PagerDuty, "my-integration-key");

        await dispatcher.DispatchAsync(SampleAlert(), [channel]);

        var request = Assert.Single(handler.Requests);
        Assert.Equal("https://events.pagerduty.com/v2/enqueue", request.Url);
        using var payload = JsonDocument.Parse(request.Body);
        Assert.Equal("my-integration-key", payload.RootElement.GetProperty("routing_key").GetString());
        Assert.Equal("trigger", payload.RootElement.GetProperty("event_action").GetString());
        Assert.Equal("critical", payload.RootElement.GetProperty("payload").GetProperty("severity").GetString());
    }

    [Fact]
    public async Task Webhook_PostsFullAlertPayload()
    {
        var (dispatcher, handler) = CreateDispatcher();
        var channel = Channel(NotificationChannelType.Webhook, "https://alerts.example.test/hook");

        await dispatcher.DispatchAsync(SampleAlert(), [channel]);

        var request = Assert.Single(handler.Requests);
        using var payload = JsonDocument.Parse(request.Body);
        Assert.Equal("surgewave-control", payload.RootElement.GetProperty("source").GetString());
        Assert.Equal("Critical", payload.RootElement.GetProperty("severity").GetString());
        Assert.Equal(50000, payload.RootElement.GetProperty("currentValue").GetDouble());
    }

    [Fact]
    public async Task ChannelWithoutUrl_IsSkippedWithoutThrowing()
    {
        var (dispatcher, handler) = CreateDispatcher();
        var channel = new NotificationChannel { Name = "empty", Type = NotificationChannelType.Slack, Enabled = true };

        await dispatcher.DispatchAsync(SampleAlert(), [channel]);

        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task SendTestAsync_ReportsHttpFailure()
    {
        var (dispatcher, _) = CreateDispatcher(HttpStatusCode.NotFound);
        var channel = Channel(NotificationChannelType.Slack, "https://hooks.slack.test/services/gone");

        Assert.False(await dispatcher.SendTestAsync(channel));
    }

    [Fact]
    public async Task SendTestAsync_SucceedsOn2xx()
    {
        var (dispatcher, _) = CreateDispatcher();
        var channel = Channel(NotificationChannelType.Slack, "https://hooks.slack.test/services/ok");

        Assert.True(await dispatcher.SendTestAsync(channel));
    }

    [Fact]
    public async Task Dispatch_OneThrowingChannel_DoesNotBlockTheOthers()
    {
        var handler = new PerUrlHandler(url => url.Contains("boom")
            ? throw new HttpRequestException("simulated outage")
            : new HttpResponseMessage(HttpStatusCode.OK));
        var dispatcher = new AlertNotificationDispatcher(new StubHttpClientFactory(handler));

        await dispatcher.DispatchAsync(SampleAlert(),
        [
            Channel(NotificationChannelType.Slack, "https://hooks.slack.test/boom"),
            Channel(NotificationChannelType.Slack, "https://hooks.slack.test/healthy"),
        ]);

        // The healthy channel still received its POST despite the first throwing.
        Assert.Contains(handler.AttemptedUrls, u => u == "https://hooks.slack.test/healthy");
    }

    [Fact]
    public async Task Dispatch_EmailChannel_IsSkippedWithoutHttpCallOrThrow()
    {
        var (dispatcher, handler) = CreateDispatcher();
        var channel = Channel(NotificationChannelType.Email, "smtp://mail.test;ops@test");

        await dispatcher.DispatchAsync(SampleAlert(), [channel]);

        Assert.Empty(handler.Requests);
    }

    private sealed class StubHttpClientFactory(HttpMessageHandler handler) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new(handler, disposeHandler: false);
    }

    private sealed class RecordingHandler(HttpStatusCode statusCode) : HttpMessageHandler
    {
        public List<(string Url, string Body)> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
            Requests.Add((request.RequestUri!.ToString(), body));
            return new HttpResponseMessage(statusCode);
        }
    }

    private sealed class PerUrlHandler(Func<string, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public List<string> AttemptedUrls { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var url = request.RequestUri!.ToString();
            AttemptedUrls.Add(url);
            return Task.FromResult(respond(url));
        }
    }
}
