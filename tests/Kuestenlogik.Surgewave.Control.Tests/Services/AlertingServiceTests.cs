using System.Net;
using Kuestenlogik.Surgewave.Control.Models;
using Kuestenlogik.Surgewave.Control.Services;
using Kuestenlogik.Surgewave.Control.Services.Alerting;

namespace Kuestenlogik.Surgewave.Control.Tests.Services;

/// <summary>
/// Behaviour of the server-side alerting engine (#38): fired alerts persist to
/// disk and survive a service restart, cooldowns suppress repeats then allow a
/// re-fire once the window elapses, rate rules fire off derived rates, and
/// notifications route to the configured channels.
/// </summary>
public sealed class AlertingServiceTests : IDisposable
{
    private static readonly DateTime Base = new(2026, 7, 6, 12, 0, 0, DateTimeKind.Utc);

    private readonly string _storePath = Path.Combine(
        Path.GetTempPath(), $"surgewave-alerting-test-{Guid.NewGuid():N}", "alerts.json");

    private readonly RecordingHandler _httpHandler = new();

    public void Dispose()
    {
        _httpHandler.Dispose();
        var directory = Path.GetDirectoryName(_storePath)!;
        if (Directory.Exists(directory))
            Directory.Delete(directory, recursive: true);
    }

    private AlertingService CreateService(TimeProvider? timeProvider = null)
    {
        var dispatcher = new AlertNotificationDispatcher(new StubHttpClientFactory(_httpHandler));
        return new AlertingService(new AlertingStore(_storePath), dispatcher, timeProvider);
    }

    private static AlertRule LagRule(double threshold = 100, int cooldownMinutes = 5) => new()
    {
        Name = "lag",
        Type = AlertRuleType.ConsumerLag,
        Threshold = threshold,
        CooldownMinutes = cooldownMinutes,
        Enabled = true,
    };

    private static ConsumerGroupLag Lag(long totalLag) => new("group", "Stable", totalLag, []);

    private static NotificationChannel Channel(string name, bool enabled = true) => new()
    {
        Name = name,
        Type = NotificationChannelType.Slack,
        Enabled = enabled,
        Config = { ["url"] = $"https://hooks.slack.test/{name}" },
    };

    [Fact]
    public async Task EvaluateAsync_FiresAlert_AndPersistsAcrossRestart()
    {
        var service = CreateService();
        service.SaveRules([LagRule()]);

        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);

        var fired = Assert.Single(service.GetEvents());
        Assert.Equal("lag", fired.RuleName);
        Assert.Equal(500, fired.CurrentValue);
        Assert.False(fired.IsResolved);

        // Fresh instance over the same store — simulates a Control restart.
        var restarted = CreateService();
        Assert.Single(restarted.GetEvents());
        Assert.Single(restarted.GetRules());
    }

    [Fact]
    public async Task EvaluateAsync_Cooldown_SuppressesRepeatWithinWindow()
    {
        var service = CreateService();
        service.SaveRules([LagRule(cooldownMinutes: 60)]);

        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);
        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);

        Assert.Single(service.GetEvents());
    }

    [Fact]
    public async Task EvaluateAsync_Cooldown_RefiresAfterWindowElapses()
    {
        var time = new MutableTimeProvider(Base);
        var service = CreateService(time);
        service.SaveRules([LagRule(cooldownMinutes: 10)]);

        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);
        Assert.Single(service.GetEvents());

        // Still inside the cooldown window — suppressed.
        time.Advance(TimeSpan.FromMinutes(5));
        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);
        Assert.Single(service.GetEvents());

        // Window elapsed — the rule fires again.
        time.Advance(TimeSpan.FromMinutes(6));
        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);
        Assert.Equal(2, service.GetEvents().Count);
    }

    [Fact]
    public async Task EvaluateAsync_LowThroughput_FiresOnlyOnceRateIsAvailable()
    {
        var service = CreateService();
        service.SaveRules([new AlertRule { Name = "lt", Type = AlertRuleType.LowThroughput, Threshold = 0, Enabled = true }]);

        var first = new MetricsSnapshot { Timestamp = Base, MessagesProducedTotal = 1_000_000 };
        var second = new MetricsSnapshot { Timestamp = Base.AddSeconds(30), MessagesProducedTotal = 1_000_000 };

        // First cycle establishes the baseline — no rate yet, so a busy broker
        // with a huge lifetime total must NOT be reported as idle.
        await service.EvaluateAsync(first, [], brokerReachable: true);
        Assert.Empty(service.GetEvents());

        // Second cycle: 0 msg/s over the interval <= threshold 0 -> fires.
        await service.EvaluateAsync(second, [], brokerReachable: true);
        Assert.Single(service.GetEvents());
    }

    [Fact]
    public async Task EvaluateAsync_DisabledRule_NeverFires()
    {
        var service = CreateService();
        var rule = LagRule();
        rule.Enabled = false;
        service.SaveRules([rule]);

        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);

        Assert.Empty(service.GetEvents());
    }

    [Fact]
    public async Task EvaluateAsync_DispatchesToEnabledChannelsOnly()
    {
        var service = CreateService();
        service.SaveRules([LagRule()]);
        service.SaveChannels([Channel("ops"), Channel("muted", enabled: false)]);

        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);

        var request = Assert.Single(_httpHandler.Requests);
        Assert.Equal("https://hooks.slack.test/ops", request.Url);
        Assert.Contains("lag", request.Body);
    }

    [Fact]
    public async Task EvaluateAsync_RuleWithExplicitChannels_OnlyNotifiesThoseChannels()
    {
        var service = CreateService();
        var only = Channel("only");
        var other = Channel("other");
        var rule = LagRule();
        rule.NotificationChannels = [only.Id];
        service.SaveChannels([only, other]);
        service.SaveRules([rule]);

        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);

        var request = Assert.Single(_httpHandler.Requests);
        Assert.Equal("https://hooks.slack.test/only", request.Url);
    }

    [Fact]
    public async Task EvaluateAsync_RuleWithUnknownChannelId_NotifiesNoOne()
    {
        var service = CreateService();
        var rule = LagRule();
        rule.NotificationChannels = ["does-not-exist"];
        service.SaveChannels([Channel("ops")]);
        service.SaveRules([rule]);

        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);

        Assert.Empty(_httpHandler.Requests);
        Assert.Single(service.GetEvents()); // alert still recorded
    }

    [Fact]
    public async Task AcknowledgeAll_OnlyMarksPendingUnresolvedEvents()
    {
        var service = CreateService();
        service.SaveRules([LagRule(), LagRule()]); // two distinct rule ids -> two events
        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);

        var events = service.GetEvents();
        Assert.Equal(2, events.Count);
        service.Resolve(events[0].Id);

        var acknowledged = service.AcknowledgeAll("ops-user");

        Assert.Equal(1, acknowledged);
        Assert.Equal(0, service.AcknowledgeAll("ops-user")); // nothing left pending
    }

    [Fact]
    public async Task AcknowledgeAndResolve_UpdateEventState_AndGuardNoOps()
    {
        var service = CreateService();
        service.SaveRules([LagRule()]);
        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);
        var alertId = service.GetEvents()[0].Id;

        Assert.False(service.Acknowledge("unknown", "x"));
        Assert.False(service.Resolve("unknown"));

        Assert.True(service.Acknowledge(alertId, "ops-user"));
        Assert.False(service.Acknowledge(alertId, "again")); // already acknowledged
        Assert.Equal("ops-user", service.GetEvents()[0].AcknowledgedBy);

        Assert.True(service.Resolve(alertId));
        Assert.False(service.Resolve(alertId)); // already resolved
        Assert.True(service.GetEvents()[0].IsResolved);

        Assert.Equal(1, service.ClearResolvedHistory());
        Assert.Empty(service.GetEvents());
    }

    [Fact]
    public async Task GetRules_ReturnsIsolatedClones_MutatingResultDoesNotCorruptState()
    {
        var service = CreateService();
        service.SaveRules([LagRule()]);

        var rules = service.GetRules();
        rules[0].Name = "MUTATED";
        rules[0].Threshold = 999_999;

        var fresh = service.GetRules();
        Assert.Equal("lag", fresh[0].Name);
        Assert.Equal(100, fresh[0].Threshold);
    }

    [Fact]
    public async Task EvaluateAsync_TrimsHistoryToCap()
    {
        // Pre-seed the store just under the cap with old events, then fire one more.
        var seeded = new AlertingStateDocument();
        for (var i = 0; i < 500; i++)
            seeded.Events.Add(new AlertEvent { RuleId = "old", RuleName = "old", FiredAt = Base.AddSeconds(i) });
        new AlertingStore(_storePath).Save(seeded);

        var time = new MutableTimeProvider(Base.AddDays(1));
        var service = CreateService(time);
        service.SaveRules([LagRule()]);

        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(500)], brokerReachable: true);

        var events = service.GetEvents();
        Assert.Equal(500, events.Count);
        Assert.DoesNotContain(events, e => e.FiredAt == Base); // the single oldest was evicted
        Assert.Contains(events, e => e.RuleName == "lag");     // the freshly fired alert survived
    }

    [Fact]
    public async Task Changed_RaisedEveryCycle_ForLivenessTimestamp()
    {
        var service = CreateService();
        service.SaveRules([LagRule()]);
        var changedCount = 0;
        service.Changed += () => changedCount++;

        // No alert fires (lag below threshold), but Changed must still fire so the
        // page can refresh LastEvaluatedAt.
        await service.EvaluateAsync(new MetricsSnapshot(), [Lag(1)], brokerReachable: true);

        Assert.True(changedCount >= 1);
        Assert.NotNull(service.LastEvaluatedAt);
    }

    private sealed class MutableTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan by) => _now += by;
    }

    private sealed class StubHttpClientFactory(HttpMessageHandler handler) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new(handler, disposeHandler: false);
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        public List<(string Url, string Body)> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
            Requests.Add((request.RequestUri!.ToString(), body));
            return new HttpResponseMessage(HttpStatusCode.OK);
        }
    }
}
