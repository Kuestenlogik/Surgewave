using Kuestenlogik.Surgewave.Control.Models;
using Kuestenlogik.Surgewave.Control.Services.Alerting;

namespace Kuestenlogik.Surgewave.Control.Tests.Services;

/// <summary>
/// Persistence + robustness tests for <see cref="AlertingStore"/> (#38). A
/// structurally-valid-but-malformed file (null lists) must degrade to an empty
/// store rather than crash-loop the host via a null-deref in the worker.
/// </summary>
public sealed class AlertingStoreTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"surgewave-store-test-{Guid.NewGuid():N}");
    private readonly string _path;

    public AlertingStoreTests() => _path = Path.Combine(_directory, "alerts.json");

    public void Dispose()
    {
        if (Directory.Exists(_directory))
            Directory.Delete(_directory, recursive: true);
    }

    [Fact]
    public void Load_MissingFile_ReturnsEmptyDocument()
    {
        var doc = new AlertingStore(_path).Load();

        Assert.Empty(doc.Rules);
        Assert.Empty(doc.Channels);
        Assert.Empty(doc.Events);
    }

    [Fact]
    public void SaveThenLoad_RoundTripsRulesAndChannels()
    {
        var store = new AlertingStore(_path);
        store.Save(new AlertingStateDocument
        {
            Rules = [new AlertRule { Name = "r1", Type = AlertRuleType.ConsumerLag, Threshold = 100 }],
            Channels = [new NotificationChannel { Name = "c1", Type = NotificationChannelType.Slack }],
        });

        var loaded = new AlertingStore(_path).Load();

        Assert.Equal("r1", Assert.Single(loaded.Rules).Name);
        Assert.Equal(AlertRuleType.ConsumerLag, loaded.Rules[0].Type);
        Assert.Equal("c1", Assert.Single(loaded.Channels).Name);
    }

    [Fact]
    public void Load_NullListsInFile_NormalizedToEmpty_NoThrow()
    {
        Directory.CreateDirectory(_directory);
        File.WriteAllText(_path, """{"rules":null,"channels":null,"events":null}""");

        var doc = new AlertingStore(_path).Load();

        Assert.NotNull(doc.Rules);
        Assert.NotNull(doc.Channels);
        Assert.NotNull(doc.Events);
        Assert.Empty(doc.Rules);
        Assert.Empty(doc.Channels);
        Assert.Empty(doc.Events);
    }

    [Fact]
    public void Load_NullElementsInList_AreDropped()
    {
        Directory.CreateDirectory(_directory);
        File.WriteAllText(_path, """{"rules":[null],"channels":[],"events":[null,null]}""");

        var doc = new AlertingStore(_path).Load();

        Assert.Empty(doc.Rules);
        Assert.Empty(doc.Events);
    }

    [Fact]
    public void Load_GarbageFile_ReturnsEmptyDocument()
    {
        Directory.CreateDirectory(_directory);
        File.WriteAllText(_path, "{ this is not json");

        var doc = new AlertingStore(_path).Load();

        Assert.Empty(doc.Rules);
    }

    [Fact]
    public void NormalizedNullListDocument_IsUsableByService_NoNullRef()
    {
        // The concrete crash-loop path: a null-list file must let the service's
        // HasRules/EvaluateAsync run without a NullReferenceException.
        Directory.CreateDirectory(_directory);
        File.WriteAllText(_path, """{"rules":null,"channels":null,"events":null}""");

        using var handler = new NoopHandler();
        var dispatcher = new AlertNotificationDispatcher(new SingleHandlerHttpClientFactory(handler));
        var service = new AlertingService(new AlertingStore(_path), dispatcher);

        Assert.False(service.HasRules);
        Assert.Empty(service.GetRules());
    }

    private sealed class SingleHandlerHttpClientFactory(HttpMessageHandler handler) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new(handler, disposeHandler: false);
    }

    private sealed class NoopHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.OK));
    }
}
