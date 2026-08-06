using Kuestenlogik.Surgewave.Clustering.GeoReplication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// End-to-end check that a hostile topic name cannot forge a log record
/// (CodeQL cs/log-forging, alerts #113 / #114). Mirror topic names originate
/// from the remote cluster's metadata response, so they are attacker-influenced
/// on a link pointed at an untrusted peer.
/// </summary>
/// <remarks>
/// This is the one sink of the fifteen that reaches its logging branch with no
/// setup at all — an unknown topic short-circuits straight into the warning —
/// which makes it the cheapest place to pin that the sanitiser is actually
/// applied at a call site, not merely available.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class MirrorTopicLogForgingTests
{
    private const string ForgedTopicName =
        "orders\ninfo: Surgewave.Broker[0] Mirror promoted, lag 0";

    private static (MirrorTopicManager Manager, RecordingLogger Logger) CreateManager()
    {
        var logManager = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var logger = new RecordingLogger();
        return (new MirrorTopicManager(logManager, logger), logger);
    }

    [Fact]
    public async Task PromoteMirrorTopic_UnknownTopicWithNewline_DoesNotSplitTheLogRecord()
    {
        var (manager, logger) = CreateManager();

        var promoted = await manager.PromoteMirrorTopicAsync(
            ForgedTopicName, fetcher: null, timeout: TimeSpan.FromMilliseconds(50));

        Assert.False(promoted);
        var message = Assert.Single(logger.Messages);
        Assert.DoesNotContain('\n', message);
        Assert.DoesNotContain('\r', message);
        Assert.Contains("is not a mirror topic", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FailoverMirrorTopic_UnknownTopicWithNewline_DoesNotSplitTheLogRecord()
    {
        var (manager, logger) = CreateManager();

        var failed = await manager.FailoverMirrorTopicAsync(ForgedTopicName, fetcher: null);

        Assert.False(failed);
        var message = Assert.Single(logger.Messages);
        Assert.DoesNotContain('\n', message);
        Assert.DoesNotContain('\r', message);
    }

    /// <summary>Captures the rendered message of every log call.</summary>
    private sealed class RecordingLogger : ILogger
    {
        public List<string> Messages { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Messages.Add(formatter(state, exception));
    }
}
