using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Verifies the KIP-1010 topic-lifecycle hook surface: hooks fire on create, config
/// change, and delete; a throwing hook is isolated from the rest of the chain so a
/// buggy plugin cannot stall topic operations.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TopicLifecycleHookTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;

    public TopicLifecycleHookTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-topic-hooks-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task CreateTopic_FiresOnTopicCreatedHook()
    {
        var hook = new RecordingHook();
        _logManager.RegisterTopicHook(hook);

        await _logManager.CreateTopicAsync("hook-topic", partitionCount: 3);

        var ev = Assert.Single(hook.Events);
        Assert.Equal("created", ev.Kind);
        Assert.Equal("hook-topic", ev.Context.TopicName);
        Assert.Equal(3, ev.Context.PartitionCount);
        Assert.Null(ev.Context.PreviousConfig);
    }

    [Fact]
    public async Task UpdateTopicConfig_FiresOnTopicConfigChangedWithPreviousSnapshot()
    {
        var hook = new RecordingHook();
        await _logManager.CreateTopicAsync("hook-cfg", partitionCount: 1, config: new() { ["retention.ms"] = "60000" });
        _logManager.RegisterTopicHook(hook);

        var ok = _logManager.UpdateTopicConfig("hook-cfg", new() { ["retention.ms"] = "120000" });

        Assert.True(ok);
        var ev = Assert.Single(hook.Events);
        Assert.Equal("config-changed", ev.Kind);
        Assert.Equal("60000", ev.Context.PreviousConfig!["retention.ms"]);
        Assert.Equal("120000", ev.Context.Config["retention.ms"]);
    }

    [Fact]
    public async Task DeleteTopic_FiresOnTopicDeletedHook()
    {
        var hook = new RecordingHook();
        await _logManager.CreateTopicAsync("hook-del", partitionCount: 2);
        _logManager.RegisterTopicHook(hook);

        await _logManager.DeleteTopicAsync("hook-del");

        var ev = Assert.Single(hook.Events);
        Assert.Equal("deleted", ev.Kind);
        Assert.Equal("hook-del", ev.Context.TopicName);
    }

    [Fact]
    public async Task ThrowingHook_DoesNotAbortOperation_AndOtherHooksStillFire()
    {
        var failing = new ThrowingHook();
        var following = new RecordingHook();
        _logManager.RegisterTopicHook(failing);
        _logManager.RegisterTopicHook(following);

        await _logManager.CreateTopicAsync("hook-resilient", partitionCount: 1);

        var following_ev = Assert.Single(following.Events);
        Assert.Equal("created", following_ev.Kind);
        Assert.NotNull(_logManager.GetTopicMetadata("hook-resilient"));
    }

    private sealed class RecordingHook : ITopicLifecycleHook
    {
        public List<(string Kind, TopicLifecycleContext Context)> Events { get; } = [];

        public Task OnTopicCreatedAsync(TopicLifecycleContext context, CancellationToken cancellationToken)
        { Events.Add(("created", context)); return Task.CompletedTask; }

        public Task OnTopicConfigChangedAsync(TopicLifecycleContext context, CancellationToken cancellationToken)
        { Events.Add(("config-changed", context)); return Task.CompletedTask; }

        public Task OnTopicDeletedAsync(TopicLifecycleContext context, CancellationToken cancellationToken)
        { Events.Add(("deleted", context)); return Task.CompletedTask; }
    }

    private sealed class ThrowingHook : ITopicLifecycleHook
    {
        public Task OnTopicCreatedAsync(TopicLifecycleContext context, CancellationToken cancellationToken)
            => throw new InvalidOperationException("test failure in hook");
        public Task OnTopicConfigChangedAsync(TopicLifecycleContext context, CancellationToken cancellationToken)
            => throw new InvalidOperationException("test failure in hook");
        public Task OnTopicDeletedAsync(TopicLifecycleContext context, CancellationToken cancellationToken)
            => throw new InvalidOperationException("test failure in hook");
    }
}
