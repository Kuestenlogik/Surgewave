using Kuestenlogik.Surgewave.Clustering.GeoReplication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.GeoReplication;

[Trait("Category", TestCategories.Unit)]
public sealed class MirrorTopicManagerTests : IAsyncLifetime, IDisposable
{
    private readonly LogManager _logManager;
    private readonly MirrorTopicManager _manager;

    public MirrorTopicManagerTests()
    {
        _logManager = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-mirror-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        _manager = new MirrorTopicManager(_logManager, NullLogger.Instance);
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        _logManager.Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose() => _logManager.Dispose();

    [Fact]
    public async Task CreateMirrorTopic_Success()
    {
        // Act
        var result = await _manager.CreateMirrorTopicAsync("link-1", "orders", 3);

        // Assert
        Assert.True(result);
        var metadata = _logManager.GetTopicMetadata("orders");
        Assert.NotNull(metadata);
        Assert.True(metadata.IsMirror);
        Assert.True(metadata.IsReadOnly);
        Assert.Equal("link-1", metadata.SourceLinkId);
    }

    [Fact]
    public async Task CreateMirrorTopic_AlreadyExists_ReturnsFalse()
    {
        // Arrange
        await _manager.CreateMirrorTopicAsync("link-1", "orders", 3);

        // Act
        var result = await _manager.CreateMirrorTopicAsync("link-1", "orders", 3);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task IsMirrorTopic_ReturnsTrueForMirror()
    {
        // Arrange
        await _manager.CreateMirrorTopicAsync("link-1", "events", 2);

        // Act & Assert
        Assert.True(_manager.IsMirrorTopic("events"));
    }

    [Fact]
    public void IsMirrorTopic_ReturnsFalseForNormal()
    {
        // Act & Assert
        Assert.False(_manager.IsMirrorTopic("non-existent-topic"));
    }

    [Fact]
    public async Task IsReadOnly_ReturnsTrueForMirror()
    {
        // Arrange
        await _manager.CreateMirrorTopicAsync("link-1", "metrics", 1);

        // Act & Assert
        Assert.True(_manager.IsReadOnly("metrics"));
    }

    [Fact]
    public async Task GetMirrorTopics_ReturnsAll()
    {
        // Arrange
        await _manager.CreateMirrorTopicAsync("link-1", "topic-a", 2);
        await _manager.CreateMirrorTopicAsync("link-1", "topic-b", 3);
        await _manager.CreateMirrorTopicAsync("link-2", "topic-c", 1);

        // Act
        var topics = _manager.GetMirrorTopics();

        // Assert
        Assert.Equal(3, topics.Count);
    }

    [Fact]
    public async Task GetMirrorTopicState_ReturnsState()
    {
        // Arrange
        await _manager.CreateMirrorTopicAsync("link-1", "orders", 4);

        // Act
        var state = _manager.GetMirrorTopicState("orders");

        // Assert
        Assert.NotNull(state);
        Assert.Equal("link-1", state.LinkId);
        Assert.Equal("orders", state.SourceTopic);
        Assert.Equal(4, state.PartitionCount);
        Assert.True(state.IsReadOnly);
    }

    [Fact]
    public void GetMirrorTopicState_NotFound_ReturnsNull()
    {
        // Act & Assert
        Assert.Null(_manager.GetMirrorTopicState("unknown-topic"));
    }

    [Fact]
    public async Task GetMirrorTopicsForLink_FiltersCorrectly()
    {
        // Arrange
        await _manager.CreateMirrorTopicAsync("link-1", "topic-a", 2);
        await _manager.CreateMirrorTopicAsync("link-1", "topic-b", 3);
        await _manager.CreateMirrorTopicAsync("link-2", "topic-c", 1);

        // Act
        var link1Topics = _manager.GetMirrorTopicsForLink("link-1");
        var link2Topics = _manager.GetMirrorTopicsForLink("link-2");

        // Assert
        Assert.Equal(2, link1Topics.Count);
        Assert.Single(link2Topics);
        Assert.All(link1Topics, t => Assert.Equal("link-1", t.LinkId));
        Assert.All(link2Topics, t => Assert.Equal("link-2", t.LinkId));
    }

    [Fact]
    public async Task FailoverMirrorTopic_SetsWritable()
    {
        // Arrange
        await _manager.CreateMirrorTopicAsync("link-1", "orders", 3);

        // Act
        var result = await _manager.FailoverMirrorTopicAsync("orders", fetcher: null);

        // Assert
        Assert.True(result);
        Assert.False(_manager.IsMirrorTopic("orders"));
        Assert.False(_manager.IsReadOnly("orders"));

        var metadata = _logManager.GetTopicMetadata("orders");
        Assert.NotNull(metadata);
        Assert.False(metadata.IsMirror);
        Assert.False(metadata.IsReadOnly);
        Assert.Null(metadata.SourceLinkId);
    }

    [Fact]
    public async Task FailoverMirrorTopic_NotMirror_ReturnsFalse()
    {
        // Act
        var result = await _manager.FailoverMirrorTopicAsync("unknown-topic", fetcher: null);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task PromoteMirrorTopic_WithZeroLag_SetsWritable()
    {
        // Arrange
        await _manager.CreateMirrorTopicAsync("link-1", "orders", 2);

        // Act - null fetcher means lag is always 0
        var result = await _manager.PromoteMirrorTopicAsync("orders", fetcher: null, TimeSpan.FromSeconds(5));

        // Assert
        Assert.True(result);
        Assert.False(_manager.IsMirrorTopic("orders"));
        Assert.False(_manager.IsReadOnly("orders"));
    }
}
