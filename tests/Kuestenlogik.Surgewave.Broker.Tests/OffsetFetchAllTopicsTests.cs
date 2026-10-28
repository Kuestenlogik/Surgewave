using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// An OffsetFetch with null topics answers every offset the group has committed (#195): a Java admin client's
/// `kafka-consumer-groups --describe` asks exactly that, and an empty answer showed no offsets for any group.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class OffsetFetchAllTopicsTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-offsetfetch-all-" + Guid.NewGuid().ToString("N"));
    private readonly OffsetStore _store;
    private readonly ConsumerGroupCoordinator _coordinator;

    public OffsetFetchAllTopicsTests()
    {
        Directory.CreateDirectory(_dataDir);
        _store = new OffsetStore(_dataDir, NullLogger<OffsetStore>.Instance);
        _coordinator = new ConsumerGroupCoordinator(NullLogger<ConsumerGroupCoordinator>.Instance, _store);
    }

    public void Dispose()
    {
        _store.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void NullTopicsFetchEveryCommittedOffsetOfTheGroup()
    {
        // A KIP-848 group's commits live only in the store, so the group has no in-memory entry.
        _store.CommitOffset("g-all", "orders", 1, 40);
        _store.CommitOffset("g-all", "orders", 0, 25);
        _store.CommitOffset("g-all", "audit.log", 0, 7);
        _store.CommitOffset("g-other", "orders", 0, 99);

        var result = _coordinator.FetchOffsets(new OffsetFetchCommand
        {
            UseTopicId = false,
            Groups = [new OffsetFetchGroupRequest { GroupId = "g-all", Topics = null }],
        });

        var topics = Assert.Single(result.Groups).Topics;
        Assert.Collection(topics,
            t =>
            {
                Assert.Equal("audit.log", t.Topic);
                Assert.Equal(7, Assert.Single(t.Partitions).CommittedOffset);
            },
            t =>
            {
                Assert.Equal("orders", t.Topic);
                Assert.Equal([(0, 25L), (1, 40L)], t.Partitions.Select(p => (p.PartitionIndex, p.CommittedOffset)));
            });
    }

    [Fact]
    public void NullTopicsOfAGroupWithoutCommitsFetchNothing()
    {
        var result = _coordinator.FetchOffsets(new OffsetFetchCommand
        {
            UseTopicId = false,
            Groups = [new OffsetFetchGroupRequest { GroupId = "g-none", Topics = null }],
        });

        Assert.Empty(Assert.Single(result.Groups).Topics);
    }
}
