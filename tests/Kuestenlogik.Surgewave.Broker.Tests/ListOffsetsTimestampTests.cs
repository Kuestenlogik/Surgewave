using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-1059 (`EARLIEST_LOCAL_TIMESTAMP=-4`) and the surrounding KIP-734 / KIP-1005 /
/// KIP-1023 reserved timestamps on <c>ListOffsets</c>. Surgewave's broker-internal
/// tiered storage means the local-log boundary equals the global one for non-tiered
/// brokers, but the wire contract still has to recognise every reserved value
/// instead of falling through to <c>FindOffsetByTimestamp(timestamp)</c>.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ListOffsetsTimestampTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager LogManager;

    // The partition log is owned by the LogManager; we look it up on demand
    // rather than caching it as a field so the analyzer doesn't think we leak
    // a disposable.
    private IPartitionLog Log => LogManager.GetOrCreateLog(new TopicPartition { Topic = Topic, Partition = 0 });

    private const string Topic = "kip1059-topic";

    public ListOffsetsTimestampTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-listoffsets-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        LogManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        LogManager.CreateTopicAsync(Topic, partitionCount: 1).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        LogManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public void EarliestLocal_MinusFour_ReturnsLogStartOffset()
    {
        Assert.Equal(Log.LogStartOffset, DataApiHandler.ResolveListOffsetTimestamp(Log, -4));
    }

    [Fact]
    public void Earliest_MinusTwo_AndEarliestLocal_AgreeOnNonTieredBroker()
    {
        Assert.Equal(
            DataApiHandler.ResolveListOffsetTimestamp(Log, -2),
            DataApiHandler.ResolveListOffsetTimestamp(Log, -4));
    }

    [Fact]
    public void Latest_MinusOne_ReturnsNextOffset()
    {
        Assert.Equal(Log.NextOffset, DataApiHandler.ResolveListOffsetTimestamp(Log, -1));
    }

    [Fact]
    public void LastTiered_MinusFive_ReturnsMinusOneOnNonTieredBroker()
    {
        Assert.Equal(-1, DataApiHandler.ResolveListOffsetTimestamp(Log, -5));
    }

    [Fact]
    public void EarliestPendingUpload_MinusSix_ReturnsMinusOneOnNonTieredBroker()
    {
        Assert.Equal(-1, DataApiHandler.ResolveListOffsetTimestamp(Log, -6));
    }

    [Fact]
    public void MaxTimestamp_MinusThree_ReturnsMinusOneOnEmptyLog()
    {
        // FindOffsetByTimestamp(long.MaxValue) on an empty log returns null →
        // resolver maps to -1 (no record found).
        Assert.Equal(-1, DataApiHandler.ResolveListOffsetTimestamp(Log, -3));
    }

    [Fact]
    public void NullLog_ReturnsZeroForOffsetQueries()
    {
        Assert.Equal(0, DataApiHandler.ResolveListOffsetTimestamp(log: null, timestamp: -1));
        Assert.Equal(0, DataApiHandler.ResolveListOffsetTimestamp(log: null, timestamp: -2));
        Assert.Equal(0, DataApiHandler.ResolveListOffsetTimestamp(log: null, timestamp: -4));
        Assert.Equal(-1, DataApiHandler.ResolveListOffsetTimestamp(log: null, timestamp: -3));
        Assert.Equal(-1, DataApiHandler.ResolveListOffsetTimestamp(log: null, timestamp: -5));
        Assert.Equal(-1, DataApiHandler.ResolveListOffsetTimestamp(log: null, timestamp: -6));
    }

    [Fact]
    public void PositiveTimestamp_FallsThroughToFindOffsetByTimestamp()
    {
        // Empty log → FindOffsetByTimestamp returns null → resolver returns
        // log.NextOffset (the contract preserved from the original handler).
        Assert.Equal(Log.NextOffset, DataApiHandler.ResolveListOffsetTimestamp(Log, 12345));
    }
}
