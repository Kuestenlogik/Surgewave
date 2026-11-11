using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Testing.Chaos;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Storage;

/// <summary>
/// The storage half of #162: an I/O failure a partition log cannot recover from takes its log directory offline, as
/// Kafka's LogDirFailureChannel does, and the directory says so under a stable id the heartbeat can report.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class LogDirectoryFailureTests : IDisposable
{
    private const int BrokerId = 1;

    private readonly ChaosEngine _chaos = new();
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-logdir-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logs;

    public LogDirectoryFailureTests()
    {
        _logs = new LogManager(
            _dataDir,
            new ChaosLogSegmentFactory(new MemoryLogSegmentFactory(), _chaos, BrokerId),
            persistTopicsToFile: false);
    }

    public void Dispose()
    {
        _logs.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task AFailedAppendTakesTheDirectoryOfflineForEveryLogInIt()
    {
        var failing = new TopicPartition { Topic = "orders", Partition = 0 };
        var other = new TopicPartition { Topic = "payments", Partition = 3 };
        var failingLog = _logs.GetOrCreateLog(failing);
        var otherLog = _logs.GetOrCreateLog(other);
        await failingLog.AppendBatchAsync(Batch());

        var wentOffline = 0;
        _logs.LogDirectory.WentOffline += _ => Interlocked.Increment(ref wentOffline);
        Assert.False(_logs.LogDirectory.IsOffline);
        Assert.Empty(_logs.OfflineLogDirectoryIds);

        using (DiskFailureScenario.Create(_chaos, BrokerId))
        {
            var failure = await Assert.ThrowsAsync<LogDirOfflineException>(async () => await failingLog.AppendBatchAsync(Batch()));
            Assert.IsType<IOException>(failure.InnerException, exactMatch: false);
            Assert.Equal(_logs.LogDirectory.Id, failure.DirectoryId);
        }

        // The handshake the broker waits on, and the state its heartbeat reads.
        await _logs.LogDirectory.Offline.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.True(_logs.LogDirectory.IsOffline);
        Assert.Equal([_logs.LogDirectory.Id], _logs.OfflineLogDirectoryIds);
        Assert.True(_logs.IsLogDirOffline(other));

        // The disk is back, but the directory stays offline — as in Kafka, only a restart brings it back. And the
        // other partition, whose own writes never failed, is offline with it: the failure is the directory's.
        await Assert.ThrowsAsync<LogDirOfflineException>(async () => await otherLog.AppendBatchAsync(Batch()));
        await Assert.ThrowsAsync<LogDirOfflineException>(async () => await otherLog.ReadBatchesAsync(0));
        await Assert.ThrowsAsync<LogDirOfflineException>(async () => await failingLog.ReadContiguousAsync(0));
        Assert.Throws<LogDirOfflineException>(() => _logs.GetOrCreateLog(new TopicPartition { Topic = "new", Partition = 0 }));

        Assert.Equal(1, wentOffline);
    }

    [Fact]
    public async Task AFailedReadTakesTheDirectoryOffline()
    {
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        var log = _logs.GetOrCreateLog(tp);
        await log.AppendBatchAsync(Batch());

        using (DiskFailureScenario.Create(_chaos, BrokerId))
            await Assert.ThrowsAsync<LogDirOfflineException>(async () => await log.ReadBatchesContiguousAsync(0));

        Assert.True(_logs.LogDirectory.IsOffline);
    }

    [Fact]
    public async Task AFailureOfAnotherBrokersDiskLeavesThisDirectoryOnline()
    {
        var log = _logs.GetOrCreateLog(new TopicPartition { Topic = "orders", Partition = 0 });

        using (DiskFailureScenario.Create(_chaos, BrokerId + 1))
            await log.AppendBatchAsync(Batch());

        Assert.False(_logs.LogDirectory.IsOffline);
        Assert.False(_logs.LogDirectory.Offline.IsCompleted);
    }

    [Fact]
    public void APersistentDirectoryKeepsItsIdAcrossOpens()
    {
        var dir = Path.Combine(_dataDir, "persistent");

        var first = LogDirectory.Open(dir, persistent: true);
        var second = LogDirectory.Open(dir, persistent: true);

        Assert.NotEqual(Guid.Empty, first.Id);
        Assert.Equal(first.Id, second.Id);
        Assert.True(File.Exists(Path.Combine(dir, LogDirectory.MetaPropertiesFileName)));
    }

    [Fact]
    public void ANonPersistentDirectoryGetsAnIdOfItsOwn()
    {
        var first = LogDirectory.Open(_dataDir, persistent: false);
        var second = LogDirectory.Open(_dataDir, persistent: false);

        Assert.NotEqual(first.Id, second.Id);
        Assert.False(File.Exists(Path.Combine(_dataDir, LogDirectory.MetaPropertiesFileName)));
    }

    private static byte[] Batch()
    {
        var batch = new byte[61];
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        batch[16] = 2;
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        return batch;
    }
}
