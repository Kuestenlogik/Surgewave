using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Creating a topic while the first produce to it creates the same partition log must end with one log,
/// not an error. Auto-create does exactly that: the metadata request creates the topic while the produce
/// that follows brings the partition log into being through <see cref="LogManager.GetOrCreateLog"/>.
/// <c>CreateTopicAsync</c> built its logs outside the per-partition creation lock, so both opened the same
/// segment file; on Windows the second open failed with "being used by another process" and the produce
/// was answered with an unknown error.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CreateTopicRacesFirstProduceTests : IDisposable
{
    private const int PartitionCount = 8;

    private readonly string _dataDir;
    private readonly LogManager _logManager;

    public CreateTopicRacesFirstProduceTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-createrace-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        var segments = new StorageEngineSegmentFactory(new FileStorageEngineFactory(useMmap: false), isPersistent: true);
        _logManager = new LogManager(_dataDir, segments, persistTopicsToFile: false);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task CreateTopic_AndConcurrentFirstProduces_EndWithOneLogPerPartition()
    {
        for (var round = 0; round < 20; round++)
        {
            var topic = $"autocreate-{round}";
            using var start = new ManualResetEventSlim();

            var producers = Enumerable.Range(0, PartitionCount)
                .Select(p => Task.Run(() =>
                {
                    start.Wait();
                    return _logManager.GetOrCreateLog(new TopicPartition { Topic = topic, Partition = p });
                }))
                .ToArray();
            var create = Task.Run(async () =>
            {
                start.Wait();
                await _logManager.CreateTopicAsync(topic, PartitionCount);
            });

            start.Set();
            var logs = await Task.WhenAll(producers);
            await create;

            for (var p = 0; p < PartitionCount; p++)
            {
                Assert.Same(logs[p], _logManager.GetLog(new TopicPartition { Topic = topic, Partition = p }));
            }
        }
    }
}
