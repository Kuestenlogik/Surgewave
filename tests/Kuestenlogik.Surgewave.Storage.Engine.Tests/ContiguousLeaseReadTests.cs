using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// The lease-borrowing contiguous read (#78) removes the payload-sized copy that
/// <c>ReadBatchesContiguousAsync</c> has to make. These pin the two things that make borrowing
/// safe: the bytes are identical to the copying path, and the lease really is released — because
/// a lease that is never returned leaks the pool, while one released too early hands out another
/// partition's bytes.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ContiguousLeaseReadTests : IDisposable
{
    private readonly string _tempDir;

    public ContiguousLeaseReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-lease-read-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { }
        GC.SuppressFinalize(this);
    }

    private FileStorageEngine CreateFileEngine()
    {
        var dir = Path.Combine(_tempDir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return new FileStorageEngine(dir, baseOffset: 0, createNew: true);
    }

    [Fact]
    public async Task FileEngine_LeasedRead_IsByteIdenticalToTheCopyingRead()
    {
        var engine = CreateFileEngine();
        using var adapter = new StorageEngineSegmentAdapter(engine);

        for (int i = 0; i < 4; i++)
            await adapter.AppendBatchAsync(TestRecordBatch.Create(baseOffset: i * 3, recordCount: 3));
        await adapter.FlushAsync();

        var copied = await adapter.ReadBatchesContiguousAsync(0, 1024 * 1024);
        using var leased = await adapter.ReadContiguousAsync(0, 1024 * 1024);

        Assert.Equal(copied.Data.ToArray(), leased.Data.ToArray());
        Assert.Equal(copied.BatchOffsets, leased.BatchOffsets);
        Assert.NotEmpty(leased.BatchOffsets);
    }

    [Fact]
    public async Task MemoryEngine_LeasedRead_IsByteIdenticalToTheCopyingRead()
    {
        var engine = new MemoryStorageEngine(baseOffset: 0);
        using var adapter = new StorageEngineSegmentAdapter(engine);

        for (int i = 0; i < 3; i++)
            await adapter.AppendBatchAsync(TestRecordBatch.Create(baseOffset: i * 2, recordCount: 2));

        var copied = await adapter.ReadBatchesContiguousAsync(0, 1024 * 1024);
        using var leased = await adapter.ReadContiguousAsync(0, 1024 * 1024);

        Assert.Equal(copied.Data.ToArray(), leased.Data.ToArray());
        Assert.Equal(copied.BatchOffsets, leased.BatchOffsets);
    }

    [Fact]
    public async Task EmptyRead_YieldsAnEmptyResultThatIsSafeToDispose()
    {
        var engine = CreateFileEngine();
        using var adapter = new StorageEngineSegmentAdapter(engine);

        using var read = await adapter.ReadContiguousAsync(0, 1024);

        Assert.True(read.Data.IsEmpty);
        Assert.Empty(read.BatchOffsets);
        read.Dispose(); // disposing twice must stay harmless
    }

    [Fact]
    public async Task RepeatedLeasedReads_DoNotExhaustThePool()
    {
        var engine = CreateFileEngine();
        using var adapter = new StorageEngineSegmentAdapter(engine);

        for (int i = 0; i < 4; i++)
            await adapter.AppendBatchAsync(TestRecordBatch.Create(baseOffset: i * 3, recordCount: 3));
        await adapter.FlushAsync();

        var expected = (await adapter.ReadBatchesContiguousAsync(0, 1024 * 1024)).Data.ToArray();

        // A lease that is never returned would starve the pool; one returned too early would let
        // a later read observe recycled bytes. Many sequential reads catch both.
        for (int i = 0; i < 200; i++)
        {
            using var read = await adapter.ReadContiguousAsync(0, 1024 * 1024);
            Assert.Equal(expected, read.Data.ToArray());
        }
    }

    [Fact]
    public async Task LeasedRead_ConsumedInsideTheScope_SurvivesAConcurrentSecondRead()
    {
        var engine = CreateFileEngine();
        using var adapter = new StorageEngineSegmentAdapter(engine);

        for (int i = 0; i < 4; i++)
            await adapter.AppendBatchAsync(TestRecordBatch.Create(baseOffset: i * 3, recordCount: 3));
        await adapter.FlushAsync();

        var expected = (await adapter.ReadBatchesContiguousAsync(0, 1024 * 1024)).Data.ToArray();

        using var first = await adapter.ReadContiguousAsync(0, 1024 * 1024);
        // Taking a second lease while the first is still open must not disturb it — if both were
        // served from the same recycled buffer, the first read's bytes would change here.
        using (var second = await adapter.ReadContiguousAsync(0, 1024 * 1024))
        {
            Assert.Equal(expected, second.Data.ToArray());
        }

        Assert.Equal(expected, first.Data.ToArray());
    }
}
