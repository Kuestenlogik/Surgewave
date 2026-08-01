using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Segments written in this process now take the memory-mapped read path too (#78): the manager
/// is created lazily on first use instead of only for files reopened after a restart.
///
/// <para>The risk this guards against is a mapped read that disagrees with the file — either by
/// missing a completed append (the mapping lags the write) or by reading past the written region
/// into garbage. The log is append-only, so everything below the write position is final; these
/// tests interleave appends and reads to prove the mapped bytes match the pooled path exactly.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class MmapOnWrittenSegmentTests : IDisposable
{
    private readonly string _tempDir;

    public MmapOnWrittenSegmentTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-mmap-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); } catch { }
        GC.SuppressFinalize(this);
    }

    private string NewDir()
    {
        var dir = Path.Combine(_tempDir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static async Task<byte[]> ReadAllAsync(FileStorageEngine engine, long fromOffset)
    {
        using var lease = await engine.ReadAsync(fromOffset, 1024 * 1024);
        return lease.IsEmpty ? [] : lease.Data.Span.ToArray();
    }

    [Fact]
    public async Task FreshlyWrittenSegment_ReadsMatchTheBytesThatWereAppended()
    {
        var dir = NewDir();
        var batches = new List<byte[]>();

        using (var engine = new FileStorageEngine(dir, baseOffset: 0, createNew: true))
        {
            for (int i = 0; i < 5; i++)
            {
                var batch = TestRecordBatch.Create(baseOffset: i * 3, recordCount: 3);
                batches.Add(batch);
                await engine.AppendAsync(batch);
            }
            await engine.FlushAsync();

            var read = await ReadAllAsync(engine, 0);
            Assert.Equal(batches.SelectMany(b => b).ToArray(), read);
        }
    }

    [Fact]
    public async Task InterleavedAppendAndRead_NeverServesStaleOrShortData()
    {
        var dir = NewDir();
        var written = new List<byte>();

        using var engine = new FileStorageEngine(dir, baseOffset: 0, createNew: true);

        // Each round appends, then reads everything back. The first read creates the mapping;
        // later rounds force the manager to re-map a grown file while earlier views may still
        // be alive.
        for (int round = 0; round < 12; round++)
        {
            var batch = TestRecordBatch.Create(baseOffset: round * 2, recordCount: 2);
            await engine.AppendAsync(batch);
            written.AddRange(batch);

            var read = await ReadAllAsync(engine, 0);
            Assert.Equal(written.ToArray(), read);
        }
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(2L)]
    [InlineData(6L)]
    [InlineData(10L)]
    public async Task MappedRead_IsByteIdenticalToThePooledRead_AtEveryStartOffset(long startOffset)
    {
        // Identical batch bytes into two segments, one mapped and one on the pooled path. What
        // matters is not what the offset semantics are, but that enabling mmap does not change
        // a single byte of the answer.
        var batches = new List<byte[]>();
        for (int i = 0; i < 6; i++)
            batches.Add(TestRecordBatch.Create(baseOffset: i * 2, recordCount: 2));

        byte[] mapped, pooled;

        using (var engine = new FileStorageEngine(NewDir(), baseOffset: 0, createNew: true, useMmap: true))
        {
            foreach (var batch in batches) await engine.AppendAsync(batch);
            await engine.FlushAsync();
            mapped = await ReadAllAsync(engine, startOffset);
        }

        using (var engine = new FileStorageEngine(NewDir(), baseOffset: 0, createNew: true, useMmap: false))
        {
            foreach (var batch in batches) await engine.AppendAsync(batch);
            await engine.FlushAsync();
            pooled = await ReadAllAsync(engine, startOffset);
        }

        Assert.Equal(pooled, mapped);
    }

    [Fact]
    public async Task EmptySegment_FallsBackWithoutThrowing()
    {
        var dir = NewDir();
        using var engine = new FileStorageEngine(dir, baseOffset: 0, createNew: true);

        // Mapping an empty file throws; the lazy path must simply not map yet.
        using var lease = await engine.ReadAsync(0, 1024);
        Assert.True(lease.IsEmpty);

        // And it must start working once data arrives.
        var batch = TestRecordBatch.Create(baseOffset: 0, recordCount: 2);
        await engine.AppendAsync(batch);
        await engine.FlushAsync();

        Assert.Equal(batch, await ReadAllAsync(engine, 0));
    }

    [Fact]
    public async Task ReopenedSegment_StillReadsIdenticalBytes()
    {
        var dir = NewDir();
        byte[] expected;

        using (var engine = new FileStorageEngine(dir, baseOffset: 0, createNew: true))
        {
            var all = new List<byte>();
            for (int i = 0; i < 4; i++)
            {
                var batch = TestRecordBatch.Create(baseOffset: i * 2, recordCount: 2);
                all.AddRange(batch);
                await engine.AppendAsync(batch);
            }
            await engine.FlushAsync();
            expected = all.ToArray();
        }

        // The reopen path mapped eagerly before this change and must stay equivalent.
        using var reopened = new FileStorageEngine(dir, baseOffset: 0, createNew: false);
        Assert.Equal(expected, await ReadAllAsync(reopened, 0));
    }
}
