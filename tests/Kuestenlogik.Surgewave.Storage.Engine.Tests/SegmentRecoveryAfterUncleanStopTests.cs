using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// A segment reopened after a broker that did not shut down cleanly holds what its log holds (#222). The index is
/// written behind the log and flushed in batches; a segment whose index was never flushed reopened as empty, so its
/// records could not be read and new ones were written over their offsets — a killed broker lost every record and
/// every committed group offset since its last index flush.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SegmentRecoveryAfterUncleanStopTests : IDisposable
{
    private const int Batches = 5;
    private const int RecordsPerBatch = 3;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "surgewave-segment-recovery", Guid.NewGuid().ToString("N"));
    private readonly List<byte[]> _written = [];

    public SegmentRecoveryAfterUncleanStopTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task ASegmentWhoseIndexWasNeverWrittenReopensWithEveryBatch()
    {
        await WriteSegmentAsync();
        File.WriteAllBytes(IndexPath, []); // the stop came before the first index flush

        using var reopened = Reopen();

        Assert.Equal(Batches * RecordsPerBatch, reopened.CurrentOffset);
        Assert.Equal(_written.SelectMany(b => b).ToArray(), await ReadAllAsync(reopened));
    }

    [Fact]
    public async Task ASegmentWhoseIndexLagsTheLogReopensWithEveryBatch()
    {
        await WriteSegmentAsync();
        using (var index = File.Open(IndexPath, FileMode.Open))
            index.SetLength(2 * 16); // only the first two batches made it into the index

        using var reopened = Reopen();

        Assert.Equal(Batches * RecordsPerBatch, reopened.CurrentOffset);
        Assert.Equal(_written.SelectMany(b => b).ToArray(), await ReadAllAsync(reopened));
    }

    [Fact]
    public async Task ABatchCutShortByTheStopIsDroppedAndTheNextAppendTakesItsOffset()
    {
        await WriteSegmentAsync();
        var half = TestRecordBatch.Create(baseOffset: Batches * RecordsPerBatch, recordCount: RecordsPerBatch);
        using (var log = File.Open(LogPath, FileMode.Append))
            log.Write(half, 0, half.Length / 2);

        using var reopened = Reopen();
        Assert.Equal(Batches * RecordsPerBatch, reopened.CurrentOffset);
        Assert.Equal(_written.Sum(b => b.Length), reopened.Size);

        var next = TestRecordBatch.Create(baseOffset: Batches * RecordsPerBatch, recordCount: RecordsPerBatch);
        var (baseOffset, _) = await reopened.AppendAsync(next);
        Assert.Equal(Batches * RecordsPerBatch, baseOffset);
        Assert.Equal(_written.SelectMany(b => b).Concat(next).ToArray(), await ReadAllAsync(reopened));
    }

    private string LogPath => Path.Combine(_dir, $"{0:D20}.log");

    private string IndexPath => Path.Combine(_dir, $"{0:D20}.index");

    private async Task WriteSegmentAsync()
    {
        using var engine = new FileStorageEngine(_dir, baseOffset: 0, createNew: true);
        for (var i = 0; i < Batches; i++)
        {
            var batch = TestRecordBatch.Create(baseOffset: i * RecordsPerBatch, recordCount: RecordsPerBatch);
            _written.Add(batch);
            await engine.AppendAsync(batch);
        }
        await engine.FlushAsync();
    }

    private FileStorageEngine Reopen() => new(_dir, baseOffset: 0, createNew: false);

    private static async Task<byte[]> ReadAllAsync(FileStorageEngine engine)
    {
        using var lease = await engine.ReadAsync(0, 1024 * 1024);
        return lease.IsEmpty ? [] : lease.Data.Span.ToArray();
    }
}
