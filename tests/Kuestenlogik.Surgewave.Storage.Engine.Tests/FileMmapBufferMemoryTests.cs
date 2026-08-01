using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// <c>FileMmapBuffer.Memory</c> used to return <c>ToArray()</c>, which quietly pushed every
/// Memory consumer off the zero-copy path — the mapped region was read, then copied, defeating
/// the point of mapping it (#78). It now projects the region through a MemoryManager.
///
/// <para>Correct content alone would not prove anything (a copy has the same bytes), so these
/// tests check the property that distinguishes projection from copying: the memory must observe
/// the same address as the span, and repeated access must not allocate.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class FileMmapBufferMemoryTests : IDisposable
{
    private readonly string _tempDir;

    public FileMmapBufferMemoryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-mmapmem-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); } catch { }
        GC.SuppressFinalize(this);
    }

    private async Task<(FileStorageEngine Engine, byte[] Expected)> CreateEngineWithDataAsync()
    {
        var dir = Path.Combine(_tempDir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var engine = new FileStorageEngine(dir, baseOffset: 0, createNew: true, useMmap: true);

        var all = new List<byte>();
        for (int i = 0; i < 4; i++)
        {
            var batch = TestRecordBatch.Create(baseOffset: i * 2, recordCount: 2);
            all.AddRange(batch);
            await engine.AppendAsync(batch);
        }
        await engine.FlushAsync();
        return (engine, all.ToArray());
    }

    [Fact]
    public async Task Memory_HasTheSameContentAsSpan()
    {
        var (engine, expected) = await CreateEngineWithDataAsync();
        using (engine)
        {
            using var lease = await engine.ReadAsync(0, 1024 * 1024);
            Assert.False(lease.IsEmpty);

            Assert.Equal(expected, lease.Data.Span.ToArray());
            Assert.Equal(expected, lease.Data.Memory.ToArray());
        }
    }

    [Fact]
    public async Task Memory_ProjectsTheMappedRegion_InsteadOfCopyingIt()
    {
        var (engine, _) = await CreateEngineWithDataAsync();
        using (engine)
        {
            using var lease = await engine.ReadAsync(0, 1024 * 1024);
            var buffer = lease.Data;

            // A copy would live at a different address than the mapped span; a projection does not.
            ref readonly var spanStart = ref buffer.Span[0];
            ref readonly var memoryStart = ref buffer.Memory.Span[0];

            Assert.True(System.Runtime.CompilerServices.Unsafe.AreSame(
                in spanStart, in memoryStart),
                "Memory does not point at the mapped region — it is still copying");
        }
    }

    [Fact]
    public async Task Memory_DoesNotAllocateOnRepeatedAccess()
    {
        var (engine, _) = await CreateEngineWithDataAsync();
        using (engine)
        {
            using var lease = await engine.ReadAsync(0, 1024 * 1024);
            var buffer = lease.Data;

            _ = buffer.Memory; // first access may create the manager

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 100; i++)
            {
                _ = buffer.Memory;
            }
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            // The old implementation allocated a full payload-sized array per access.
            Assert.Equal(0, allocated);
        }
    }

    [Fact]
    public async Task TryGetMemory_SucceedsAndMatchesTheSpan()
    {
        var (engine, expected) = await CreateEngineWithDataAsync();
        using (engine)
        {
            using var lease = await engine.ReadAsync(0, 1024 * 1024);

            Assert.True(lease.Data.TryGetMemory(out var memory),
                "a mapped buffer can hand out borrowed memory, so this must not report failure");
            Assert.Equal(expected, memory.ToArray());
        }
    }

    [Fact]
    public async Task Memory_OfASlice_CoversOnlyTheSlicedRegion()
    {
        var (engine, expected) = await CreateEngineWithDataAsync();
        using (engine)
        {
            using var lease = await engine.ReadAsync(0, 1024 * 1024);
            using var slice = lease.Data.Slice(4, 16);

            Assert.Equal(16, slice.Memory.Length);
            Assert.Equal(expected.Skip(4).Take(16).ToArray(), slice.Memory.ToArray());
        }
    }
}
