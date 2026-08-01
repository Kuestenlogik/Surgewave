using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Win32.SafeHandles;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Storage;

/// <summary>
/// <see cref="DataSource"/> is the seam a kernel-side send would attach to (#81): it describes
/// either a memory slice or a file region. File-backed sources used to drop their length on
/// construction and report 0 bytes, which would have silently sent nothing.
/// </summary>
public class DataSourceTests : IDisposable
{
    private readonly string _tempFile;
    private readonly SafeFileHandle _handle;

    public DataSourceTests()
    {
        _tempFile = Path.Combine(Path.GetTempPath(), $"surgewave-datasource-{Guid.NewGuid():N}.bin");
        File.WriteAllBytes(_tempFile, new byte[512]);
        _handle = File.OpenHandle(_tempFile);
    }

    public void Dispose()
    {
        _handle.Dispose();
        try { File.Delete(_tempFile); } catch { }
        GC.SuppressFinalize(this);
    }

    [Fact]
    public void FromMemory_ReportsTheSliceLength()
    {
        var source = DataSource.FromMemory(new byte[64]);

        Assert.True(source.IsMemoryBacked);
        Assert.Equal(64, source.Length);
        Assert.Equal(64, source.Memory.Length);
    }

    [Fact]
    public void FromFile_ReportsTheRegionLength_NotZero()
    {
        var source = DataSource.FromFile(_handle, position: 128, length: 256);

        Assert.False(source.IsMemoryBacked);
        Assert.Equal(128, source.FilePosition);
        Assert.Equal(256, source.Length);
        Assert.Same(_handle, source.FileHandle);
    }

    [Fact]
    public void Empty_IsMemoryBackedAndZeroLength()
    {
        var source = DataSource.Empty;

        Assert.True(source.IsMemoryBacked);
        Assert.Equal(0, source.Length);
        Assert.True(source.Memory.IsEmpty);
    }

    [Fact]
    public void FromFile_RejectsNonsensicalRegions()
    {
        Assert.Throws<ArgumentNullException>(() => DataSource.FromFile(null!, 0, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => DataSource.FromFile(_handle, -1, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => DataSource.FromFile(_handle, 0, -1));
    }

    [Fact]
    public void FromFile_ZeroLengthRegionIsAllowed()
    {
        var source = DataSource.FromFile(_handle, position: 0, length: 0);

        Assert.False(source.IsMemoryBacked);
        Assert.Equal(0, source.Length);
    }
}
