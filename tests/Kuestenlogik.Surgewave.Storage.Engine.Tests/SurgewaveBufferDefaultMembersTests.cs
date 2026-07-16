using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Pins the default interface members of <see cref="ISurgewaveBuffer"/> — IsEmpty and the
/// single-argument Slice overload — as executed through implementations that do not
/// override them: the suffix slice shares the parent's data, an empty suffix at Length is
/// legal, and a start offset past the end fails fast.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SurgewaveBufferDefaultMembersTests
{
    [Fact]
    public void IsEmpty_TrueForEmptySingleton_FalseForNonEmptyBuffer()
    {
        ISurgewaveBuffer empty = DefaultSurgewaveBufferPool.Shared.Empty;
        Assert.True(empty.IsEmpty);

        byte[] data = [1, 2, 3];
        using var buffer = DefaultSurgewaveBufferPool.Shared.Wrap(data);
        Assert.False(buffer.IsEmpty);
    }

    [Fact]
    public void SliceToEnd_ReturnsSuffixSharingUnderlyingData()
    {
        byte[] data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        using var buffer = DefaultSurgewaveBufferPool.Shared.Wrap(data);

        using var suffix = buffer.Slice(4);

        byte[] expected = [4, 5, 6, 7, 8, 9];
        Assert.Equal(6, suffix.Length);
        Assert.Equal(expected, suffix.ToArray());
    }

    [Fact]
    public void SliceToEnd_AtZero_CoversWholeBuffer()
    {
        byte[] data = [10, 20, 30];
        using var buffer = DefaultSurgewaveBufferPool.Shared.Wrap(data);

        using var whole = buffer.Slice(0);

        Assert.Equal(data, whole.ToArray());
    }

    [Fact]
    public void SliceToEnd_AtLength_ReturnsEmptyBuffer()
    {
        byte[] data = [1, 2, 3, 4];
        using var buffer = DefaultSurgewaveBufferPool.Shared.Wrap(data);

        using var tail = buffer.Slice(4);

        Assert.Equal(0, tail.Length);
        Assert.True(tail.IsEmpty);
    }

    [Fact]
    public void SliceToEnd_StartPastEnd_Throws()
    {
        byte[] data = [1, 2];
        using var buffer = DefaultSurgewaveBufferPool.Shared.Wrap(data);

        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.Slice(3));
    }

    [Fact]
    public void SliceToEnd_OnPooledBuffer_SeesWritesMadeThroughParent()
    {
        using ISurgewaveWritableBuffer parent = new PooledSurgewaveBuffer(6);
        parent.Span.Clear();
        parent.Span[5] = 0xAB;

        using var suffix = ((ISurgewaveBuffer)parent).Slice(5);

        Assert.Equal(1, suffix.Length);
        Assert.Equal(0xAB, suffix.Span[0]);
    }
}
