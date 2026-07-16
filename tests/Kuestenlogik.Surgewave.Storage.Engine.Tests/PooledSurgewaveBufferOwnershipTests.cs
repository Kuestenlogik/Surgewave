using Kuestenlogik.Surgewave.Storage.Engine;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// #75 — trimming a pooled read must TRANSFER pool ownership to the trimmed view. A plain
/// Slice() is non-owning, and handing it to a lease while nothing disposes the parent leaked the
/// rented (typically LOH-sized) array on every trimmed read.
/// </summary>
public class PooledSurgewaveBufferOwnershipTests
{
    [Fact]
    public void SliceTransferringOwnership_ParentBecomesDisposed_ViewCarriesTheData()
    {
        var parent = new PooledSurgewaveBuffer(64);
        ((ISurgewaveWritableBuffer)parent).Span.Fill(0xAB);

        var view = parent.SliceTransferringOwnership(0, 16);

        // The view carries the trimmed data …
        Assert.Equal(16, view.Length);
        Assert.All(view.Span.ToArray(), b => Assert.Equal(0xAB, b));

        // … and the parent is now disposed: it must neither serve data nor return the array again.
        Assert.Throws<ObjectDisposedException>(() => { _ = parent.Span; });
        parent.Dispose(); // no-op, must not double-return the transferred array

        Assert.All(view.Span.ToArray(), b => Assert.Equal(0xAB, b)); // still valid after parent dispose
        view.Dispose(); // the view returns the rent exactly once
    }

    [Fact]
    public void SliceTransferringOwnership_DisposedParent_Throws()
    {
        var parent = new PooledSurgewaveBuffer(16);
        parent.Dispose();
        Assert.Throws<ObjectDisposedException>(() => parent.SliceTransferringOwnership(0, 8));
    }

    [Fact]
    public void PlainSlice_RemainsNonOwning_ParentStillServesData()
    {
        // The pre-existing Slice contract is unchanged: non-owning view, parent stays alive.
        var parent = new PooledSurgewaveBuffer(32);
        var slice = parent.Slice(0, 8);

        slice.Dispose(); // must NOT return the parent's array
        Assert.Equal(32, parent.Span.Length); // parent unaffected

        parent.Dispose();
    }
}
