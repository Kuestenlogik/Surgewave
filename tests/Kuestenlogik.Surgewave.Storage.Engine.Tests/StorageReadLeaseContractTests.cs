using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Pins the <see cref="IStorageReadLease"/> contract: the BatchCount/IsEmpty default
/// interface members derive from BatchOffsets/Data, GetBatch slices exact batch
/// boundaries (the last batch runs to the end of Data), and disposing the lease releases
/// the underlying buffer exactly once.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class StorageReadLeaseContractTests
{
    [Fact]
    public void LeaseWithTwoBatches_ExposesCountAndSlicesExactBoundaries()
    {
        byte[] data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        using IStorageReadLease lease = new StorageReadLease(
            DefaultSurgewaveBufferPool.Shared.RentAndCopy(data),
            [0, 8]);

        Assert.Equal(2, lease.BatchCount);
        Assert.False(lease.IsEmpty);

        using var first = lease.GetBatch(0);
        Assert.Equal(data[..8], first.ToArray());

        using var second = lease.GetBatch(1);
        Assert.Equal(data[8..], second.ToArray());
    }

    [Fact]
    public void GetBatch_IndexOutOfRange_Throws()
    {
        byte[] data = [1, 2, 3];
        using IStorageReadLease lease = new StorageReadLease(
            DefaultSurgewaveBufferPool.Shared.RentAndCopy(data),
            [0]);

        Assert.Throws<ArgumentOutOfRangeException>(() => lease.GetBatch(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => lease.GetBatch(-1));
    }

    [Fact]
    public void Dispose_ReleasesUnderlyingBuffer_AndFurtherAccessThrows()
    {
        var data = new PooledSurgewaveBuffer(4);
        IStorageReadLease lease = new StorageReadLease(data, [0]);

        lease.Dispose();

        Assert.Throws<ObjectDisposedException>(() => { _ = lease.Data; });
        Assert.Throws<ObjectDisposedException>(() => lease.GetBatch(0));
        Assert.Throws<ObjectDisposedException>(() => { _ = data.Span; });

        lease.Dispose(); // idempotent — must not double-return the pooled array
    }

    [Fact]
    public void EmptyLease_HasNoBatches_AndIsEmpty()
    {
        IStorageReadLease lease = EmptyStorageReadLease.Instance;

        Assert.Equal(0, lease.BatchCount);
        Assert.True(lease.IsEmpty);
        Assert.Throws<ArgumentOutOfRangeException>(() => lease.GetBatch(0));

        lease.Dispose(); // no-op for the singleton
    }
}
