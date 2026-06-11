using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Disaggregated;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Tests;

public sealed class InMemoryPartitionManifestStoreTests
{
    private static readonly TopicPartition P0 = new() { Topic = "orders", Partition = 0 };
    private static readonly TopicPartition P1 = new() { Topic = "orders", Partition = 1 };

    [Fact]
    public async Task GetAsync_returns_empty_manifest_for_unknown_partition()
    {
        var store = new InMemoryPartitionManifestStore();
        var manifest = await store.GetAsync(P0);

        Assert.Equal(0, manifest.Version);
        Assert.Empty(manifest.Objects);
        Assert.Equal(P0, manifest.Partition);
    }

    [Fact]
    public async Task AppendObjectAsync_round_trips_through_GetAsync()
    {
        var store = new InMemoryPartitionManifestStore();
        var newRef = new StreamObjectRef("orders/0/000.so", 0, 99, 1024, DateTime.UtcNow);

        var appended = await store.AppendObjectAsync(P0, newRef);
        var fetched = await store.GetAsync(P0);

        Assert.Equal(1, appended.Version);
        Assert.Equal(appended.Version, fetched.Version);
        Assert.Equal("orders/0/000.so", fetched.Objects[0].ObjectKey);
    }

    [Fact]
    public async Task AppendObjectAsync_rejects_overlapping_ranges()
    {
        var store = new InMemoryPartitionManifestStore();
        await store.AppendObjectAsync(P0, new StreamObjectRef("a", 0, 99, 1024, DateTime.UtcNow));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await store.AppendObjectAsync(P0, new StreamObjectRef("b", 50, 149, 1024, DateTime.UtcNow)));
    }

    [Fact]
    public async Task ListPartitionsAsync_returns_only_partitions_with_appends()
    {
        var store = new InMemoryPartitionManifestStore();
        await store.AppendObjectAsync(P0, new StreamObjectRef("a", 0, 99, 1024, DateTime.UtcNow));

        var partitions = await store.ListPartitionsAsync();

        Assert.Single(partitions);
        Assert.Equal(P0, partitions[0]);
    }

    [Fact]
    public async Task Concurrent_appends_to_different_partitions_dont_serialise()
    {
        var store = new InMemoryPartitionManifestStore();

        // Both calls fire in parallel; they target different partitions, so
        // the per-partition gate should not serialise them. We assert via
        // result correctness, not timing.
        var t0 = store.AppendObjectAsync(P0, new StreamObjectRef("p0", 0, 99, 1024, DateTime.UtcNow));
        var t1 = store.AppendObjectAsync(P1, new StreamObjectRef("p1", 0, 99, 1024, DateTime.UtcNow));
        await Task.WhenAll(t0.AsTask(), t1.AsTask());

        Assert.Equal(2, (await store.ListPartitionsAsync()).Count);
    }
}
