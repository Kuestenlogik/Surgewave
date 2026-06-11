using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Disaggregated;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Tests;

/// <summary>
/// File-backed store tests use a per-test temp dir so suites can run
/// in parallel without colliding on disk.
/// </summary>
public sealed class FilePartitionManifestStoreTests : IDisposable
{
    private readonly string _tempDir;

    public FilePartitionManifestStoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "swv-mfst-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly TopicPartition P0 = new() { Topic = "orders", Partition = 0 };

    [Fact]
    public async Task Append_writes_json_file_at_expected_path()
    {
        var store = new FilePartitionManifestStore(_tempDir);
        await store.AppendObjectAsync(P0, new StreamObjectRef("k", 0, 99, 1024, DateTime.UtcNow));

        var expected = Path.Combine(_tempDir, "disaggregated", "manifests", "orders__0.json");
        Assert.True(File.Exists(expected));
    }

    [Fact]
    public async Task Manifest_survives_store_recreation()
    {
        var s1 = new FilePartitionManifestStore(_tempDir);
        var first = new StreamObjectRef("k0", 0, 99, 1024, new DateTime(2026, 6, 11, 0, 0, 0, DateTimeKind.Utc));
        var second = new StreamObjectRef("k1", 100, 199, 1024, new DateTime(2026, 6, 11, 0, 1, 0, DateTimeKind.Utc));
        await s1.AppendObjectAsync(P0, first);
        await s1.AppendObjectAsync(P0, second);

        // Brand-new store instance — must re-hydrate from disk on first call.
        var s2 = new FilePartitionManifestStore(_tempDir);
        var reloaded = await s2.GetAsync(P0);

        Assert.Equal(2, reloaded.Version);
        Assert.Equal("k0", reloaded.Objects[0].ObjectKey);
        Assert.Equal("k1", reloaded.Objects[1].ObjectKey);
        Assert.Equal(0, reloaded.FirstOffset);
        Assert.Equal(199, reloaded.LastOffset);
    }

    [Fact]
    public async Task No_temp_file_remains_after_successful_append()
    {
        var store = new FilePartitionManifestStore(_tempDir);
        await store.AppendObjectAsync(P0, new StreamObjectRef("k", 0, 99, 1024, DateTime.UtcNow));

        var manifestDir = Path.Combine(_tempDir, "disaggregated", "manifests");
        var lingering = Directory.GetFiles(manifestDir, "*.tmp");
        Assert.Empty(lingering);
    }

    [Fact]
    public async Task ListPartitions_returns_partitions_loaded_from_disk()
    {
        var s1 = new FilePartitionManifestStore(_tempDir);
        await s1.AppendObjectAsync(P0, new StreamObjectRef("k", 0, 99, 1024, DateTime.UtcNow));
        await s1.AppendObjectAsync(new TopicPartition { Topic = "orders", Partition = 1 },
            new StreamObjectRef("k", 0, 99, 1024, DateTime.UtcNow));

        var s2 = new FilePartitionManifestStore(_tempDir);
        var partitions = await s2.ListPartitionsAsync();

        Assert.Equal(2, partitions.Count);
    }

    [Fact]
    public void FileNameFor_uses_double_underscore_separator()
    {
        var name = FilePartitionManifestStore.FileNameFor(new TopicPartition { Topic = "my.topic", Partition = 7 });
        Assert.Equal("my.topic__7.json", name);
    }
}
