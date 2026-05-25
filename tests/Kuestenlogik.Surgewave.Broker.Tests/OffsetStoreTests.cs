using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for the OffsetStore: commit, retrieve, persist, and delete offsets.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class OffsetStoreTests : IDisposable
{
    private readonly string _tempDir;
    private readonly OffsetStore _store;

    public OffsetStoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-offset-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        // Use a large flush interval to avoid background timer interfering with tests
        _store = new OffsetStore(_tempDir, NullLogger<OffsetStore>.Instance, flushIntervalMs: 60_000);
    }

    public void Dispose()
    {
        _store.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); }
        catch { /* ignore */ }
    }

    // ── CommitOffset / GetCommittedOffset ─────────────────────────────────────

    [Fact]
    public void GetCommittedOffset_NoOffset_ReturnsMinusOne()
    {
        var offset = _store.GetCommittedOffset("group-1", "topic-a", 0);
        Assert.Equal(-1, offset);
    }

    [Fact]
    public void CommitOffset_ThenGet_ReturnsCommittedOffset()
    {
        _store.CommitOffset("group-1", "topic-a", 0, offset: 42);

        var result = _store.GetCommittedOffset("group-1", "topic-a", 0);
        Assert.Equal(42, result);
    }

    [Fact]
    public void CommitOffset_Overwrites_PreviousOffset()
    {
        _store.CommitOffset("group-1", "topic-a", 0, offset: 10);
        _store.CommitOffset("group-1", "topic-a", 0, offset: 99);

        var result = _store.GetCommittedOffset("group-1", "topic-a", 0);
        Assert.Equal(99, result);
    }

    [Fact]
    public void CommitOffset_MultipleTopicsAndPartitions_AllTracked()
    {
        _store.CommitOffset("group-1", "topic-a", 0, 100);
        _store.CommitOffset("group-1", "topic-a", 1, 200);
        _store.CommitOffset("group-1", "topic-b", 0, 300);

        Assert.Equal(100, _store.GetCommittedOffset("group-1", "topic-a", 0));
        Assert.Equal(200, _store.GetCommittedOffset("group-1", "topic-a", 1));
        Assert.Equal(300, _store.GetCommittedOffset("group-1", "topic-b", 0));
    }

    [Fact]
    public void CommitOffset_DifferentGroups_AreIsolated()
    {
        _store.CommitOffset("group-1", "topic-a", 0, 50);
        _store.CommitOffset("group-2", "topic-a", 0, 999);

        Assert.Equal(50, _store.GetCommittedOffset("group-1", "topic-a", 0));
        Assert.Equal(999, _store.GetCommittedOffset("group-2", "topic-a", 0));
    }

    // ── GetAllOffsets ─────────────────────────────────────────────────────────

    [Fact]
    public void GetAllOffsets_ReturnsAllCommittedOffsets()
    {
        _store.CommitOffset("grp", "t", 0, 10);
        _store.CommitOffset("grp", "t", 1, 20);

        var all = _store.GetAllOffsets("grp");

        Assert.Equal(2, all.Count);
        Assert.Equal(10L, all["t:0"]);
        Assert.Equal(20L, all["t:1"]);
    }

    [Fact]
    public void GetAllOffsets_UnknownGroup_ReturnsEmpty()
    {
        var all = _store.GetAllOffsets("unknown-group");
        Assert.Empty(all);
    }

    [Fact]
    public void GetAllOffsets_ReturnsCopy_ModifyingDoesNotAffectStore()
    {
        _store.CommitOffset("grp", "t", 0, 42);
        var all = _store.GetAllOffsets("grp");

        // Mutate the returned dictionary
        all["t:0"] = 999;

        // Store value should be unchanged
        Assert.Equal(42, _store.GetCommittedOffset("grp", "t", 0));
    }

    // ── HasCommittedOffsets ────────────────────────────────────────────────────

    [Fact]
    public void HasCommittedOffsets_NoOffsets_ReturnsFalse()
    {
        Assert.False(_store.HasCommittedOffsets("group-1"));
    }

    [Fact]
    public void HasCommittedOffsets_AfterCommit_ReturnsTrue()
    {
        _store.CommitOffset("group-1", "topic", 0, 1);
        Assert.True(_store.HasCommittedOffsets("group-1"));
    }

    // ── DeleteOffset ──────────────────────────────────────────────────────────

    [Fact]
    public void DeleteOffset_RemovesSpecificPartition()
    {
        _store.CommitOffset("grp", "t", 0, 100);
        _store.CommitOffset("grp", "t", 1, 200);

        _store.DeleteOffset("grp", "t", 0);

        Assert.Equal(-1, _store.GetCommittedOffset("grp", "t", 0));
        Assert.Equal(200, _store.GetCommittedOffset("grp", "t", 1));
    }

    [Fact]
    public void DeleteOffset_UnknownGroup_IsNoOp()
    {
        // Should not throw
        _store.DeleteOffset("unknown-group", "t", 0);
    }

    [Fact]
    public void DeleteOffset_UnknownPartition_IsNoOp()
    {
        _store.CommitOffset("grp", "t", 0, 50);
        _store.DeleteOffset("grp", "t", 99); // partition 99 doesn't exist

        Assert.Equal(50, _store.GetCommittedOffset("grp", "t", 0));
    }

    // ── DeleteGroup ────────────────────────────────────────────────────────────

    [Fact]
    public void DeleteGroup_RemovesAllOffsets()
    {
        _store.CommitOffset("grp", "t", 0, 10);
        _store.CommitOffset("grp", "t", 1, 20);

        _store.DeleteGroup("grp");

        Assert.False(_store.HasCommittedOffsets("grp"));
        Assert.Equal(-1, _store.GetCommittedOffset("grp", "t", 0));
    }

    [Fact]
    public void DeleteGroup_UnknownGroup_IsNoOp()
    {
        _store.DeleteGroup("nonexistent-group");
    }

    [Fact]
    public void DeleteGroup_OtherGroupsUnaffected()
    {
        _store.CommitOffset("grp-a", "t", 0, 100);
        _store.CommitOffset("grp-b", "t", 0, 200);

        _store.DeleteGroup("grp-a");

        Assert.Equal(200, _store.GetCommittedOffset("grp-b", "t", 0));
    }

    // ── Flush / Persistence ────────────────────────────────────────────────────

    [Fact]
    public void Flush_WritesGroupFileToDisk()
    {
        _store.CommitOffset("grp", "t", 0, 42);
        _store.Flush();

        var groupsDir = Path.Combine(_tempDir, ".metadata", "groups");
        Assert.True(Directory.Exists(groupsDir));
        var files = Directory.GetFiles(groupsDir, "*.json");
        Assert.NotEmpty(files);
    }

    [Fact]
    public void Flush_AfterDelete_RemovesFileFromDisk()
    {
        _store.CommitOffset("grp", "t", 0, 42);
        _store.Flush();

        var groupsDir = Path.Combine(_tempDir, ".metadata", "groups");
        var filesBefore = Directory.GetFiles(groupsDir, "*.json").Length;
        Assert.True(filesBefore > 0);

        _store.DeleteGroup("grp");
        // After delete, file is removed directly (not debounced)
        var filesAfter = Directory.GetFiles(groupsDir, "*.json").Length;
        Assert.True(filesAfter < filesBefore);
    }

    [Fact]
    public void Persistence_LoadsBackOnRestart()
    {
        _store.CommitOffset("grp", "t", 0, 77);
        _store.Flush();

        // Create a new store pointing to the same directory (simulates restart)
        using var store2 = new OffsetStore(_tempDir, NullLogger<OffsetStore>.Instance, flushIntervalMs: 60_000);

        var offset = store2.GetCommittedOffset("grp", "t", 0);
        Assert.Equal(77, offset);
    }

    // ── Edge cases ─────────────────────────────────────────────────────────────

    [Fact]
    public void CommitOffset_PartitionZero_Works()
    {
        _store.CommitOffset("g", "t", 0, 0);
        Assert.Equal(0L, _store.GetCommittedOffset("g", "t", 0));
    }

    [Fact]
    public void CommitOffset_LargeOffset_Works()
    {
        _store.CommitOffset("g", "t", 0, long.MaxValue);
        Assert.Equal(long.MaxValue, _store.GetCommittedOffset("g", "t", 0));
    }

    [Fact]
    public void CommitOffset_TopicWithColon_HandledCorrectly()
    {
        // Topics can contain colons — make sure the key parsing handles it
        _store.CommitOffset("grp", "my-topic:v2", 3, 55);
        _store.Flush();

        // Reload from disk and verify
        using var store2 = new OffsetStore(_tempDir, NullLogger<OffsetStore>.Instance, flushIntervalMs: 60_000);
        var offset = store2.GetCommittedOffset("grp", "my-topic:v2", 3);
        Assert.Equal(55, offset);
    }

    [Fact]
    public void MultipleGroups_IndependentlyPersisted()
    {
        _store.CommitOffset("grp-a", "t", 0, 11);
        _store.CommitOffset("grp-b", "t", 0, 22);
        _store.CommitOffset("grp-c", "t", 0, 33);
        _store.Flush();

        using var store2 = new OffsetStore(_tempDir, NullLogger<OffsetStore>.Instance, flushIntervalMs: 60_000);

        Assert.Equal(11, store2.GetCommittedOffset("grp-a", "t", 0));
        Assert.Equal(22, store2.GetCommittedOffset("grp-b", "t", 0));
        Assert.Equal(33, store2.GetCommittedOffset("grp-c", "t", 0));
    }
}
