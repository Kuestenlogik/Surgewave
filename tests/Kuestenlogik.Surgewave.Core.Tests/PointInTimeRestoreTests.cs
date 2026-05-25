using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Backup;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for point-in-time restore (G14). These exercise the segment-boundary
/// PIT contract directly: a segment whose <c>BaseOffset &gt; cutoff</c> or
/// whose <c>MaxTimestampMs &gt; cutoff</c> is skipped; a segment with
/// <c>MaxTimestampMs == 0</c> always restores (we have no time-index data to
/// reason about it).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PointInTimeRestoreTests : IDisposable
{
    private readonly string _tempDir;

    public PointInTimeRestoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-pit-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public void ShouldRestoreSegment_NoCutoffs_AlwaysIncludes()
    {
        var seg = new BackupSegmentInfo { BaseOffset = 0, MaxTimestampMs = 1_000_000 };
        Assert.True(RestoreServiceProbe.ShouldRestoreSegment(seg, offsetCutoff: null, timestampCutoffMs: null));
    }

    [Fact]
    public void ShouldRestoreSegment_OffsetCutoff_SkipsSegmentsBeyond()
    {
        var keep = new BackupSegmentInfo { BaseOffset = 500 };
        var atBoundary = new BackupSegmentInfo { BaseOffset = 1000 };
        var skip = new BackupSegmentInfo { BaseOffset = 1001 };

        Assert.True(RestoreServiceProbe.ShouldRestoreSegment(keep, offsetCutoff: 1000, timestampCutoffMs: null));
        Assert.True(RestoreServiceProbe.ShouldRestoreSegment(atBoundary, offsetCutoff: 1000, timestampCutoffMs: null));
        Assert.False(RestoreServiceProbe.ShouldRestoreSegment(skip, offsetCutoff: 1000, timestampCutoffMs: null));
    }

    [Fact]
    public void ShouldRestoreSegment_TimestampCutoff_SkipsSegmentsAfter()
    {
        var keep = new BackupSegmentInfo { BaseOffset = 0, MaxTimestampMs = 1_000_000 };
        var atBoundary = new BackupSegmentInfo { BaseOffset = 100, MaxTimestampMs = 2_000_000 };
        var skip = new BackupSegmentInfo { BaseOffset = 200, MaxTimestampMs = 2_000_001 };

        Assert.True(RestoreServiceProbe.ShouldRestoreSegment(keep, offsetCutoff: null, timestampCutoffMs: 2_000_000));
        Assert.True(RestoreServiceProbe.ShouldRestoreSegment(atBoundary, offsetCutoff: null, timestampCutoffMs: 2_000_000));
        Assert.False(RestoreServiceProbe.ShouldRestoreSegment(skip, offsetCutoff: null, timestampCutoffMs: 2_000_000));
    }

    [Fact]
    public void ShouldRestoreSegment_MaxTimestampZero_AlwaysIncludesEvenWithCutoff()
    {
        // A segment with no time-index entries has MaxTimestampMs=0. Excluding
        // it would silently drop its data; the right thing is to include it.
        var seg = new BackupSegmentInfo { BaseOffset = 0, MaxTimestampMs = 0 };
        Assert.True(RestoreServiceProbe.ShouldRestoreSegment(seg, offsetCutoff: null, timestampCutoffMs: 500));
    }

    [Fact]
    public void ShouldRestoreSegment_BothCutoffs_AnyOneSkips()
    {
        // Either cutoff alone keeps it, both together skip it via the offset side.
        var seg = new BackupSegmentInfo { BaseOffset = 1500, MaxTimestampMs = 999 };
        Assert.False(RestoreServiceProbe.ShouldRestoreSegment(seg, offsetCutoff: 1000, timestampCutoffMs: 2000));
    }

    [Fact]
    public async Task RestoreAsync_OffsetCutoff_SkipsSegmentsBeyondCutoff()
    {
        // Build a fake backup with three segments at offsets 0, 100, 200 for one partition.
        // Cutoff = 100 → expect segments 0 and 100 to land in destination, segment 200 skipped.
        var backupRoot = Path.Combine(_tempDir, "backup");
        var dataDest = Path.Combine(_tempDir, "restored");
        Directory.CreateDirectory(backupRoot);

        var partitionDir = Path.Combine(backupRoot, "data", "orders", "partition-0");
        Directory.CreateDirectory(partitionDir);

        var segments = new[] { 0L, 100L, 200L };
        foreach (var baseOffset in segments)
        {
            await CreateFakeSegment(partitionDir, baseOffset, payloadSize: 64);
        }

        var manifest = new BackupManifest
        {
            CreatedAt = DateTimeOffset.UtcNow,
            Topics =
            {
                new BackupTopicInfo
                {
                    Name = "orders",
                    PartitionCount = 1,
                    Partitions =
                    {
                        new BackupPartitionInfo
                        {
                            PartitionId = 0,
                            LogStartOffset = 0,
                            HighWatermark = 200,
                            SegmentCount = 3,
                            Segments = segments.Select(o => new BackupSegmentInfo
                            {
                                BaseOffset = o,
                                LogFile = $"{o:D20}.log",
                                IndexFile = $"{o:D20}.index",
                                TimeIndexFile = $"{o:D20}.timeindex",
                                LogSize = 64,
                                MaxTimestampMs = 1_000 * o + 5_000,
                            }).ToList(),
                        }
                    }
                }
            }
        };
        await manifest.SaveAsync(Path.Combine(backupRoot, BackupManifest.FileName));

        var service = new RestoreService();
        var result = await service.RestoreAsync(
            backupRoot,
            dataDest,
            new RestoreOptions
            {
                VerifyChecksums = false,
                Overwrite = true,
                TargetOffsetsPerPartition = new Dictionary<string, long>
                {
                    [RestoreOptions.PartitionKey("orders", 0)] = 100,
                },
            });

        Assert.True(result.Success);
        Assert.Equal(1, result.TopicsRestored);
        Assert.Equal(1, result.SegmentsSkipped);

        var destPartition = Path.Combine(dataDest, "orders", "partition-0");
        Assert.True(File.Exists(Path.Combine(destPartition, "00000000000000000000.log")));
        Assert.True(File.Exists(Path.Combine(destPartition, "00000000000000000100.log")));
        Assert.False(File.Exists(Path.Combine(destPartition, "00000000000000000200.log")));
    }

    [Fact]
    public async Task RestoreAsync_TimestampCutoff_SkipsRecentSegments()
    {
        var backupRoot = Path.Combine(_tempDir, "backup");
        var dataDest = Path.Combine(_tempDir, "restored");
        Directory.CreateDirectory(backupRoot);

        var partitionDir = Path.Combine(backupRoot, "data", "events", "partition-0");
        Directory.CreateDirectory(partitionDir);

        var segments = new[]
        {
            (BaseOffset: 0L,  TimestampMs: 1_000L),
            (BaseOffset: 100L, TimestampMs: 5_000L),
            (BaseOffset: 200L, TimestampMs: 9_999L),
        };
        foreach (var (baseOffset, _) in segments)
        {
            await CreateFakeSegment(partitionDir, baseOffset, payloadSize: 32);
        }

        var manifest = new BackupManifest
        {
            CreatedAt = DateTimeOffset.UtcNow,
            Topics =
            {
                new BackupTopicInfo
                {
                    Name = "events",
                    PartitionCount = 1,
                    Partitions =
                    {
                        new BackupPartitionInfo
                        {
                            PartitionId = 0,
                            SegmentCount = segments.Length,
                            Segments = segments.Select(s => new BackupSegmentInfo
                            {
                                BaseOffset = s.BaseOffset,
                                LogFile = $"{s.BaseOffset:D20}.log",
                                IndexFile = $"{s.BaseOffset:D20}.index",
                                TimeIndexFile = $"{s.BaseOffset:D20}.timeindex",
                                LogSize = 32,
                                MaxTimestampMs = s.TimestampMs,
                            }).ToList(),
                        }
                    }
                }
            }
        };
        await manifest.SaveAsync(Path.Combine(backupRoot, BackupManifest.FileName));

        var service = new RestoreService();
        var result = await service.RestoreAsync(
            backupRoot,
            dataDest,
            new RestoreOptions
            {
                VerifyChecksums = false,
                Overwrite = true,
                TargetTimestampMs = 5_000,
            });

        Assert.True(result.Success);
        Assert.Equal(1, result.SegmentsSkipped);

        var destPartition = Path.Combine(dataDest, "events", "partition-0");
        Assert.True(File.Exists(Path.Combine(destPartition, "00000000000000000000.log")));
        Assert.True(File.Exists(Path.Combine(destPartition, "00000000000000000100.log")));
        Assert.False(File.Exists(Path.Combine(destPartition, "00000000000000000200.log")));
    }

    private static async Task CreateFakeSegment(string dir, long baseOffset, int payloadSize)
    {
        var stem = baseOffset.ToString("D20");
        // .log payload — content is irrelevant for this test, only file presence
        // and size matter.
        var payload = new byte[payloadSize];
        BinaryPrimitives.WriteInt64BigEndian(payload, baseOffset);
        await File.WriteAllBytesAsync(Path.Combine(dir, $"{stem}.log"), payload);
        await File.WriteAllBytesAsync(Path.Combine(dir, $"{stem}.index"), new byte[16]);
        await File.WriteAllBytesAsync(Path.Combine(dir, $"{stem}.timeindex"), new byte[12]);
    }

    /// <summary>
    /// Reflection probe over the internal <c>RestoreService.ShouldRestoreSegment</c>
    /// helper so tests can call it without weakening the production access modifier.
    /// </summary>
    private static class RestoreServiceProbe
    {
        private static readonly System.Reflection.MethodInfo Method = typeof(RestoreService)
            .GetMethod("ShouldRestoreSegment",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
            ?? throw new InvalidOperationException("RestoreService.ShouldRestoreSegment not found");

        public static bool ShouldRestoreSegment(BackupSegmentInfo segment, long? offsetCutoff, long? timestampCutoffMs)
            => (bool)Method.Invoke(null, [segment, offsetCutoff, timestampCutoffMs])!;
    }
}
