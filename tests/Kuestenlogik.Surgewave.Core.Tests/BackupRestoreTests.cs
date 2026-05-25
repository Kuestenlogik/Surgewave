using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Backup;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Unit tests for backup and restore functionality.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class BackupRestoreTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _tempDir;

    public BackupRestoreTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-backup-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    #region BackupManifest Tests

    [Fact]
    public void BackupManifest_Serialization_RoundTrip()
    {
        // Arrange
        var manifest = new BackupManifest
        {
            BackupId = "test-backup-id",
            Description = "Test backup",
            TotalFiles = 10,
            TotalBytes = 1024 * 1024
        };
        manifest.Topics.Add(new BackupTopicInfo
        {
            Name = "test-topic",
            PartitionCount = 3,
            TotalBytes = 512 * 1024
        });
        manifest.FileChecksums["file1.log"] = "ABC123";

        // Act
        var json = manifest.ToJson();
        var deserialized = BackupManifest.FromJson(json);

        // Assert
        Assert.Equal(manifest.BackupId, deserialized.BackupId);
        Assert.Equal(manifest.Description, deserialized.Description);
        Assert.Equal(manifest.TotalFiles, deserialized.TotalFiles);
        Assert.Equal(manifest.TotalBytes, deserialized.TotalBytes);
        Assert.Single(deserialized.Topics);
        Assert.Equal("test-topic", deserialized.Topics[0].Name);
        Assert.Equal(3, deserialized.Topics[0].PartitionCount);
        Assert.Single(deserialized.FileChecksums);
        Assert.Equal("ABC123", deserialized.FileChecksums["file1.log"]);

        _output.WriteLine($"Serialized manifest:\n{json}");
    }

    [Fact]
    public async Task BackupManifest_SaveAndLoad_RoundTrip()
    {
        // Arrange
        var manifest = new BackupManifest
        {
            BackupId = "save-load-test",
            TotalFiles = 5,
            TotalBytes = 2048
        };
        var path = Path.Combine(_tempDir, "test-manifest.json");

        // Act
        await manifest.SaveAsync(path);
        var loaded = await BackupManifest.LoadAsync(path);

        // Assert
        Assert.Equal(manifest.BackupId, loaded.BackupId);
        Assert.Equal(manifest.TotalFiles, loaded.TotalFiles);
        Assert.Equal(manifest.TotalBytes, loaded.TotalBytes);
    }

    [Fact]
    public void BackupManifest_DefaultValues_AreCorrect()
    {
        // Act
        var manifest = new BackupManifest();

        // Assert
        Assert.Equal(1, manifest.Version);
        Assert.NotEmpty(manifest.BackupId);
        Assert.True(manifest.IncludesMetadata);
        Assert.False(manifest.Verified);
        Assert.Empty(manifest.Topics);
        Assert.Empty(manifest.FileChecksums);
    }

    #endregion

    #region BackupService Tests

    [Fact]
    public async Task BackupService_EmptyDataDirectory_ReturnsEmptyBackup()
    {
        // Arrange
        var dataDir = Path.Combine(_tempDir, "empty-data");
        Directory.CreateDirectory(dataDir);
        var outputDir = Path.Combine(_tempDir, "empty-backup");

        var service = new BackupService(dataDir);

        // Act
        var manifest = await service.CreateBackupAsync(outputDir);

        // Assert
        Assert.Empty(manifest.Topics);
        Assert.Equal(0, manifest.TotalBytes);
        Assert.True(File.Exists(Path.Combine(outputDir, BackupManifest.FileName)));

        _output.WriteLine($"Backup ID: {manifest.BackupId}");
    }

    [Fact]
    public async Task BackupService_WithTopics_BacksUpAllTopics()
    {
        // Arrange
        var dataDir = Path.Combine(_tempDir, "data-with-topics");
        CreateTestTopicStructure(dataDir, "topic-1", 2);
        CreateTestTopicStructure(dataDir, "topic-2", 1);
        var outputDir = Path.Combine(_tempDir, "topics-backup");

        var service = new BackupService(dataDir);

        // Act
        var manifest = await service.CreateBackupAsync(outputDir);

        // Assert
        Assert.Equal(2, manifest.Topics.Count);
        Assert.Contains(manifest.Topics, t => t.Name == "topic-1");
        Assert.Contains(manifest.Topics, t => t.Name == "topic-2");
        Assert.True(manifest.TotalBytes > 0);

        // Verify files were copied
        Assert.True(Directory.Exists(Path.Combine(outputDir, "data", "topic-1", "partition-0")));
        Assert.True(Directory.Exists(Path.Combine(outputDir, "data", "topic-1", "partition-1")));
        Assert.True(Directory.Exists(Path.Combine(outputDir, "data", "topic-2", "partition-0")));

        _output.WriteLine($"Backed up {manifest.Topics.Count} topics, {manifest.TotalBytes} bytes");
    }

    [Fact]
    public async Task BackupService_WithTopicFilter_BacksUpOnlySelectedTopics()
    {
        // Arrange
        var dataDir = Path.Combine(_tempDir, "data-with-filter");
        CreateTestTopicStructure(dataDir, "include-topic", 1);
        CreateTestTopicStructure(dataDir, "exclude-topic", 1);
        var outputDir = Path.Combine(_tempDir, "filtered-backup");

        var service = new BackupService(dataDir);

        // Act
        var manifest = await service.CreateBackupAsync(outputDir, topics: ["include-topic"]);

        // Assert
        Assert.Single(manifest.Topics);
        Assert.Equal("include-topic", manifest.Topics[0].Name);
        Assert.False(Directory.Exists(Path.Combine(outputDir, "data", "exclude-topic")));
    }

    [Fact]
    public async Task BackupService_WithChecksums_ComputesHashes()
    {
        // Arrange
        var dataDir = Path.Combine(_tempDir, "data-checksums");
        CreateTestTopicStructure(dataDir, "checksum-topic", 1);
        var outputDir = Path.Combine(_tempDir, "checksums-backup");

        var service = new BackupService(dataDir);

        // Act
        var manifest = await service.CreateBackupAsync(outputDir, computeChecksums: true);

        // Assert
        Assert.NotEmpty(manifest.FileChecksums);
        foreach (var (path, hash) in manifest.FileChecksums)
        {
            Assert.NotEmpty(hash);
            Assert.Equal(64, hash.Length); // SHA256 produces 64 hex characters
            _output.WriteLine($"{path}: {hash}");
        }
    }

    [Fact]
    public async Task BackupService_ProgressEvent_IsFired()
    {
        // Arrange
        var dataDir = Path.Combine(_tempDir, "data-progress");
        CreateTestTopicStructure(dataDir, "progress-topic-1", 1);
        CreateTestTopicStructure(dataDir, "progress-topic-2", 1);
        var outputDir = Path.Combine(_tempDir, "progress-backup");

        var service = new BackupService(dataDir);
        var progressEvents = new List<BackupProgress>();
        service.ProgressChanged += (_, e) => progressEvents.Add(e.Progress);

        // Act
        await service.CreateBackupAsync(outputDir);

        // Assert
        Assert.True(progressEvents.Count >= 2);
        Assert.Equal(2, progressEvents.Max(p => p.TotalTopics));
    }

    #endregion

    #region RestoreService Tests

    [Fact]
    public async Task RestoreService_ValidBackup_RestoresSuccessfully()
    {
        // Arrange - Create a backup first
        var dataDir = Path.Combine(_tempDir, "restore-source");
        CreateTestTopicStructure(dataDir, "restore-topic", 2);
        CreateTestMetadata(dataDir);
        var backupDir = Path.Combine(_tempDir, "restore-backup");

        var backupService = new BackupService(dataDir);
        await backupService.CreateBackupAsync(backupDir);

        // Act - Restore to new directory
        var restoreDir = Path.Combine(_tempDir, "restore-target");
        var restoreService = new RestoreService();
        var result = await restoreService.RestoreAsync(backupDir, restoreDir);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(1, result.TopicsRestored);
        Assert.True(result.FilesRestored > 0);
        Assert.True(result.BytesRestored > 0);
        Assert.True(Directory.Exists(Path.Combine(restoreDir, "restore-topic", "partition-0")));
        Assert.True(Directory.Exists(Path.Combine(restoreDir, "restore-topic", "partition-1")));

        _output.WriteLine($"Restored {result.TopicsRestored} topics, {result.FilesRestored} files, {result.BytesRestored} bytes");
    }

    [Fact]
    public async Task RestoreService_MissingManifest_ReturnsFalse()
    {
        // Arrange
        var emptyDir = Path.Combine(_tempDir, "no-manifest");
        Directory.CreateDirectory(emptyDir);
        var restoreDir = Path.Combine(_tempDir, "restore-target-fail");

        var service = new RestoreService();

        // Act
        var result = await service.RestoreAsync(emptyDir, restoreDir);

        // Assert
        Assert.False(result.Success);
        Assert.Contains("Manifest not found", result.ErrorMessage);
    }

    [Fact]
    public async Task RestoreService_WithTopicFilter_RestoresOnlySelectedTopics()
    {
        // Arrange
        var dataDir = Path.Combine(_tempDir, "filter-restore-source");
        CreateTestTopicStructure(dataDir, "topic-a", 1);
        CreateTestTopicStructure(dataDir, "topic-b", 1);
        var backupDir = Path.Combine(_tempDir, "filter-restore-backup");

        var backupService = new BackupService(dataDir);
        await backupService.CreateBackupAsync(backupDir);

        // Act
        var restoreDir = Path.Combine(_tempDir, "filter-restore-target");
        var restoreService = new RestoreService();
        var result = await restoreService.RestoreAsync(backupDir, restoreDir, topics: ["topic-a"]);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(1, result.TopicsRestored);
        Assert.True(Directory.Exists(Path.Combine(restoreDir, "topic-a")));
        Assert.False(Directory.Exists(Path.Combine(restoreDir, "topic-b")));
    }

    #endregion

    #region VerifyResult Tests

    [Fact]
    public async Task RestoreService_VerifyBackup_ValidBackup_ReturnsTrue()
    {
        // Arrange
        var dataDir = Path.Combine(_tempDir, "verify-source");
        CreateTestTopicStructure(dataDir, "verify-topic", 1);
        var backupDir = Path.Combine(_tempDir, "verify-backup");

        var backupService = new BackupService(dataDir);
        await backupService.CreateBackupAsync(backupDir, computeChecksums: true);

        // Act
        var restoreService = new RestoreService();
        var result = await restoreService.VerifyAsync(backupDir);

        // Assert
        Assert.True(result.IsValid);
        Assert.Empty(result.MissingFiles);
        Assert.Empty(result.CorruptedFiles);
        Assert.True(result.FilesVerified > 0);

        _output.WriteLine($"Verified {result.FilesVerified} files, {result.BytesVerified} bytes");
    }

    [Fact]
    public async Task RestoreService_VerifyBackup_CorruptedFile_ReturnsFalse()
    {
        // Arrange
        var dataDir = Path.Combine(_tempDir, "corrupt-verify-source");
        CreateTestTopicStructure(dataDir, "corrupt-topic", 1);
        var backupDir = Path.Combine(_tempDir, "corrupt-verify-backup");

        var backupService = new BackupService(dataDir);
        await backupService.CreateBackupAsync(backupDir, computeChecksums: true);

        // Corrupt a file
        var logFiles = Directory.GetFiles(Path.Combine(backupDir, "data", "corrupt-topic", "partition-0"), "*.log");
        if (logFiles.Length > 0)
        {
            await File.WriteAllTextAsync(logFiles[0], "corrupted content");
        }

        // Act
        var restoreService = new RestoreService();
        var result = await restoreService.VerifyAsync(backupDir);

        // Assert
        Assert.False(result.IsValid);
        Assert.NotEmpty(result.CorruptedFiles);

        _output.WriteLine($"Found {result.CorruptedFiles.Count} corrupted files");
    }

    [Fact]
    public async Task RestoreService_ListBackups_ReturnsAllBackups()
    {
        // Arrange
        var backupsDir = Path.Combine(_tempDir, "all-backups");
        Directory.CreateDirectory(backupsDir);

        // Create two backups
        var dataDir = Path.Combine(_tempDir, "list-data");
        CreateTestTopicStructure(dataDir, "list-topic", 1);

        var backupService = new BackupService(dataDir);
        await backupService.CreateBackupAsync(Path.Combine(backupsDir, "backup-1"));
        await Task.Delay(100); // Ensure different timestamps
        await backupService.CreateBackupAsync(Path.Combine(backupsDir, "backup-2"));

        // Act
        var restoreService = new RestoreService();
        var backups = await restoreService.ListBackupsAsync(backupsDir);

        // Assert
        Assert.Equal(2, backups.Count);
        // Should be sorted by date descending
        Assert.True(backups[0].CreatedAt >= backups[1].CreatedAt);

        foreach (var backup in backups)
        {
            _output.WriteLine($"Found backup: {backup.BackupId} created at {backup.CreatedAt}");
        }
    }

    #endregion

    #region Helper Methods

    private static void CreateTestTopicStructure(string dataDir, string topicName, int partitions)
    {
        for (var p = 0; p < partitions; p++)
        {
            var partitionDir = Path.Combine(dataDir, topicName, $"partition-{p}");
            Directory.CreateDirectory(partitionDir);

            // Create a simple log segment file with valid structure
            var logFile = Path.Combine(partitionDir, "00000000000000000000.log");
            var logContent = CreateValidRecordBatch();
            File.WriteAllBytes(logFile, logContent);

            // Create index file
            var indexFile = Path.Combine(partitionDir, "00000000000000000000.index");
            File.WriteAllBytes(indexFile, new byte[16]); // Simple empty index

            // Create timeindex file
            var timeIndexFile = Path.Combine(partitionDir, "00000000000000000000.timeindex");
            File.WriteAllBytes(timeIndexFile, new byte[16]); // Simple empty timeindex
        }
    }

    private static void CreateTestMetadata(string dataDir)
    {
        var metadataDir = Path.Combine(dataDir, ".metadata");
        Directory.CreateDirectory(metadataDir);

        var topicsJson = "[{\"Name\":\"test-topic\",\"TopicId\":\"00000000-0000-0000-0000-000000000001\",\"PartitionCount\":1}]";
        File.WriteAllText(Path.Combine(metadataDir, "topics.json"), topicsJson);
    }

    private static byte[] CreateValidRecordBatch()
    {
        // Create a minimal valid Kafka RecordBatch
        var batch = new byte[100];

        // BaseOffset (0-7)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0);

        // BatchLength (8-11)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);

        // PartitionLeaderEpoch (12-15)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);

        // Magic (16) = 2 for Kafka v2
        batch[16] = 2;

        // Attributes (21-22)
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0);

        // LastOffsetDelta (23-26)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), 0);

        // BaseTimestamp (27-34)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // MaxTimestamp (35-42)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // ProducerId (43-50)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);

        // ProducerEpoch (51-52)
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);

        // BaseSequence (53-56)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);

        // RecordCount (57-60)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);

        // Compute and write CRC (17-20)
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);

        return batch;
    }

    #endregion
}
