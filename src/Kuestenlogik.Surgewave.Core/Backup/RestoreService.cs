using System.Security.Cryptography;

namespace Kuestenlogik.Surgewave.Core.Backup;

/// <summary>
/// Service for restoring Surgewave data from backups.
/// </summary>
public sealed class RestoreService
{
    /// <summary>
    /// Event raised when a file is being restored.
    /// </summary>
    public event EventHandler<FileRestoreEventArgs>? FileRestore;

    /// <summary>
    /// Event raised when progress is made.
    /// </summary>
    public event EventHandler<RestoreProgressEventArgs>? ProgressChanged;

    /// <summary>
    /// Restore from a backup to the specified data directory.
    /// </summary>
    /// <param name="backupPath">Path to the backup directory containing manifest.json.</param>
    /// <param name="dataDirectory">Target data directory for restoration.</param>
    /// <param name="topics">Optional list of topics to restore. If null, restores all topics.</param>
    /// <param name="verifyChecksums">Whether to verify file checksums during restore.</param>
    /// <param name="overwrite">Whether to overwrite existing files.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Restore result with details about the operation.</returns>
    public Task<RestoreResult> RestoreAsync(
        string backupPath,
        string dataDirectory,
        IReadOnlyList<string>? topics = null,
        bool verifyChecksums = true,
        bool overwrite = false,
        CancellationToken cancellationToken = default)
        => RestoreAsync(backupPath, dataDirectory, new RestoreOptions
        {
            Topics = topics,
            VerifyChecksums = verifyChecksums,
            Overwrite = overwrite,
        }, cancellationToken);

    /// <summary>
    /// Restore from a backup with point-in-time controls. Segments newer than
    /// <see cref="RestoreOptions.TargetTimestampMs"/> or beyond a partition's
    /// <see cref="RestoreOptions.TargetOffsetsPerPartition"/> cutoff are
    /// skipped during restore. The metadata directory always restores in full
    /// — PIT applies to topic data.
    /// </summary>
    public async Task<RestoreResult> RestoreAsync(
        string backupPath,
        string dataDirectory,
        RestoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var topics = options.Topics;
        var verifyChecksums = options.VerifyChecksums;
        var overwrite = options.Overwrite;
        var result = new RestoreResult();

        // Load manifest
        var manifestPath = Path.Combine(backupPath, BackupManifest.FileName);
        if (!File.Exists(manifestPath))
        {
            result.Success = false;
            result.ErrorMessage = $"Manifest not found at {manifestPath}";
            return result;
        }

        var manifest = await BackupManifest.LoadAsync(manifestPath, cancellationToken);
        result.BackupId = manifest.BackupId;
        result.BackupCreatedAt = manifest.CreatedAt;

        // Determine topics to restore
        var topicsToRestore = topics == null || topics.Count == 0
            ? manifest.Topics
            : manifest.Topics.Where(t => topics.Contains(t.Name)).ToList();

        if (topicsToRestore.Count == 0)
        {
            result.Success = false;
            result.ErrorMessage = "No matching topics found in backup";
            return result;
        }

        var progress = new RestoreProgress { TotalTopics = topicsToRestore.Count };

        // Restore each topic
        foreach (var topicInfo in topicsToRestore)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var topicResult = await RestoreTopicAsync(
                backupPath, dataDirectory, topicInfo, manifest.FileChecksums,
                verifyChecksums, overwrite, options, cancellationToken);

            result.TopicsRestored++;
            result.FilesRestored += topicResult.FilesRestored;
            result.BytesRestored += topicResult.BytesRestored;
            result.SegmentsSkipped += topicResult.SegmentsSkipped;

            if (topicResult.VerificationErrors.Count > 0)
            {
                result.VerificationErrors.AddRange(topicResult.VerificationErrors);
            }

            progress.CompletedTopics++;
            progress.CurrentTopic = topicInfo.Name;
            progress.BytesRestored = result.BytesRestored;
            ProgressChanged?.Invoke(this, new RestoreProgressEventArgs(progress));
        }

        // Restore metadata if included
        if (manifest.IncludesMetadata)
        {
            var metadataResult = await RestoreMetadataAsync(
                backupPath, dataDirectory, manifest.FileChecksums,
                verifyChecksums, overwrite, cancellationToken);

            result.FilesRestored += metadataResult.FilesRestored;
            result.BytesRestored += metadataResult.BytesRestored;

            if (metadataResult.VerificationErrors.Count > 0)
            {
                result.VerificationErrors.AddRange(metadataResult.VerificationErrors);
            }
        }

        result.Success = result.VerificationErrors.Count == 0;
        if (!result.Success)
        {
            result.ErrorMessage = $"{result.VerificationErrors.Count} file(s) failed checksum verification";
        }

        return result;
    }

    /// <summary>
    /// Verify a backup without restoring.
    /// </summary>
    public async Task<VerifyResult> VerifyAsync(
        string backupPath,
        CancellationToken cancellationToken = default)
    {
        var result = new VerifyResult();

        // Load manifest
        var manifestPath = Path.Combine(backupPath, BackupManifest.FileName);
        if (!File.Exists(manifestPath))
        {
            result.IsValid = false;
            result.ErrorMessage = $"Manifest not found at {manifestPath}";
            return result;
        }

        var manifest = await BackupManifest.LoadAsync(manifestPath, cancellationToken);
        result.BackupId = manifest.BackupId;
        result.BackupCreatedAt = manifest.CreatedAt;
        result.TotalFiles = manifest.TotalFiles;
        result.TotalBytes = manifest.TotalBytes;
        result.TopicCount = manifest.Topics.Count;

        var dataPath = Path.Combine(backupPath, "data");

        // Verify checksums if available
        if (manifest.FileChecksums.Count > 0)
        {
            foreach (var (relativePath, expectedHash) in manifest.FileChecksums)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var fullPath = Path.Combine(dataPath, relativePath);
                if (!File.Exists(fullPath))
                {
                    result.MissingFiles.Add(relativePath);
                    continue;
                }

                var actualHash = await ComputeFileHashAsync(fullPath, cancellationToken);
                if (!string.Equals(actualHash, expectedHash, StringComparison.OrdinalIgnoreCase))
                {
                    result.CorruptedFiles.Add(new CorruptedFileInfo
                    {
                        Path = relativePath,
                        ExpectedHash = expectedHash,
                        ActualHash = actualHash
                    });
                }
                else
                {
                    result.FilesVerified++;
                    result.BytesVerified += new FileInfo(fullPath).Length;
                }
            }
        }
        else
        {
            // No checksums, just verify files exist
            foreach (var topic in manifest.Topics)
            {
                foreach (var partition in topic.Partitions)
                {
                    foreach (var segment in partition.Segments)
                    {
                        var segmentPath = Path.Combine(dataPath, topic.Name, $"partition-{partition.PartitionId}");

                        var logPath = Path.Combine(segmentPath, segment.LogFile);
                        if (!File.Exists(logPath))
                        {
                            result.MissingFiles.Add(Path.Combine(topic.Name, $"partition-{partition.PartitionId}", segment.LogFile));
                        }
                        else
                        {
                            result.FilesVerified++;
                            result.BytesVerified += new FileInfo(logPath).Length;
                        }

                        // Check index files
                        if (!File.Exists(Path.Combine(segmentPath, segment.IndexFile)))
                        {
                            result.MissingFiles.Add(Path.Combine(topic.Name, $"partition-{partition.PartitionId}", segment.IndexFile));
                        }

                        if (!File.Exists(Path.Combine(segmentPath, segment.TimeIndexFile)))
                        {
                            result.MissingFiles.Add(Path.Combine(topic.Name, $"partition-{partition.PartitionId}", segment.TimeIndexFile));
                        }
                    }
                }
            }
        }

        result.IsValid = result.MissingFiles.Count == 0 && result.CorruptedFiles.Count == 0;
        if (!result.IsValid)
        {
            var errors = new List<string>();
            if (result.MissingFiles.Count > 0)
            {
                errors.Add($"{result.MissingFiles.Count} missing file(s)");
            }
            if (result.CorruptedFiles.Count > 0)
            {
                errors.Add($"{result.CorruptedFiles.Count} corrupted file(s)");
            }
            result.ErrorMessage = string.Join(", ", errors);
        }

        return result;
    }

    /// <summary>
    /// List backups in a directory.
    /// </summary>
    public async Task<List<BackupManifest>> ListBackupsAsync(
        string backupsDirectory,
        CancellationToken cancellationToken = default)
    {
        var manifests = new List<BackupManifest>();

        if (!Directory.Exists(backupsDirectory))
        {
            return manifests;
        }

        // Look for manifest.json in immediate subdirectories
        foreach (var dir in Directory.GetDirectories(backupsDirectory))
        {
            var manifestPath = Path.Combine(dir, BackupManifest.FileName);
            if (File.Exists(manifestPath))
            {
                try
                {
                    var manifest = await BackupManifest.LoadAsync(manifestPath, cancellationToken);
                    manifests.Add(manifest);
                }
                catch
                {
                    // Skip invalid manifests
                }
            }
        }

        // Also check root directory for single backup
        var rootManifest = Path.Combine(backupsDirectory, BackupManifest.FileName);
        if (File.Exists(rootManifest))
        {
            try
            {
                var manifest = await BackupManifest.LoadAsync(rootManifest, cancellationToken);
                manifests.Add(manifest);
            }
            catch
            {
                // Skip invalid manifest
            }
        }

        return manifests.OrderByDescending(m => m.CreatedAt).ToList();
    }

    /// <summary>
    /// Restore a single topic.
    /// </summary>
    private async Task<TopicRestoreResult> RestoreTopicAsync(
        string backupPath,
        string dataDirectory,
        BackupTopicInfo topicInfo,
        Dictionary<string, string> checksums,
        bool verifyChecksums,
        bool overwrite,
        RestoreOptions options,
        CancellationToken cancellationToken)
    {
        var result = new TopicRestoreResult();

        foreach (var partition in topicInfo.Partitions)
        {
            var sourcePath = Path.Combine(backupPath, "data", topicInfo.Name, $"partition-{partition.PartitionId}");
            var destPath = Path.Combine(dataDirectory, topicInfo.Name, $"partition-{partition.PartitionId}");

            if (!Directory.Exists(sourcePath))
            {
                continue;
            }

            Directory.CreateDirectory(destPath);

            var partitionOffsetCutoff = ResolveOffsetCutoff(options, topicInfo.Name, partition.PartitionId);
            var timestampCutoff = options.TargetTimestampMs;

            foreach (var segment in partition.Segments)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (!ShouldRestoreSegment(segment, partitionOffsetCutoff, timestampCutoff))
                {
                    result.SegmentsSkipped++;
                    continue;
                }

                // Restore log file
                result.FilesRestored += await RestoreFileAsync(
                    Path.Combine(sourcePath, segment.LogFile),
                    Path.Combine(destPath, segment.LogFile),
                    checksums, verifyChecksums, overwrite, result, cancellationToken);

                // Restore index file
                result.FilesRestored += await RestoreFileAsync(
                    Path.Combine(sourcePath, segment.IndexFile),
                    Path.Combine(destPath, segment.IndexFile),
                    checksums, verifyChecksums, overwrite, result, cancellationToken);

                // Restore time index file
                result.FilesRestored += await RestoreFileAsync(
                    Path.Combine(sourcePath, segment.TimeIndexFile),
                    Path.Combine(destPath, segment.TimeIndexFile),
                    checksums, verifyChecksums, overwrite, result, cancellationToken);
            }
        }

        return result;
    }

    private static long? ResolveOffsetCutoff(RestoreOptions options, string topic, int partitionId)
    {
        if (options.TargetOffsetsPerPartition is null) return null;
        var key = RestoreOptions.PartitionKey(topic, partitionId);
        return options.TargetOffsetsPerPartition.TryGetValue(key, out var cutoff) ? cutoff : null;
    }

    /// <summary>
    /// Decide whether a segment falls within the PIT window. Segment-boundary
    /// granularity: a segment whose <c>BaseOffset</c> is past the offset
    /// cutoff is skipped entirely; one whose <c>MaxTimestampMs</c> exceeds
    /// the timestamp cutoff is also skipped (the segment was rotated after
    /// the target moment). Segments with <c>MaxTimestampMs == 0</c> have no
    /// time-index entries to base a decision on, so we keep them — silently
    /// dropping them would lose their data.
    /// </summary>
    internal static bool ShouldRestoreSegment(
        BackupSegmentInfo segment,
        long? offsetCutoff,
        long? timestampCutoffMs)
    {
        if (offsetCutoff is { } off && segment.BaseOffset > off)
        {
            return false;
        }
        if (timestampCutoffMs is { } tsCutoff
            && segment.MaxTimestampMs > 0
            && segment.MaxTimestampMs > tsCutoff)
        {
            return false;
        }
        return true;
    }

    /// <summary>
    /// Restore metadata files.
    /// </summary>
    private async Task<TopicRestoreResult> RestoreMetadataAsync(
        string backupPath,
        string dataDirectory,
        Dictionary<string, string> checksums,
        bool verifyChecksums,
        bool overwrite,
        CancellationToken cancellationToken)
    {
        var result = new TopicRestoreResult();

        var sourcePath = Path.Combine(backupPath, "data", ".metadata");
        var destPath = Path.Combine(dataDirectory, ".metadata");

        if (!Directory.Exists(sourcePath))
        {
            return result;
        }

        await RestoreDirectoryAsync(sourcePath, destPath, checksums, verifyChecksums, overwrite, result, cancellationToken);

        return result;
    }

    /// <summary>
    /// Recursively restore a directory.
    /// </summary>
    private async Task RestoreDirectoryAsync(
        string sourceDir,
        string destDir,
        Dictionary<string, string> checksums,
        bool verifyChecksums,
        bool overwrite,
        TopicRestoreResult result,
        CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(destDir);

        foreach (var file in Directory.GetFiles(sourceDir))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fileName = Path.GetFileName(file);
            var destFile = Path.Combine(destDir, fileName);
            result.FilesRestored += await RestoreFileAsync(file, destFile, checksums, verifyChecksums, overwrite, result, cancellationToken);
        }

        foreach (var subDir in Directory.GetDirectories(sourceDir))
        {
            var subDirName = Path.GetFileName(subDir);
            var destSubDir = Path.Combine(destDir, subDirName);
            await RestoreDirectoryAsync(subDir, destSubDir, checksums, verifyChecksums, overwrite, result, cancellationToken);
        }
    }

    /// <summary>
    /// Restore a single file with optional checksum verification.
    /// </summary>
    private async Task<int> RestoreFileAsync(
        string sourcePath,
        string destPath,
        Dictionary<string, string> checksums,
        bool verifyChecksums,
        bool overwrite,
        TopicRestoreResult result,
        CancellationToken cancellationToken)
    {
        if (!File.Exists(sourcePath))
        {
            return 0;
        }

        if (File.Exists(destPath) && !overwrite)
        {
            return 0; // Skip existing file
        }

        var fileInfo = new FileInfo(sourcePath);
        FileRestore?.Invoke(this, new FileRestoreEventArgs(sourcePath, fileInfo.Length));

        await using var sourceStream = new FileStream(sourcePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
        await using var destStream = new FileStream(destPath, FileMode.Create, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);

        if (verifyChecksums && checksums.Count > 0)
        {
            using var sha256 = SHA256.Create();
            var buffer = new byte[81920];
            int bytesRead;

            while ((bytesRead = await sourceStream.ReadAsync(buffer, cancellationToken)) > 0)
            {
                await destStream.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken);
                sha256.TransformBlock(buffer, 0, bytesRead, null, 0);
            }

            sha256.TransformFinalBlock([], 0, 0);
            var actualHash = Convert.ToHexString(sha256.Hash!);

            // Check against expected hash
            var relativePath = Path.GetFileName(sourcePath);
            if (checksums.TryGetValue(relativePath, out var expectedHash))
            {
                if (!string.Equals(actualHash, expectedHash, StringComparison.OrdinalIgnoreCase))
                {
                    result.VerificationErrors.Add($"{sourcePath}: expected {expectedHash}, got {actualHash}");
                }
            }
        }
        else
        {
            await sourceStream.CopyToAsync(destStream, cancellationToken);
        }

        result.BytesRestored += fileInfo.Length;
        return 1;
    }

    /// <summary>
    /// Compute SHA256 hash of a file.
    /// </summary>
    private static async Task<string> ComputeFileHashAsync(string filePath, CancellationToken cancellationToken)
    {
        using var sha256 = SHA256.Create();
        await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);

        var buffer = new byte[81920];
        int bytesRead;

        while ((bytesRead = await stream.ReadAsync(buffer, cancellationToken)) > 0)
        {
            sha256.TransformBlock(buffer, 0, bytesRead, null, 0);
        }

        sha256.TransformFinalBlock([], 0, 0);
        return Convert.ToHexString(sha256.Hash!);
    }

    private sealed class TopicRestoreResult
    {
        public int FilesRestored { get; set; }
        public long BytesRestored { get; set; }
        public int SegmentsSkipped { get; set; }
        public List<string> VerificationErrors { get; } = [];
    }
}

/// <summary>
/// Result of a restore operation.
/// </summary>
public sealed class RestoreResult
{
    /// <summary>Whether the restore completed successfully.</summary>
    public bool Success { get; set; } = true;

    /// <summary>Error message if restore failed.</summary>
    public string? ErrorMessage { get; set; }

    /// <summary>Backup ID that was restored.</summary>
    public string? BackupId { get; set; }

    /// <summary>When the backup was created.</summary>
    public DateTimeOffset? BackupCreatedAt { get; set; }

    /// <summary>Number of topics restored.</summary>
    public int TopicsRestored { get; set; }

    /// <summary>Number of files restored.</summary>
    public int FilesRestored { get; set; }

    /// <summary>Total bytes restored.</summary>
    public long BytesRestored { get; set; }

    /// <summary>
    /// Number of segments deliberately skipped because they fell outside the
    /// point-in-time restore window (offset or timestamp cutoff). 0 for an
    /// unbounded restore.
    /// </summary>
    public int SegmentsSkipped { get; set; }

    /// <summary>List of checksum verification errors.</summary>
    public List<string> VerificationErrors { get; } = [];
}

/// <summary>
/// Result of a backup verification.
/// </summary>
public sealed class VerifyResult
{
    /// <summary>Whether the backup is valid.</summary>
    public bool IsValid { get; set; }

    /// <summary>Error message if verification failed.</summary>
    public string? ErrorMessage { get; set; }

    /// <summary>Backup ID.</summary>
    public string? BackupId { get; set; }

    /// <summary>When the backup was created.</summary>
    public DateTimeOffset? BackupCreatedAt { get; set; }

    /// <summary>Total files expected in backup.</summary>
    public int TotalFiles { get; set; }

    /// <summary>Total bytes expected in backup.</summary>
    public long TotalBytes { get; set; }

    /// <summary>Number of topics in backup.</summary>
    public int TopicCount { get; set; }

    /// <summary>Number of files verified.</summary>
    public int FilesVerified { get; set; }

    /// <summary>Bytes verified.</summary>
    public long BytesVerified { get; set; }

    /// <summary>List of missing files.</summary>
    public List<string> MissingFiles { get; } = [];

    /// <summary>List of corrupted files.</summary>
    public List<CorruptedFileInfo> CorruptedFiles { get; } = [];
}

/// <summary>
/// Information about a corrupted file.
/// </summary>
public sealed class CorruptedFileInfo
{
    /// <summary>Relative path of the file.</summary>
    public string Path { get; set; } = string.Empty;

    /// <summary>Expected SHA256 hash.</summary>
    public string ExpectedHash { get; set; } = string.Empty;

    /// <summary>Actual SHA256 hash.</summary>
    public string ActualHash { get; set; } = string.Empty;
}

/// <summary>
/// Progress information for restore operation.
/// </summary>
public sealed class RestoreProgress
{
    /// <summary>Total number of topics to restore.</summary>
    public int TotalTopics { get; set; }

    /// <summary>Number of topics completed.</summary>
    public int CompletedTopics { get; set; }

    /// <summary>Current topic being restored.</summary>
    public string? CurrentTopic { get; set; }

    /// <summary>Total bytes restored so far.</summary>
    public long BytesRestored { get; set; }
}

/// <summary>
/// Event arguments for file restore events.
/// </summary>
public sealed class FileRestoreEventArgs : EventArgs
{
    /// <summary>Path of the file being restored.</summary>
    public string FilePath { get; }

    /// <summary>Size of the file in bytes.</summary>
    public long FileSize { get; }

    public FileRestoreEventArgs(string filePath, long fileSize)
    {
        FilePath = filePath;
        FileSize = fileSize;
    }
}

/// <summary>
/// Event arguments for restore progress events.
/// </summary>
public sealed class RestoreProgressEventArgs : EventArgs
{
    /// <summary>Current progress information.</summary>
    public RestoreProgress Progress { get; }

    public RestoreProgressEventArgs(RestoreProgress progress)
    {
        Progress = progress;
    }
}
