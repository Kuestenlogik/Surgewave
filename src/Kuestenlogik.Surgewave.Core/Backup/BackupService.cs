using System.Security.Cryptography;

namespace Kuestenlogik.Surgewave.Core.Backup;

/// <summary>
/// Service for creating backups of Surgewave data.
/// </summary>
public sealed class BackupService
{
    private readonly string _dataDirectory;

    /// <summary>
    /// Event raised when a file is being backed up.
    /// </summary>
    public event EventHandler<FileBackupEventArgs>? FileBackup;

    /// <summary>
    /// Event raised when progress is made.
    /// </summary>
    public event EventHandler<BackupProgressEventArgs>? ProgressChanged;

    public BackupService(string dataDirectory)
    {
        _dataDirectory = dataDirectory;
    }

    /// <summary>
    /// Create a backup of all or selected topics.
    /// </summary>
    /// <param name="outputPath">Directory to store the backup.</param>
    /// <param name="topics">Optional list of topics to backup. If null, backs up all topics.</param>
    /// <param name="includeMetadata">Whether to include metadata files.</param>
    /// <param name="computeChecksums">Whether to compute SHA256 checksums for verification.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Backup manifest with details about the backup.</returns>
    public async Task<BackupManifest> CreateBackupAsync(
        string outputPath,
        IReadOnlyList<string>? topics = null,
        bool includeMetadata = true,
        bool computeChecksums = true,
        CancellationToken cancellationToken = default)
    {
        // Create output directory
        Directory.CreateDirectory(outputPath);

        var manifest = new BackupManifest
        {
            CreatedAt = DateTimeOffset.UtcNow,
            IncludesMetadata = includeMetadata
        };

        var totalFiles = 0;
        var totalBytes = 0L;

        // Define backup data root for relative path calculation
        var backupDataRoot = Path.Combine(outputPath, "data");

        // Get list of topics to backup
        var topicsToBackup = GetTopicsToBackup(topics);
        var progress = new BackupProgress { TotalTopics = topicsToBackup.Count };

        foreach (var topicName in topicsToBackup)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var topicInfo = await BackupTopicAsync(
                topicName, outputPath, backupDataRoot, computeChecksums, manifest.FileChecksums, cancellationToken);

            if (topicInfo != null)
            {
                manifest.Topics.Add(topicInfo);
                totalFiles += topicInfo.Partitions.Sum(p => p.Segments.Count * 3); // log, index, timeindex
                totalBytes += topicInfo.TotalBytes;

                progress.CompletedTopics++;
                progress.CurrentTopic = topicName;
                ProgressChanged?.Invoke(this, new BackupProgressEventArgs(progress));
            }
        }

        // Backup metadata
        if (includeMetadata)
        {
            var metadataFiles = await BackupMetadataAsync(outputPath, backupDataRoot, computeChecksums, manifest.FileChecksums, cancellationToken);
            totalFiles += metadataFiles;
        }

        manifest.TotalFiles = totalFiles;
        manifest.TotalBytes = totalBytes;

        // Save manifest
        var manifestPath = Path.Combine(outputPath, BackupManifest.FileName);
        await manifest.SaveAsync(manifestPath, cancellationToken);

        return manifest;
    }

    /// <summary>
    /// Get list of topics to backup.
    /// </summary>
    private List<string> GetTopicsToBackup(IReadOnlyList<string>? requestedTopics)
    {
        if (!Directory.Exists(_dataDirectory))
        {
            return [];
        }

        var allTopics = Directory.GetDirectories(_dataDirectory)
            .Select(Path.GetFileName)
            .Where(name => name != null && !name.StartsWith('.')) // Exclude metadata directories
            .Cast<string>()
            .ToList();

        if (requestedTopics == null || requestedTopics.Count == 0)
        {
            return allTopics;
        }

        // Filter to requested topics that exist
        return allTopics.Where(t => requestedTopics.Contains(t)).ToList();
    }

    /// <summary>
    /// Backup a single topic.
    /// </summary>
    private async Task<BackupTopicInfo?> BackupTopicAsync(
        string topicName,
        string outputPath,
        string backupDataRoot,
        bool computeChecksums,
        Dictionary<string, string> checksums,
        CancellationToken cancellationToken)
    {
        var topicDir = Path.Combine(_dataDirectory, topicName);
        if (!Directory.Exists(topicDir))
        {
            return null;
        }

        var topicInfo = new BackupTopicInfo
        {
            Name = topicName
        };

        // Find all partition directories
        var partitionDirs = Directory.GetDirectories(topicDir, "partition-*")
            .OrderBy(d => ParsePartitionId(Path.GetFileName(d)))
            .ToList();

        topicInfo.PartitionCount = partitionDirs.Count;

        foreach (var partitionDir in partitionDirs)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var partitionId = ParsePartitionId(Path.GetFileName(partitionDir));
            var partitionInfo = await BackupPartitionAsync(
                topicName, partitionId, partitionDir, outputPath, backupDataRoot, computeChecksums, checksums, cancellationToken);

            topicInfo.Partitions.Add(partitionInfo);
            topicInfo.TotalBytes += partitionInfo.TotalBytes;
            topicInfo.TotalSegments += partitionInfo.SegmentCount;
        }

        return topicInfo;
    }

    /// <summary>
    /// Backup a single partition.
    /// </summary>
    private async Task<BackupPartitionInfo> BackupPartitionAsync(
        string topicName,
        int partitionId,
        string sourceDir,
        string outputPath,
        string backupDataRoot,
        bool computeChecksums,
        Dictionary<string, string> checksums,
        CancellationToken cancellationToken)
    {
        var partitionInfo = new BackupPartitionInfo
        {
            PartitionId = partitionId
        };

        // Create destination directory
        var destDir = Path.Combine(outputPath, "data", topicName, $"partition-{partitionId}");
        Directory.CreateDirectory(destDir);

        // Find all log segment files
        var logFiles = Directory.GetFiles(sourceDir, "*.log")
            .OrderBy(f => ParseBaseOffset(Path.GetFileName(f)))
            .ToList();

        partitionInfo.SegmentCount = logFiles.Count;

        foreach (var logFile in logFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var baseOffset = ParseBaseOffset(Path.GetFileName(logFile));
            var segmentInfo = await BackupSegmentAsync(
                sourceDir, destDir, backupDataRoot, baseOffset, computeChecksums, checksums, cancellationToken);

            partitionInfo.Segments.Add(segmentInfo);
            partitionInfo.TotalBytes += segmentInfo.LogSize + segmentInfo.IndexSize + segmentInfo.TimeIndexSize;

            // Track offsets from first and last segments
            if (partitionInfo.Segments.Count == 1)
            {
                partitionInfo.LogStartOffset = baseOffset;
            }
        }

        // High watermark is the base offset of the last segment (approximate)
        if (partitionInfo.Segments.Count > 0)
        {
            partitionInfo.HighWatermark = partitionInfo.Segments[^1].BaseOffset;
        }

        return partitionInfo;
    }

    /// <summary>
    /// Backup a single segment (log, index, timeindex files).
    /// </summary>
    private async Task<BackupSegmentInfo> BackupSegmentAsync(
        string sourceDir,
        string destDir,
        string backupDataRoot,
        long baseOffset,
        bool computeChecksums,
        Dictionary<string, string> checksums,
        CancellationToken cancellationToken)
    {
        var segmentInfo = new BackupSegmentInfo
        {
            BaseOffset = baseOffset,
            LogFile = $"{baseOffset:D20}.log",
            IndexFile = $"{baseOffset:D20}.index",
            TimeIndexFile = $"{baseOffset:D20}.timeindex"
        };

        // Copy log file
        var logSource = Path.Combine(sourceDir, segmentInfo.LogFile);
        var logDest = Path.Combine(destDir, segmentInfo.LogFile);
        if (File.Exists(logSource))
        {
            segmentInfo.LogSize = await CopyFileAsync(logSource, logDest, backupDataRoot, computeChecksums, checksums, cancellationToken);
        }

        // Copy index file
        var indexSource = Path.Combine(sourceDir, segmentInfo.IndexFile);
        var indexDest = Path.Combine(destDir, segmentInfo.IndexFile);
        if (File.Exists(indexSource))
        {
            segmentInfo.IndexSize = await CopyFileAsync(indexSource, indexDest, backupDataRoot, computeChecksums, checksums, cancellationToken);
        }

        // Copy time index file
        var timeIndexSource = Path.Combine(sourceDir, segmentInfo.TimeIndexFile);
        var timeIndexDest = Path.Combine(destDir, segmentInfo.TimeIndexFile);

        // Read the last entry of the time-index to record the max timestamp
        // in this segment — point-in-time restore uses this to filter
        // segments at the timestamp cutoff. The on-disk format is a packed
        // sequence of (timestamp:i64-be, relativeOffset:i32-be) entries; the
        // last 12 bytes give the largest timestamp seen at backup time.
        if (File.Exists(timeIndexSource))
        {
            segmentInfo.MaxTimestampMs = await ReadMaxTimestampAsync(timeIndexSource, cancellationToken).ConfigureAwait(false);
        }
        if (File.Exists(timeIndexSource))
        {
            segmentInfo.TimeIndexSize = await CopyFileAsync(timeIndexSource, timeIndexDest, backupDataRoot, computeChecksums, checksums, cancellationToken);
        }

        return segmentInfo;
    }

    /// <summary>
    /// Backup metadata files.
    /// </summary>
    private async Task<int> BackupMetadataAsync(
        string outputPath,
        string backupDataRoot,
        bool computeChecksums,
        Dictionary<string, string> checksums,
        CancellationToken cancellationToken)
    {
        var metadataSource = Path.Combine(_dataDirectory, ".metadata");
        if (!Directory.Exists(metadataSource))
        {
            return 0;
        }

        var metadataDest = Path.Combine(outputPath, "data", ".metadata");
        Directory.CreateDirectory(metadataDest);

        var fileCount = 0;

        // Copy all files in metadata directory
        foreach (var file in Directory.GetFiles(metadataSource))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fileName = Path.GetFileName(file);
            var destFile = Path.Combine(metadataDest, fileName);
            await CopyFileAsync(file, destFile, backupDataRoot, computeChecksums, checksums, cancellationToken);
            fileCount++;
        }

        // Recursively copy subdirectories (groups, transactions, etc.)
        foreach (var subDir in Directory.GetDirectories(metadataSource))
        {
            var subDirName = Path.GetFileName(subDir);
            var destSubDir = Path.Combine(metadataDest, subDirName);
            fileCount += await CopyDirectoryAsync(subDir, destSubDir, backupDataRoot, computeChecksums, checksums, cancellationToken);
        }

        return fileCount;
    }

    /// <summary>
    /// Recursively copy a directory.
    /// </summary>
    private async Task<int> CopyDirectoryAsync(
        string sourceDir,
        string destDir,
        string backupDataRoot,
        bool computeChecksums,
        Dictionary<string, string> checksums,
        CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(destDir);
        var fileCount = 0;

        foreach (var file in Directory.GetFiles(sourceDir))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fileName = Path.GetFileName(file);
            var destFile = Path.Combine(destDir, fileName);
            await CopyFileAsync(file, destFile, backupDataRoot, computeChecksums, checksums, cancellationToken);
            fileCount++;
        }

        foreach (var subDir in Directory.GetDirectories(sourceDir))
        {
            var subDirName = Path.GetFileName(subDir);
            var destSubDir = Path.Combine(destDir, subDirName);
            fileCount += await CopyDirectoryAsync(subDir, destSubDir, backupDataRoot, computeChecksums, checksums, cancellationToken);
        }

        return fileCount;
    }

    /// <summary>
    /// Copy a file and optionally compute its checksum.
    /// </summary>
    private async Task<long> CopyFileAsync(
        string source,
        string dest,
        string backupDataRoot,
        bool computeChecksum,
        Dictionary<string, string> checksums,
        CancellationToken cancellationToken)
    {
        var fileInfo = new FileInfo(source);
        FileBackup?.Invoke(this, new FileBackupEventArgs(source, fileInfo.Length));

        await using var sourceStream = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);
        await using var destStream = new FileStream(dest, FileMode.Create, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan);

        if (computeChecksum)
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
            var hash = Convert.ToHexString(sha256.Hash!);

            // Store relative path from backup data root in checksums
            var relativePath = Path.GetRelativePath(backupDataRoot, dest);
            checksums[relativePath] = hash;
        }
        else
        {
            await sourceStream.CopyToAsync(destStream, cancellationToken);
        }

        return fileInfo.Length;
    }

    /// <summary>
    /// Parse partition ID from directory name like "partition-0".
    /// </summary>
    private static int ParsePartitionId(string? dirName)
    {
        if (dirName == null || !dirName.StartsWith("partition-", StringComparison.Ordinal))
        {
            return -1;
        }

        return int.TryParse(dirName.AsSpan(10), out var id) ? id : -1;
    }

    /// <summary>
    /// Parse base offset from filename like "00000000000000000000.log".
    /// </summary>
    private static long ParseBaseOffset(string? fileName)
    {
        if (fileName == null || fileName.Length < 20)
        {
            return -1;
        }

        return long.TryParse(fileName.AsSpan(0, 20), out var offset) ? offset : -1;
    }

    /// <summary>
    /// Reads the last (timestamp, relativeOffset) entry from a Kafka-format
    /// time index, returning the timestamp in Unix milliseconds. Returns 0
    /// when the file is empty (pre-rotation or no records yet) — callers
    /// treat 0 as "unknown / always include" during PIT filtering.
    /// </summary>
    private static async Task<long> ReadMaxTimestampAsync(string timeIndexPath, CancellationToken cancellationToken)
    {
        const int EntrySize = 12; // i64 timestamp + i32 relative offset
        try
        {
            await using var stream = new FileStream(
                timeIndexPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                bufferSize: EntrySize, FileOptions.Asynchronous);
            if (stream.Length < EntrySize) return 0;

            stream.Seek(-EntrySize, SeekOrigin.End);
            var buffer = new byte[EntrySize];
            var read = 0;
            while (read < EntrySize)
            {
                var n = await stream.ReadAsync(buffer.AsMemory(read), cancellationToken).ConfigureAwait(false);
                if (n == 0) break;
                read += n;
            }
            if (read < EntrySize) return 0;

            // Big-endian int64 timestamp.
            return ((long)buffer[0] << 56) | ((long)buffer[1] << 48)
                 | ((long)buffer[2] << 40) | ((long)buffer[3] << 32)
                 | ((long)buffer[4] << 24) | ((long)buffer[5] << 16)
                 | ((long)buffer[6] << 8)  | buffer[7];
        }
        catch (IOException)
        {
            return 0;
        }
    }
}

/// <summary>
/// Progress information for backup operation.
/// </summary>
public sealed class BackupProgress
{
    /// <summary>Total number of topics to backup.</summary>
    public int TotalTopics { get; set; }

    /// <summary>Number of topics completed.</summary>
    public int CompletedTopics { get; set; }

    /// <summary>Current topic being backed up.</summary>
    public string? CurrentTopic { get; set; }

    /// <summary>Total bytes copied so far.</summary>
    public long BytesCopied { get; set; }
}

/// <summary>
/// Event arguments for file backup events.
/// </summary>
public sealed class FileBackupEventArgs : EventArgs
{
    /// <summary>Path of the file being backed up.</summary>
    public string FilePath { get; }

    /// <summary>Size of the file in bytes.</summary>
    public long FileSize { get; }

    public FileBackupEventArgs(string filePath, long fileSize)
    {
        FilePath = filePath;
        FileSize = fileSize;
    }
}

/// <summary>
/// Event arguments for backup progress events.
/// </summary>
public sealed class BackupProgressEventArgs : EventArgs
{
    /// <summary>Current progress information.</summary>
    public BackupProgress Progress { get; }

    public BackupProgressEventArgs(BackupProgress progress)
    {
        Progress = progress;
    }
}
