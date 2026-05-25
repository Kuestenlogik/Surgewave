using System.Buffers.Binary;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Persists Raft state (currentTerm, votedFor, log, snapshots) to disk.
/// Ensures durability of the Raft protocol.
/// </summary>
public sealed partial class RaftPersistence
{
    private readonly ILogger<RaftPersistence> _logger;
    private readonly string _dataDirectory;
    private readonly string _stateFile;
    private readonly string _logFile;
    private readonly string _snapshotDirectory;
    private readonly object _lock = new();

    public RaftPersistence(ILogger<RaftPersistence> logger, ClusteringConfig config)
    {
        _logger = logger;
        _dataDirectory = config.RaftDataDirectory;
        _stateFile = Path.Combine(_dataDirectory, "raft-state.json");
        _logFile = Path.Combine(_dataDirectory, "raft-log.bin");
        _snapshotDirectory = Path.Combine(_dataDirectory, "snapshots");
    }

    /// <summary>
    /// Ensure the data directory exists.
    /// </summary>
    public void EnsureDirectoryExists()
    {
        if (!Directory.Exists(_dataDirectory))
        {
            Directory.CreateDirectory(_dataDirectory);
            LogDirectoryCreated(_dataDirectory);
        }
    }

    /// <summary>
    /// Load persistent state (currentTerm, votedFor).
    /// </summary>
    public async Task<RaftPersistentState> LoadStateAsync(CancellationToken ct)
    {
        EnsureDirectoryExists();

        if (!File.Exists(_stateFile))
        {
            return new RaftPersistentState(0, null);
        }

        try
        {
            var json = await File.ReadAllTextAsync(_stateFile, ct);
            var state = JsonSerializer.Deserialize(json, ClusteringJsonContext.Default.RaftPersistentState);
            LogStateLoaded(state?.CurrentTerm ?? 0, state?.VotedFor);
            return state ?? new RaftPersistentState(0, null);
        }
        catch (Exception ex)
        {
            LogStateLoadError(ex);
            return new RaftPersistentState(0, null);
        }
    }

    /// <summary>
    /// Save persistent state (currentTerm, votedFor).
    /// </summary>
    public async Task SaveStateAsync(int currentTerm, int? votedFor, CancellationToken ct)
    {
        EnsureDirectoryExists();

        var state = new RaftPersistentState(currentTerm, votedFor);
        var json = JsonSerializer.Serialize(state, ClusteringJsonContext.Default.RaftPersistentState);

        // Write to temp file then rename for atomicity
        var tempFile = _stateFile + ".tmp";
        await File.WriteAllTextAsync(tempFile, json, ct);
        File.Move(tempFile, _stateFile, overwrite: true);

        LogStateSaved(currentTerm, votedFor);
    }

    /// <summary>
    /// Load log entries from disk.
    /// </summary>
    public async Task<List<RaftLogEntry>> LoadLogAsync(CancellationToken ct)
    {
        EnsureDirectoryExists();

        var entries = new List<RaftLogEntry>();

        if (!File.Exists(_logFile))
        {
            return entries;
        }

        try
        {
            await using var stream = new FileStream(_logFile, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(stream);

            while (stream.Position < stream.Length)
            {
                var entry = ReadLogEntry(reader);
                if (entry != null)
                {
                    entries.Add(entry);
                }
            }

            LogEntriesLoaded(entries.Count);
        }
        catch (Exception ex)
        {
            LogLogLoadError(ex);
        }

        return entries;
    }

    /// <summary>
    /// Append entries to the log file.
    /// </summary>
    public async Task AppendEntriesAsync(IEnumerable<RaftLogEntry> entries, CancellationToken ct)
    {
        EnsureDirectoryExists();

        lock (_lock)
        {
            using var stream = new FileStream(_logFile, FileMode.Append, FileAccess.Write, FileShare.None);
            using var writer = new BinaryWriter(stream);

            foreach (var entry in entries)
            {
                WriteLogEntry(writer, entry);
            }

            stream.Flush();
        }
    }

    /// <summary>
    /// Truncate log from the given index (delete index and all following).
    /// </summary>
    public async Task TruncateLogAsync(long fromIndex, CancellationToken ct)
    {
        if (!File.Exists(_logFile))
            return;

        // Read all entries, filter, and rewrite
        var entries = await LoadLogAsync(ct);
        var remaining = entries.Where(e => e.Index < fromIndex).ToList();

        lock (_lock)
        {
            // Rewrite log file
            using var stream = new FileStream(_logFile, FileMode.Create, FileAccess.Write, FileShare.None);
            using var writer = new BinaryWriter(stream);

            foreach (var entry in remaining)
            {
                WriteLogEntry(writer, entry);
            }

            stream.Flush();
        }

        LogLogTruncated(fromIndex, entries.Count - remaining.Count);
    }

    /// <summary>
    /// Save a snapshot to disk.
    /// </summary>
    public async Task SaveSnapshotAsync(RaftSnapshot snapshot, CancellationToken ct)
    {
        EnsureSnapshotDirectoryExists();

        var filename = GetSnapshotFilename(snapshot.EndOffset, snapshot.Epoch);
        var filepath = Path.Combine(_snapshotDirectory, filename);
        var tempFile = filepath + ".tmp";

        // Write snapshot: [endOffset:8][epoch:4][dataLength:4][data:N]
        await using (var stream = new FileStream(tempFile, FileMode.Create, FileAccess.Write, FileShare.None))
        await using (var writer = new BinaryWriter(stream))
        {
            writer.Write(snapshot.EndOffset);
            writer.Write(snapshot.Epoch);
            writer.Write(snapshot.Data.Length);
            writer.Write(snapshot.Data);
            await stream.FlushAsync(ct);
        }

        File.Move(tempFile, filepath, overwrite: true);
        LogSnapshotSaved(snapshot.EndOffset, snapshot.Epoch, snapshot.Data.Length);
    }

    /// <summary>
    /// Load a snapshot from disk by endOffset and epoch.
    /// </summary>
    public async Task<RaftSnapshot?> LoadSnapshotAsync(long endOffset, int epoch, CancellationToken ct)
    {
        var filename = GetSnapshotFilename(endOffset, epoch);
        var filepath = Path.Combine(_snapshotDirectory, filename);

        if (!File.Exists(filepath))
        {
            return null;
        }

        try
        {
            await using var stream = new FileStream(filepath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(stream);

            var readEndOffset = reader.ReadInt64();
            var readEpoch = reader.ReadInt32();
            var dataLength = reader.ReadInt32();
            var data = reader.ReadBytes(dataLength);

            LogSnapshotLoaded(readEndOffset, readEpoch, dataLength);

            return new RaftSnapshot(readEndOffset, readEpoch, data);
        }
        catch (Exception ex)
        {
            LogSnapshotLoadError(endOffset, epoch, ex);
            return null;
        }
    }

    /// <summary>
    /// Get the latest snapshot info if one exists.
    /// </summary>
    public RaftSnapshot? GetLatestSnapshotInfo()
    {
        if (!Directory.Exists(_snapshotDirectory))
            return null;

        var files = Directory.GetFiles(_snapshotDirectory, "snapshot-*.bin")
            .OrderByDescending(f => f)
            .FirstOrDefault();

        if (files == null)
            return null;

        try
        {
            using var stream = new FileStream(files, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(stream);

            var endOffset = reader.ReadInt64();
            var epoch = reader.ReadInt32();
            var dataLength = reader.ReadInt32();
            var data = reader.ReadBytes(dataLength);

            return new RaftSnapshot(endOffset, epoch, data);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Delete old snapshots keeping only the latest N.
    /// </summary>
    public void CleanupOldSnapshots(int keepCount = 3)
    {
        if (!Directory.Exists(_snapshotDirectory))
            return;

        var files = Directory.GetFiles(_snapshotDirectory, "snapshot-*.bin")
            .OrderByDescending(f => f)
            .Skip(keepCount)
            .ToList();

        foreach (var file in files)
        {
            try
            {
                File.Delete(file);
                LogSnapshotDeleted(file);
            }
            catch (Exception ex)
            {
                LogSnapshotDeleteError(file, ex);
            }
        }
    }

    private void EnsureSnapshotDirectoryExists()
    {
        if (!Directory.Exists(_snapshotDirectory))
        {
            Directory.CreateDirectory(_snapshotDirectory);
        }
    }

    private static string GetSnapshotFilename(long endOffset, int epoch)
    {
        return $"snapshot-{endOffset:D20}-{epoch:D10}.bin";
    }

    private static RaftLogEntry? ReadLogEntry(BinaryReader reader)
    {
        try
        {
            var term = reader.ReadInt32();
            var index = reader.ReadInt64();
            var commandType = (MetadataCommandType)reader.ReadInt32();
            var timestamp = reader.ReadInt64();
            var dataLength = reader.ReadInt32();
            var data = reader.ReadBytes(dataLength);

            return new RaftLogEntry
            {
                Term = term,
                Index = index,
                CommandType = commandType,
                Timestamp = timestamp,
                Data = data
            };
        }
        catch
        {
            return null;
        }
    }

    private static void WriteLogEntry(BinaryWriter writer, RaftLogEntry entry)
    {
        writer.Write(entry.Term);
        writer.Write(entry.Index);
        writer.Write((int)entry.CommandType);
        writer.Write(entry.Timestamp);
        writer.Write(entry.Data.Length);
        writer.Write(entry.Data);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Created Raft data directory: {Directory}")]
    private partial void LogDirectoryCreated(string directory);

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded Raft state: term={Term}, votedFor={VotedFor}")]
    private partial void LogStateLoaded(int term, int? votedFor);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error loading Raft state")]
    private partial void LogStateLoadError(Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Saved Raft state: term={Term}, votedFor={VotedFor}")]
    private partial void LogStateSaved(int term, int? votedFor);

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded {Count} Raft log entries")]
    private partial void LogEntriesLoaded(int count);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error loading Raft log")]
    private partial void LogLogLoadError(Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Truncated log from index {FromIndex}, removed {Count} entries")]
    private partial void LogLogTruncated(long fromIndex, int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Saved snapshot: endOffset={EndOffset}, epoch={Epoch}, size={Size} bytes")]
    private partial void LogSnapshotSaved(long endOffset, int epoch, int size);

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded snapshot: endOffset={EndOffset}, epoch={Epoch}, size={Size} bytes")]
    private partial void LogSnapshotLoaded(long endOffset, int epoch, int size);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error loading snapshot: endOffset={EndOffset}, epoch={Epoch}")]
    private partial void LogSnapshotLoadError(long endOffset, int epoch, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Deleted old snapshot: {Filename}")]
    private partial void LogSnapshotDeleted(string filename);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error deleting snapshot: {Filename}")]
    private partial void LogSnapshotDeleteError(string filename, Exception ex);
}

/// <summary>
/// Persistent state for Raft protocol.
/// </summary>
public sealed record RaftPersistentState(int CurrentTerm, int? VotedFor);

/// <summary>
/// Raft snapshot containing state machine state at a specific log position.
/// </summary>
public sealed record RaftSnapshot(long EndOffset, int Epoch, byte[] Data);
