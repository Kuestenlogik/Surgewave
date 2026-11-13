using System.Buffers.Binary;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Persists Raft state (currentTerm, votedFor, log, snapshots) to disk.
/// </summary>
/// <remarks>
/// What this says is saved is on disk, not in the operating system's cache: a vote is answered and an entry counts
/// for a commit on the strength of it, and a node that comes back from a power loss without the vote it cast or the
/// entry it acknowledged breaks what Raft rests on (#260). What it loads is what was saved or an error: a state
/// file, a log or a snapshot that cannot be read is not taken for one that was never written (#261).
/// </remarks>
public sealed partial class RaftPersistence
{
    private const int SnapshotHeaderSize = 16;

    private readonly ILogger<RaftPersistence> _logger;
    private readonly string _dataDirectory;
    private readonly string _stateFile;
    private readonly string _snapshotDirectory;
    private readonly RaftLogFile _log;
    private readonly Action<FileStream> _flushToDisk;

    // Term and vote are saved from several paths (own election, granting a vote, following a new leader),
    // concurrently. Unserialised, two saves wrote the same .tmp file at once ("being used by another
    // process", #184), and an older save could land after a newer one and take the term back.
    private readonly Lock _stateLock = new();
    private int _savedTerm = -1;
    private int? _savedVotedFor;

    public RaftPersistence(ILogger<RaftPersistence> logger, ClusteringConfig config)
        : this(logger, config, DurableFile.FlushToDisk)
    {
    }

    /// <summary>
    /// As the public constructor, with the flush to disk handed in — test-only (via InternalsVisibleTo), so a test
    /// can hold a write at the point where it is written and not yet durable.
    /// </summary>
    internal RaftPersistence(ILogger<RaftPersistence> logger, ClusteringConfig config, Action<FileStream> flushToDisk)
    {
        _logger = logger;
        _flushToDisk = flushToDisk;
        _dataDirectory = config.ResolvedRaftDataDirectory;
        _stateFile = Path.Combine(_dataDirectory, "raft-state.json");
        _snapshotDirectory = Path.Combine(_dataDirectory, "snapshots");
        _log = new RaftLogFile(logger, Path.Combine(_dataDirectory, "raft-log.bin"), flushToDisk);
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
    /// <remarks>
    /// No state file is the state of a node that never ran. One that is there and cannot be read is an
    /// <see cref="InvalidDataException"/>: it used to be answered with term 0 and no vote, and a node that starts on
    /// that votes again in a term it has voted in.
    /// </remarks>
    public async Task<RaftPersistentState> LoadStateAsync(CancellationToken ct)
    {
        EnsureDirectoryExists();

        if (!File.Exists(_stateFile))
        {
            return new RaftPersistentState(0, null);
        }

        var json = await File.ReadAllTextAsync(_stateFile, ct).ConfigureAwait(false);

        RaftPersistentState? state;
        try
        {
            state = JsonSerializer.Deserialize(json, ClusteringJsonContext.Default.RaftPersistentState);
        }
        catch (JsonException ex)
        {
            throw UnreadableState(ex);
        }

        if (state is null)
            throw UnreadableState(null);

        lock (_stateLock)
        {
            _savedTerm = state.CurrentTerm;
            _savedVotedFor = state.VotedFor;
        }

        LogStateLoaded(state.CurrentTerm, state.VotedFor);
        return state;
    }

    private InvalidDataException UnreadableState(Exception? inner) => new(
        $"The Raft state file '{_stateFile}' cannot be read. It holds the term this node is in and the vote it cast in it; " +
        "starting without them would let the node vote a second time in that term, so it does not start.",
        inner);

    /// <summary>
    /// Save persistent state (currentTerm, votedFor). On disk when this returns.
    /// </summary>
    /// <remarks>
    /// One save at a time, and never backwards: a state older than the one on disk — a lower term, or the same
    /// term without the vote already cast in it — is skipped. The file is a few bytes, so it is written
    /// synchronously under the lock rather than holding an async lock across the write.
    /// </remarks>
    public Task SaveStateAsync(int currentTerm, int? votedFor, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        EnsureDirectoryExists();

        lock (_stateLock)
        {
            if (currentTerm < _savedTerm || (currentTerm == _savedTerm && votedFor is null && _savedVotedFor is not null))
                return Task.CompletedTask;

            var state = new RaftPersistentState(currentTerm, votedFor);
            var json = JsonSerializer.SerializeToUtf8Bytes(state, ClusteringJsonContext.Default.RaftPersistentState);

            // Written beside the state file and put in its place, so that a crash leaves the old state or the new
            // one; flushed before and written through after, so that a power loss does too.
            var tempFile = _stateFile + ".tmp";
            using (var file = new FileStream(tempFile, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                file.Write(json);
                _flushToDisk(file);
            }

            DurableFile.Replace(tempFile, _stateFile);

            _savedTerm = currentTerm;
            _savedVotedFor = votedFor;
        }

        LogStateSaved(currentTerm, votedFor);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Load log entries from disk.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A log that cannot be opened or read is an error, and the caller's: it used to be logged and answered with no
    /// entries, which a node takes for the log of a broker that never ran. That node then led a new cluster — a new
    /// cluster id, none of the topics — and appended its entries, numbered from 1 again, behind the ones it could
    /// not read.
    /// </para>
    /// <para>
    /// A write the last run did not complete is cut off the end of the file. A log that is damaged elsewhere is an
    /// <see cref="InvalidDataException"/>; <see cref="RaftLogFile"/> says what tells the two apart.
    /// </para>
    /// </remarks>
    public Task<List<RaftLogEntry>> LoadLogAsync(CancellationToken ct)
    {
        EnsureDirectoryExists();
        return Task.Run(_log.Load, ct);
    }

    /// <summary>
    /// Appends entries to the log file, behind every write asked for before this call. The task completes when
    /// they are on disk.
    /// </summary>
    /// <remarks>
    /// The write is queued before this returns, so a caller that calls under a lock has its writes on disk in the
    /// order it took the lock in, and waits for the task outside it (#262). Not cancellable: the caller already
    /// holds the entries in its own log.
    /// </remarks>
    public Task AppendEntriesAsync(IEnumerable<RaftLogEntry> entries)
    {
        EnsureDirectoryExists();
        return _log.Append([.. entries]);
    }

    /// <summary>
    /// Removes the entry with <paramref name="fromIndex"/> and all following ones from the log file, behind every
    /// write asked for before this call. The task completes when that is on disk.
    /// </summary>
    public Task TruncateLogAsync(long fromIndex)
    {
        EnsureDirectoryExists();
        return _log.TruncateFrom(fromIndex);
    }

    /// <summary>
    /// Completes when every log write asked for so far is on disk, and fails if one of them failed.
    /// </summary>
    public Task LogWritten => _log.Written;

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
        var header = new byte[SnapshotHeaderSize];
        BinaryPrimitives.WriteInt64LittleEndian(header, snapshot.EndOffset);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8), snapshot.Epoch);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(12), snapshot.Data.Length);

        await using (var stream = new FileStream(tempFile, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            await stream.WriteAsync(header, ct).ConfigureAwait(false);
            await stream.WriteAsync(snapshot.Data, ct).ConfigureAwait(false);
            _flushToDisk(stream);
        }

        DurableFile.Replace(tempFile, filepath);
        LogSnapshotSaved(snapshot.EndOffset, snapshot.Epoch, snapshot.Data.Length);
    }

    /// <summary>
    /// Load a snapshot from disk by endOffset and epoch. Null if there is none; an
    /// <see cref="InvalidDataException"/> if there is one that cannot be read.
    /// </summary>
    public async Task<RaftSnapshot?> LoadSnapshotAsync(long endOffset, int epoch, CancellationToken ct)
    {
        var filename = GetSnapshotFilename(endOffset, epoch);
        var filepath = Path.Combine(_snapshotDirectory, filename);

        if (!File.Exists(filepath))
        {
            return null;
        }

        var snapshot = ReadSnapshot(filepath, await File.ReadAllBytesAsync(filepath, ct).ConfigureAwait(false));
        LogSnapshotLoaded(snapshot.EndOffset, snapshot.Epoch, snapshot.Data.Length);
        return snapshot;
    }

    /// <summary>
    /// Get the latest snapshot if one exists. An <see cref="InvalidDataException"/> if it cannot be read.
    /// </summary>
    public RaftSnapshot? GetLatestSnapshotInfo()
    {
        if (!Directory.Exists(_snapshotDirectory))
            return null;

        var latest = Directory.GetFiles(_snapshotDirectory, "snapshot-*.bin")
            .OrderByDescending(f => f)
            .FirstOrDefault();

        return latest is null ? null : ReadSnapshot(latest, File.ReadAllBytes(latest));
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

    private static RaftSnapshot ReadSnapshot(string path, ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < SnapshotHeaderSize ||
            BinaryPrimitives.ReadInt32LittleEndian(bytes[12..]) != bytes.Length - SnapshotHeaderSize)
        {
            throw new InvalidDataException(
                $"The snapshot '{path}' cannot be read: its {bytes.Length} bytes are not the header and the data the header announces.");
        }

        return new RaftSnapshot(
            BinaryPrimitives.ReadInt64LittleEndian(bytes),
            BinaryPrimitives.ReadInt32LittleEndian(bytes[8..]),
            bytes[SnapshotHeaderSize..].ToArray());
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Created Raft data directory: {Directory}")]
    private partial void LogDirectoryCreated(string directory);

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded Raft state: term={Term}, votedFor={VotedFor}")]
    private partial void LogStateLoaded(int term, int? votedFor);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Saved Raft state: term={Term}, votedFor={VotedFor}")]
    private partial void LogStateSaved(int term, int? votedFor);

    [LoggerMessage(Level = LogLevel.Information, Message = "Saved snapshot: endOffset={EndOffset}, epoch={Epoch}, size={Size} bytes")]
    private partial void LogSnapshotSaved(long endOffset, int epoch, int size);

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded snapshot: endOffset={EndOffset}, epoch={Epoch}, size={Size} bytes")]
    private partial void LogSnapshotLoaded(long endOffset, int epoch, int size);

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
