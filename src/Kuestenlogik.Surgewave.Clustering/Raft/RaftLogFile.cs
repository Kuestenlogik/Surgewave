using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Util;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// The metadata log on disk: <c>raft-log.bin</c>, written in the order its writes are asked for and flushed to disk
/// before a write counts as done.
/// </summary>
/// <remarks>
/// <para>
/// The file starts with eight bytes that name the format and holds one frame per entry behind them:
/// <c>[length:4][crc32c:4][term:4][index:8][commandType:4][timestamp:8][data]</c>, little-endian, where the length
/// and the checksum are those of what follows them. The entry with index <c>n</c> is the <c>n</c>-th frame.
/// </para>
/// <para>
/// One writer, in the order of the calls: <see cref="Append"/> and <see cref="TruncateFrom"/> put their write in a
/// queue before they return, and the task they return completes when it is on disk. A caller that holds a lock while
/// it calls decides the order by that lock, and waits outside it. Writes that queue up while one is under way go to
/// disk together, with one flush: the flush is what a write costs, and it costs the same for one entry as for many.
/// </para>
/// <para>
/// A write that fails is the last one: every later write fails with the same error, since it would stand in the file
/// behind a gap.
/// </para>
/// <para>
/// Reading tells a write the crash tore from a damaged file by where the damage is. A write is flushed before it
/// counts, so what a crash can tear is the end of the file: a frame that ends at the end of the file or past it, or
/// a tail of zeros a file system leaves where the file had grown and the bytes had not arrived. That is cut off, and
/// nothing is lost that anyone was told was written. A frame that fails its checksum with frames behind it was
/// flushed and has changed since; it is not skipped, because the entries behind it would then stand at the wrong
/// index, and the file is refused. A damaged length that points past the end of the file cannot be told from a torn
/// write and is cut off as one.
/// </para>
/// </remarks>
internal sealed partial class RaftLogFile
{
    private const int FrameHeaderSize = 8;
    private const int EntryHeaderSize = 24;

    private readonly ILogger _logger;
    private readonly string _path;
    private readonly Action<FileStream> _flushToDisk;

    // The file and what is known of it. Null until the file was read: where each entry ends, by its position.
    private readonly Lock _fileLock = new();
    private List<long>? _entryEnds;

    private readonly Lock _queueLock = new();
    private List<Write> _queue = [];
    private bool _writing;
    private Exception? _failure;
    private Task _lastWrite = Task.CompletedTask;

    public RaftLogFile(ILogger logger, string path, Action<FileStream> flushToDisk)
    {
        _logger = logger;
        _path = path;
        _flushToDisk = flushToDisk;
    }

    private static ReadOnlySpan<byte> Format => "SWRLOG01"u8;

    /// <summary>
    /// Completes when every write asked for so far is on disk, and fails if one of them failed.
    /// </summary>
    public Task Written
    {
        get
        {
            lock (_queueLock)
            {
                return _lastWrite;
            }
        }
    }

    /// <summary>
    /// Reads the log. A torn end is cut off; a file that is damaged elsewhere, or is not a log of this format, is an
    /// <see cref="InvalidDataException"/>.
    /// </summary>
    public List<RaftLogEntry> Load()
    {
        lock (_fileLock)
        {
            return Read();
        }
    }

    /// <summary>
    /// Appends <paramref name="entries"/> behind the writes asked for before. The task completes when they are on disk.
    /// </summary>
    public Task Append(RaftLogEntry[] entries) => Enqueue(new Write(entries, TruncateFrom: 0));

    /// <summary>
    /// Removes the entry with <paramref name="index"/> and all behind it. The task completes when that is on disk.
    /// </summary>
    public Task TruncateFrom(long index) => Enqueue(new Write([], index));

    private Task Enqueue(Write write)
    {
        lock (_queueLock)
        {
            if (_failure is not null)
                return _lastWrite;

            _queue.Add(write);
            _lastWrite = write.Done.Task;

            if (!_writing)
            {
                _writing = true;
                _ = Task.Run(WriteQueued);
            }

            return write.Done.Task;
        }
    }

    private void WriteQueued()
    {
        while (true)
        {
            List<Write> writes;
            lock (_queueLock)
            {
                if (_queue.Count == 0)
                {
                    _writing = false;
                    return;
                }

                writes = _queue;
                _queue = [];
            }

            try
            {
                lock (_fileLock)
                {
                    WriteToDisk(writes);
                }
            }
            catch (Exception ex)
            {
                LogWriteFailed(_path, ex);

                lock (_queueLock)
                {
                    // Whatever queued up meanwhile fails with it, and so does whatever is asked for from here on.
                    _failure = ex;
                    writes.AddRange(_queue);
                    _queue = [];
                    _writing = false;
                    _lastWrite = Task.FromException(ex);
                    Observe(_lastWrite);
                }

                foreach (var write in writes)
                {
                    write.Done.TrySetException(ex);
                    Observe(write.Done.Task);
                }

                return;
            }

            foreach (var write in writes)
            {
                write.Done.TrySetResult();
            }
        }
    }

    // A caller that asks for two writes waits for the second; the first one's failure is the same one.
    private static void Observe(Task failed) => _ = failed.Exception;

    private void WriteToDisk(List<Write> writes)
    {
        var entryEnds = _entryEnds;
        if (entryEnds is null)
        {
            Read();
            entryEnds = _entryEnds!;
        }

        try
        {
            using var file = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            var created = file.Length == 0;
            if (created)
                file.Write(Format);

            var end = entryEnds.Count > 0 ? entryEnds[^1] : Format.Length;
            file.Position = end;

            foreach (var write in writes)
            {
                if (write.TruncateFrom > 0)
                {
                    var kept = (int)Math.Min(write.TruncateFrom - 1, entryEnds.Count);
                    if (kept == entryEnds.Count)
                        continue;

                    entryEnds.RemoveRange(kept, entryEnds.Count - kept);
                    end = kept > 0 ? entryEnds[kept - 1] : Format.Length;
                    file.SetLength(end);
                    file.Position = end;
                    continue;
                }

                foreach (var entry in write.Entries)
                {
                    if (entry.Index != entryEnds.Count + 1)
                    {
                        throw new InvalidOperationException(
                            $"The entry with index {entry.Index} cannot be written to '{_path}' behind the entry with index {entryEnds.Count}.");
                    }

                    var frame = Frame(entry);
                    file.Write(frame);
                    end += frame.Length;
                    entryEnds.Add(end);
                }
            }

            _flushToDisk(file);

            if (created)
                DurableFile.SyncDirectory(Path.GetDirectoryName(Path.GetFullPath(_path))!);
        }
        catch
        {
            // What the file holds now is not known.
            _entryEnds = null;
            throw;
        }
    }

    private static byte[] Frame(RaftLogEntry entry)
    {
        var length = EntryHeaderSize + entry.Data.Length;
        var frame = new byte[FrameHeaderSize + length];
        var payload = frame.AsSpan(FrameHeaderSize);

        BinaryPrimitives.WriteInt32LittleEndian(payload, entry.Term);
        BinaryPrimitives.WriteInt64LittleEndian(payload[4..], entry.Index);
        BinaryPrimitives.WriteInt32LittleEndian(payload[12..], (int)entry.CommandType);
        BinaryPrimitives.WriteInt64LittleEndian(payload[16..], entry.Timestamp);
        entry.Data.CopyTo(payload[EntryHeaderSize..]);

        BinaryPrimitives.WriteInt32LittleEndian(frame, length);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(4), Crc32C.Compute((ReadOnlySpan<byte>)payload));
        return frame;
    }

    // Under _fileLock.
    private List<RaftLogEntry> Read()
    {
        var entries = new List<RaftLogEntry>();
        var entryEnds = new List<long>();

        if (!File.Exists(_path))
        {
            _entryEnds = entryEnds;
            return entries;
        }

        ReadOnlySpan<byte> bytes = File.ReadAllBytes(_path);

        if (bytes.Length < Format.Length)
        {
            // Nothing, or the start of the first write: the file was created and the crash came before its first flush.
            if (!Format.StartsWith(bytes))
                throw NotThisFormat();

            if (bytes.Length > 0)
                CutOff(0, bytes.Length);

            _entryEnds = entryEnds;
            return entries;
        }

        if (!bytes[..Format.Length].SequenceEqual(Format))
            throw NotThisFormat();

        var position = Format.Length;
        while (position < bytes.Length)
        {
            var rest = bytes[position..];
            var torn = rest.Length < FrameHeaderSize;

            if (!torn)
            {
                var length = BinaryPrimitives.ReadInt32LittleEndian(rest);
                var end = (long)FrameHeaderSize + length;

                if (length >= EntryHeaderSize && end <= rest.Length)
                {
                    var payload = rest.Slice(FrameHeaderSize, length);
                    if (Crc32C.Compute(payload) == BinaryPrimitives.ReadUInt32LittleEndian(rest[4..]))
                    {
                        var entry = new RaftLogEntry
                        {
                            Term = BinaryPrimitives.ReadInt32LittleEndian(payload),
                            Index = BinaryPrimitives.ReadInt64LittleEndian(payload[4..]),
                            CommandType = (MetadataCommandType)BinaryPrimitives.ReadInt32LittleEndian(payload[12..]),
                            Timestamp = BinaryPrimitives.ReadInt64LittleEndian(payload[16..]),
                            Data = payload[EntryHeaderSize..].ToArray(),
                        };

                        if (entry.Index != entries.Count + 1)
                        {
                            throw new InvalidDataException(
                                $"The metadata log '{_path}' holds the entry with index {entry.Index} at position {entries.Count + 1}, " +
                                $"at byte {position}. It is not read past that, and the node does not start on it.");
                        }

                        entries.Add(entry);
                        position += (int)end;
                        entryEnds.Add(position);
                        continue;
                    }
                }

                // Not an entry. Torn, if the write could still have been under way: it ends where the file ends, or
                // would end past it, or the file holds nothing but zeros from here on.
                torn = (length >= EntryHeaderSize && end >= rest.Length) || rest.IndexOfAnyExcept((byte)0) < 0;
            }

            if (!torn)
            {
                throw new InvalidDataException(
                    $"The metadata log '{_path}' is damaged at byte {position}, behind {entries.Count} readable entries and with " +
                    $"{bytes.Length - position} bytes from there on. The entries behind the damage cannot be placed, so the log is " +
                    "not read past it, and the node does not start on it.");
            }

            CutOff(position, bytes.Length - position);
            break;
        }

        _entryEnds = entryEnds;
        LogLoaded(entries.Count, _path);
        return entries;
    }

    private void CutOff(long length, long tornBytes)
    {
        using (var file = new FileStream(_path, FileMode.Open, FileAccess.Write, FileShare.Read))
        {
            file.SetLength(length);
            _flushToDisk(file);
        }

        LogTornEndCutOff(_path, tornBytes, length);
    }

    private InvalidDataException NotThisFormat() => new(
        $"'{_path}' is not a metadata log this version reads: it does not start with the format's name. A log written " +
        "by a version before 0.7.0 has no such start and is not read.");

    private sealed record Write(RaftLogEntry[] Entries, long TruncateFrom)
    {
        public TaskCompletionSource Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded {Count} Raft log entries from {Path}")]
    private partial void LogLoaded(int count, string path);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "The metadata log {Path} ended in a write that was not completed: {TornBytes} bytes were cut off, and the log ends at byte {Length}")]
    private partial void LogTornEndCutOff(string path, long tornBytes, long length);

    [LoggerMessage(Level = LogLevel.Error,
        Message = "Writing the metadata log {Path} failed. Nothing more is written to it: a later entry would stand behind a gap")]
    private partial void LogWriteFailed(string path, Exception ex);
}
