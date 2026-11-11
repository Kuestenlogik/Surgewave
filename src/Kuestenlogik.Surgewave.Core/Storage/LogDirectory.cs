using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// A log directory: where the partition logs of a broker live, with a stable id and whether it is still online (#162).
/// </summary>
/// <remarks>
/// <para>
/// Kafka's <c>LogDirFailureChannel</c> in one object. An I/O failure that a partition log cannot recover from — a
/// segment that cannot be written, read, rolled, truncated or opened — takes the whole directory offline, because the
/// failure is the disk's, not the partition's: every log in the directory answers <see cref="LogDirOfflineException"/>
/// from then on. Offline is final for the life of the process; Kafka brings a failed directory back only with a
/// restart, and so does this.
/// </para>
/// <para>
/// The id is what the broker reports in its heartbeat (<c>OfflineLogDirs</c>, KIP-858), so it must survive a restart:
/// a persistent directory keeps it in <c>meta.properties</c> under <c>directory.id</c>, as Kafka does. A directory of a
/// non-persistent storage engine has nothing to keep it in and gets a new one per process.
/// </para>
/// </remarks>
public sealed partial class LogDirectory
{
    /// <summary>The file a persistent log directory keeps its id in.</summary>
    public const string MetaPropertiesFileName = "meta.properties";

    private const string DirectoryIdKey = "directory.id";

    private readonly TaskCompletionSource _offline = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ILogger? _logger;
    private int _isOffline;

    public LogDirectory(string path, Guid id, ILogger? logger = null)
    {
        Path = path;
        Id = id;
        _logger = logger;
    }

    /// <summary>The directory's path.</summary>
    public string Path { get; }

    /// <summary>The directory's stable id.</summary>
    public Guid Id { get; }

    /// <summary>Whether the directory has failed.</summary>
    public bool IsOffline => Volatile.Read(ref _isOffline) != 0;

    /// <summary>Completes when the directory goes offline.</summary>
    public Task Offline => _offline.Task;

    /// <summary>Raised once, when the directory goes offline.</summary>
    public event Action<LogDirectory>? WentOffline;

    /// <summary>
    /// Opens the log directory at <paramref name="path"/>: a persistent one with the id in its
    /// <see cref="MetaPropertiesFileName"/>, written there on first use; a non-persistent one with a new id.
    /// </summary>
    public static LogDirectory Open(string path, bool persistent, ILogger? logger = null)
    {
        if (!persistent)
            return new LogDirectory(path, Guid.NewGuid(), logger);

        var file = System.IO.Path.Combine(path, MetaPropertiesFileName);
        if (File.Exists(file))
        {
            foreach (var line in File.ReadAllLines(file))
            {
                var separator = line.IndexOf('=', StringComparison.Ordinal);
                if (separator <= 0 || line.StartsWith('#'))
                    continue;
                if (line.AsSpan(0, separator).Trim().SequenceEqual(DirectoryIdKey)
                    && Guid.TryParse(line.AsSpan(separator + 1).Trim(), out var stored))
                {
                    return new LogDirectory(path, stored, logger);
                }
            }
        }

        var id = Guid.NewGuid();
        Directory.CreateDirectory(path);
        var temp = file + ".tmp";
        File.WriteAllText(temp, $"version=1{Environment.NewLine}{DirectoryIdKey}={id}{Environment.NewLine}");
        File.Move(temp, file, overwrite: true);
        return new LogDirectory(path, id, logger);
    }

    /// <summary>Throws <see cref="LogDirOfflineException"/> when the directory is offline.</summary>
    public void ThrowIfOffline(TopicPartition partition)
    {
        if (IsOffline)
            throw new LogDirOfflineException($"Log directory {Path} ({Id}) of {partition} is offline") { DirectoryId = Id };
    }

    /// <summary>
    /// Takes the directory offline after <paramref name="cause"/> failed <paramref name="operation"/> on
    /// <paramref name="partition"/>, and returns the exception to throw in its place.
    /// </summary>
    public LogDirOfflineException Fail(string operation, TopicPartition partition, Exception cause)
    {
        if (Interlocked.Exchange(ref _isOffline, 1) == 0)
        {
            LogWentOffline(cause, Path, Id, operation, partition.Topic, partition.Partition);
            try
            {
                WentOffline?.Invoke(this);
            }
            catch (Exception handlerFailure)
            {
                // A subscriber that throws must not keep the others, or the waiters, from learning it.
                LogHandlerFailed(handlerFailure, Path);
            }
            _offline.TrySetResult();
        }

        return new LogDirOfflineException(
            $"Log directory {Path} ({Id}) went offline: {operation} {partition} failed: {cause.Message}", cause)
        {
            DirectoryId = Id,
        };
    }

    /// <summary>Whether <paramref name="exception"/> is an I/O failure this directory has not answered yet.</summary>
    public static bool IsStorageFailure(Exception exception)
        => exception is IOException and not LogDirOfflineException;

    private void LogWentOffline(Exception cause, string path, Guid id, string operation, string topic, int partition)
    {
        if (_logger is not null)
            LogWentOfflineCore(_logger, cause, path, id, operation, topic, partition);
    }

    private void LogHandlerFailed(Exception failure, string path)
    {
        if (_logger is not null)
            LogHandlerFailedCore(_logger, failure, path);
    }

    [LoggerMessage(Level = LogLevel.Error,
        Message = "Log directory {Path} ({DirectoryId}) is offline: {Operation} {Topic}-{Partition} failed")]
    private static partial void LogWentOfflineCore(
        ILogger logger, Exception cause, string path, Guid directoryId, string operation, string topic, int partition);

    [LoggerMessage(Level = LogLevel.Error, Message = "A handler of the failure of log directory {Path} failed")]
    private static partial void LogHandlerFailedCore(ILogger logger, Exception failure, string path);
}
