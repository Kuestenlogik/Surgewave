using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.GroupStatePersistence;

/// <summary>
/// JSON-file-per-group persistence: each group lives in
/// <c>{baseDir}/{groupId}.json</c>. Saves are debounced through a periodic
/// flush timer so a hot coordinator doesn't fsync per heartbeat. Mirrors the
/// pattern <see cref="Kuestenlogik.Surgewave.Broker.OffsetStore"/> already uses for classic
/// consumer-group offsets, so operators see one consistent on-disk layout.
/// </summary>
public sealed class JsonFileGroupStateStore<TState> : IGroupStateStore<TState>, IDisposable
    where TState : class
{
    private readonly string _directory;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<string, TState> _pending = new(StringComparer.Ordinal);
    private readonly Timer _flushTimer;
    private readonly TimeSpan _flushInterval;
    private readonly JsonSerializerOptions _jsonOptions = new() { WriteIndented = false };
    private bool _disposed;

    public JsonFileGroupStateStore(string baseDirectory, string subfolder, ILogger logger, int flushIntervalMs = 1000)
    {
        _directory = Path.Combine(baseDirectory, ".metadata", subfolder);
        _logger = logger;
        _flushInterval = TimeSpan.FromMilliseconds(flushIntervalMs);
        Directory.CreateDirectory(_directory);
        _flushTimer = new Timer(_ => Flush(), null, _flushInterval, _flushInterval);
    }

    public void Save(string groupId, TState state)
    {
        if (_disposed) return;
        _pending[groupId] = state;
    }

    public void Delete(string groupId)
    {
        _pending.TryRemove(groupId, out _);
        var path = PathFor(groupId);
        try
        {
            if (File.Exists(path)) File.Delete(path);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "JsonFileGroupStateStore: failed to delete {Path}", path);
        }
    }

    public IReadOnlyDictionary<string, TState> LoadAll()
    {
        var result = new Dictionary<string, TState>(StringComparer.Ordinal);
        if (!Directory.Exists(_directory)) return result;

        foreach (var path in Directory.GetFiles(_directory, "*.json"))
        {
            var groupId = Path.GetFileNameWithoutExtension(path);
            try
            {
                using var stream = File.OpenRead(path);
                var state = JsonSerializer.Deserialize<TState>(stream, _jsonOptions);
                if (state is not null) result[groupId] = state;
            }
            catch (Exception ex)
            {
                // A corrupt file must not stop the broker from starting — skip and warn.
                _logger.LogWarning(ex, "JsonFileGroupStateStore: skipping corrupt state file {Path}", path);
            }
        }
        return result;
    }

    private void Flush()
    {
        // Note: this MUST proceed even when _disposed is true so Dispose can drain
        // the pending queue before exiting. The check is on _pending instead.
        if (_pending.IsEmpty) return;

        // Snapshot the pending set to a list so coordinator threads can keep enqueueing
        // while we serialise to disk.
        var batch = _pending.ToArray();
        _pending.Clear();

        foreach (var (groupId, state) in batch)
        {
            var path = PathFor(groupId);
            var tmp = path + ".tmp";
            try
            {
                using (var stream = File.Create(tmp))
                {
                    JsonSerializer.Serialize(stream, state, _jsonOptions);
                }
                File.Move(tmp, path, overwrite: true);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "JsonFileGroupStateStore: failed to persist {GroupId} to {Path}", groupId, path);
                try { File.Delete(tmp); } catch { /* best-effort */ }
            }
        }
    }

    private string PathFor(string groupId)
    {
        // Group IDs may legitimately contain characters that are invalid in file names
        // (slashes for namespacing, colons, etc.). Encode them with a stable scheme so
        // round-tripping the file name back to a group id stays unambiguous.
        var safe = string.Concat(groupId.Select(c =>
            char.IsLetterOrDigit(c) || c == '-' || c == '_' ? c.ToString() : $"%{(int)c:X2}"));
        return Path.Combine(_directory, safe + ".json");
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _flushTimer.Dispose();
        Flush();
    }
}
