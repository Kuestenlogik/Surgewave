using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Persistent windowed store — the same in-memory cache as <see cref="InMemoryWindowStore{TKey,TValue}"/>
/// plus a write-ahead log on disk for fast recovery. On <see cref="Init"/> the log is
/// replayed into the in-memory cache; every <see cref="InMemoryBackedWindowStore{TKey,TValue}.Put"/>
/// is appended to the log so the cache survives an application restart.
///
/// <para>
/// All the query-side logic (Fetch, FetchAll, retention sweep) lives in the base class.
/// This subclass only adds the durability hook: <see cref="OnPut"/> writes to the log
/// and <see cref="Init"/> replays it.
/// </para>
/// </summary>
public sealed class PersistentWindowStore<TKey, TValue> : InMemoryBackedWindowStore<TKey, TValue>
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private string _stateDir = "";
    private string _logPath = "";

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage", "CA2213:Disposable fields should be disposed",
        Justification = "Disposed in Close which is called via the base class Dispose pattern.")]
    private StreamWriter? _logWriter;

    private readonly object _lock = new();

    /// <inheritdoc />
    public override bool Persistent => true;

    /// <summary>
    /// Creates a new persistent window store. Window and retention settings are forwarded
    /// to the base class; the serdes are used by the write-ahead log for encoding keys and
    /// values on disk.
    /// </summary>
    public PersistentWindowStore(
        string name,
        TimeSpan windowSize,
        TimeSpan retentionPeriod,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde)
        : base(name, windowSize, retentionPeriod)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
    }

    /// <inheritdoc />
    public override void Init(ProcessorContext context)
    {
        base.Init(context);

        _stateDir = Path.Combine(context.Config.StateDir, context.ApplicationId, context.TaskId ?? "default", Name);
        Directory.CreateDirectory(_stateDir);
        _logPath = Path.Combine(_stateDir, "window-log");

        ReplayLog();
        _logWriter = new StreamWriter(new FileStream(_logPath, FileMode.Append, FileAccess.Write, FileShare.Read));
    }

    /// <inheritdoc />
    protected override void OnPut(TKey key, TValue value, long windowStartMs)
    {
        lock (_lock)
        {
            if (_logWriter == null) return;
            var keyBytes = Convert.ToBase64String(_keySerde.Serialize(key));
            var valueBytes = Convert.ToBase64String(_valueSerde.Serialize(value));
            _logWriter.WriteLine($"{keyBytes}|{windowStartMs}|{valueBytes}");
        }
    }

    private void ReplayLog()
    {
        if (!File.Exists(_logPath)) return;

        try
        {
            using var reader = new StreamReader(_logPath);
            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                if (string.IsNullOrEmpty(line)) continue;

                var parts = line.Split('|');
                if (parts.Length != 3) continue;

                var keyBytes = Convert.FromBase64String(parts[0]);
                var windowStart = long.Parse(parts[1]);
                var valueBytes = Convert.FromBase64String(parts[2]);

                var key = _keySerde.Deserialize(keyBytes);
                var value = _valueSerde.Deserialize(valueBytes);

                // Populate the base-class cache directly — bypasses OnPut so we don't
                // re-log entries we just replayed from the log.
                Store[(key, windowStart)] = value;
            }
        }
        catch (Exception ex)
        {
            Context?.Logger.LogWarning(ex, "Failed to replay window log for store {Name}", Name);
        }
    }

    /// <inheritdoc />
    public override void Flush()
    {
        lock (_lock)
        {
            _logWriter?.Flush();
        }
    }

    /// <inheritdoc />
    public override void Close()
    {
        lock (_lock)
        {
            _logWriter?.Dispose();
            _logWriter = null;
        }
        base.Close();
    }
}
