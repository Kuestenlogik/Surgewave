using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// #72 Inc4 — node-local high-water persistence for the controller epoch (a small
/// <c>controller.epoch</c> file next to <c>cluster.id</c>). The hosts wire it into
/// <see cref="ClusterState.OnControllerEpochAdvanced"/> and prime
/// <see cref="ClusterState.PrimeControllerEpochFloor"/> at boot, so a RESTARTED broker elects (and
/// mints composed broker epochs) strictly above every reign it already observed — without this, a
/// restarted legacy-mode controller starts back at epoch 0 and re-mints duplicate broker epochs.
/// <para>
/// Best-effort, node-local durability: a crash between the in-memory advance and the file write can
/// lose the LAST advance (the restart then re-elects AT the lost epoch rather than above it), and it
/// protects only restarts of THIS node — a quiet-reign failover onto a broker that never observed a
/// push remains a documented residual. Raft mode gets true replicated durability in #72 Inc5.
/// </para>
/// </summary>
public sealed partial class ControllerEpochStore
{
    private const string FileName = "controller.epoch";

    private readonly string _path;
    private readonly ILogger _logger;

    // Serializes Save and gates it against the highest epoch we have committed to persisting.
    private readonly Lock _writeLock = new();
    private int _highWater;

    public ControllerEpochStore(string dataDirectory, ILogger logger)
    {
        _path = Path.Combine(dataDirectory, FileName);
        _logger = logger;
    }

    /// <summary>The persisted high-water epoch, or 0 when the file is absent or unreadable.</summary>
    public int Load()
    {
        try
        {
            if (!File.Exists(_path))
                return 0;

            var epoch = int.TryParse(File.ReadAllText(_path).Trim(), out var e) && e > 0 ? e : 0;

            // Seed the in-memory high-water so a subsequent Save can never regress the persisted floor
            // below what a restart just primed from.
            lock (_writeLock)
            {
                if (epoch > _highWater)
                    _highWater = epoch;
            }

            return epoch;
        }
        catch (Exception ex)
        {
            LogLoadFailed(_path, ex);
            return 0;
        }
    }

    /// <summary>
    /// Persist an advanced epoch as a monotone HIGH-WATER (best-effort; failures are logged, never
    /// thrown — the in-memory advance must not depend on disk health). The fire sites only guarantee
    /// the epoch exceeds the CURRENT in-memory controller epoch, which a <c>Clear()</c>/snapshot-restore
    /// resets to 0 and which racing election-vs-push advances can reorder — either could hand a value
    /// below the true maximum, so a blind overwrite would clobber the floor DOWNWARD and defeat the
    /// restart-monotonicity guarantee. The write is serialized (no temp-file collision between
    /// concurrent advances) and gated on a strict increase; written atomically via temp-file + move so
    /// a torn write cannot leave a corrupt value.
    /// </summary>
    public void Save(int epoch)
    {
        lock (_writeLock)
        {
            if (epoch <= _highWater)
                return;
            _highWater = epoch;

            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(_path)!);
                var temp = _path + ".tmp";
                File.WriteAllText(temp, epoch.ToString(System.Globalization.CultureInfo.InvariantCulture));
                File.Move(temp, _path, overwrite: true);
            }
            catch (Exception ex)
            {
                LogSaveFailed(_path, epoch, ex);
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to read controller-epoch high-water file {Path}; starting from epoch 0")]
    private partial void LogLoadFailed(string path, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to persist controller-epoch high-water {Epoch} to {Path}")]
    private partial void LogSaveFailed(string path, int epoch, Exception ex);
}
