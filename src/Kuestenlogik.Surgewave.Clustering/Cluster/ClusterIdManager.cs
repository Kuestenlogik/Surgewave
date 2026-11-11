using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// This broker's cluster id: the configured one, or the one it persisted in <c>cluster.id</c> when it joined its
/// cluster — Kafka's <c>meta.properties</c> (#203).
/// </summary>
/// <remarks>
/// <para>
/// A cluster has one id. A broker does not generate its own: the first controller of a cluster writes the id to the
/// metadata log (the configured one, or one it generates), and every broker adopts it from there and persists it.
/// It used to be generated and persisted per broker, so the brokers of one cluster had different ids and none of them
/// could report its own to clients.
/// </para>
/// <para>
/// The id is checked when the broker starts: a configured id that differs from the persisted one stops it. A broker
/// that carries an id other than its cluster's is refused at registration with INCONSISTENT_CLUSTER_ID, as Kafka's
/// controller refuses it.
/// </para>
/// </remarks>
public sealed partial class ClusterIdManager
{
    private const string ClusterIdFileName = "cluster.id";

    private readonly string _dataDirectory;
    private readonly ILogger<ClusterIdManager> _logger;
    private readonly Lock _lock = new();
    private readonly TaskCompletionSource<string> _known = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<string> _refused = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private string? _clusterId;
    private string? _candidate;
    private bool _persisted;

    public ClusterIdManager(ClusteringConfig config, ILogger<ClusterIdManager> logger)
        : this(config.ClusterId, config.DataDirectory, logger)
    {
    }

    /// <exception cref="InvalidOperationException">The configured id differs from the persisted one.</exception>
    public ClusterIdManager(string? configuredClusterId, string dataDirectory, ILogger<ClusterIdManager> logger)
    {
        _dataDirectory = dataDirectory;
        _logger = logger;

        var persisted = LoadPersistedClusterId();
        if (!string.IsNullOrEmpty(configuredClusterId))
        {
            if (persisted is not null && !string.Equals(persisted, configuredClusterId, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"The configured cluster id '{configuredClusterId}' is not the id '{persisted}' this broker persisted in "
                    + $"{ClusterIdFilePath} when it joined its cluster. A broker belongs to one cluster: configure "
                    + $"'{persisted}', or remove the data directory to join another cluster.");
            }

            SetKnown(configuredClusterId);
            LogUsingConfigured(configuredClusterId);
        }
        else if (persisted is not null)
        {
            SetKnown(persisted);
            LogLoadedPersisted(persisted);
        }

        _persisted = persisted is not null;
    }

    /// <summary>
    /// This broker's cluster id — configured, persisted, or adopted from the metadata log — or <c>null</c> on a broker
    /// that has not yet joined a cluster.
    /// </summary>
    public string? ClusterId
    {
        get
        {
            lock (_lock)
                return _clusterId;
        }
    }

    /// <summary>Completes with this broker's cluster id once it has one.</summary>
    public Task<string> Known => _known.Task;

    /// <summary>
    /// Completes with this broker's cluster id when its cluster refused it as not its own (INCONSISTENT_CLUSTER_ID).
    /// </summary>
    public Task<string> Refused => _refused.Task;

    /// <summary>
    /// The id a controller writes to the metadata log of a cluster that has none: this broker's own, or one it
    /// generates — the same one on every call, so a controller that retries writes what it wrote before.
    /// </summary>
    public string ProposeClusterId()
    {
        lock (_lock)
            return _clusterId ?? (_candidate ??= GenerateClusterId());
    }

    /// <summary>
    /// The id of a broker that is its own cluster, with no metadata log: this broker's own, or one generated and
    /// persisted now — what formatting its storage gives a Kafka broker.
    /// </summary>
    public string EnsureClusterId()
    {
        string id;
        lock (_lock)
        {
            if (_clusterId is not null)
            {
                PersistOnce(_clusterId);
                return _clusterId;
            }
            id = GenerateClusterId();
            PersistOnce(id);
            SetKnown(id);
        }

        LogGenerated(id);
        return id;
    }

    /// <summary>
    /// Takes the id committed to the metadata log as this broker's own and persists it. A broker that already has
    /// another keeps it and is refused by its cluster when it registers.
    /// </summary>
    /// <returns>Whether the committed id is this broker's.</returns>
    public bool Adopt(string committedClusterId)
    {
        lock (_lock)
        {
            if (_clusterId is not null)
            {
                if (!string.Equals(_clusterId, committedClusterId, StringComparison.Ordinal))
                {
                    LogConflict(_clusterId, committedClusterId);
                    return false;
                }

                // A configured id is persisted once it is the cluster's, so a later change of the configuration is
                // caught at startup.
                PersistOnce(committedClusterId);
                return true;
            }

            PersistOnce(committedClusterId);
            SetKnown(committedClusterId);
        }

        LogAdopted(committedClusterId);
        return true;
    }

    /// <summary>Whether <paramref name="clusterId"/> is this broker's cluster id.</summary>
    public bool IsOwn(string clusterId) => string.Equals(ClusterId, clusterId, StringComparison.Ordinal);

    /// <summary>Records that the cluster refused this broker's id at registration.</summary>
    /// <returns>Whether this is the first refusal.</returns>
    public bool ReportRefused(string announcedClusterId) => _refused.TrySetResult(announcedClusterId);

    private void SetKnown(string id)
    {
        _clusterId = id;
        _known.TrySetResult(id);
    }

    private string ClusterIdFilePath => Path.Combine(_dataDirectory, ClusterIdFileName);

    private string? LoadPersistedClusterId()
    {
        var path = ClusterIdFilePath;
        if (!File.Exists(path))
            return null;

        var clusterId = File.ReadAllText(path).Trim();
        return clusterId.Length == 0 ? null : clusterId;
    }

    private void PersistOnce(string clusterId)
    {
        if (_persisted)
            return;
        Directory.CreateDirectory(_dataDirectory);
        File.WriteAllText(ClusterIdFilePath, clusterId);
        _persisted = true;
    }

    /// <summary>
    /// A new cluster id in Kafka's format: a random UUID, URL-safe Base64 without padding — what
    /// <c>kafka-storage random-uuid</c> prints.
    /// </summary>
    private static string GenerateClusterId()
    {
        Span<byte> bytes = stackalloc byte[16];
        Guid.NewGuid().TryWriteBytes(bytes);
        return Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Using configured cluster id {ClusterId}")]
    private partial void LogUsingConfigured(string clusterId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded persisted cluster id {ClusterId}")]
    private partial void LogLoadedPersisted(string clusterId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Generated cluster id {ClusterId} for a broker without a cluster")]
    private partial void LogGenerated(string clusterId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Adopted cluster id {ClusterId} from the metadata log")]
    private partial void LogAdopted(string clusterId);

    [LoggerMessage(Level = LogLevel.Error,
        Message = "This broker's cluster id {Own} is not {Committed}, the id in the metadata log: it belongs to another cluster and its registration will be refused")]
    private partial void LogConflict(string own, string committed);
}
