using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Schema.Registry.Linking;

/// <summary>
/// Background service that periodically synchronizes schemas between linked registries.
/// Detects new schema versions on local and remote registries and replicates them
/// according to the configured sync mode and conflict resolution strategy.
/// </summary>
public sealed class SchemaLinkingService : BackgroundService
{
    private readonly SchemaLinkingConfig _config;
    private readonly ISchemaStore _store;
    private readonly ILogger<SchemaLinkingService> _logger;
    private readonly SchemaLinkingState _state;
    private readonly SchemaLinkingMetrics _metrics = new();
    private readonly string _localClusterId;
    private readonly string? _statePath;

    /// <summary>
    /// Initializes a new instance of the <see cref="SchemaLinkingService"/>.
    /// </summary>
    public SchemaLinkingService(
        SchemaLinkingConfig config,
        ISchemaStore store,
        ILogger<SchemaLinkingService> logger,
        string? localClusterId = null,
        string? statePath = null)
    {
        _config = config;
        _store = store;
        _logger = logger;
        _localClusterId = localClusterId ?? "local";
        _statePath = statePath;
        _state = !string.IsNullOrEmpty(statePath)
            ? SchemaLinkingState.LoadFromFile(statePath)
            : new SchemaLinkingState();
    }

    /// <summary>
    /// Gets the current sync state (for REST API queries).
    /// </summary>
    public SchemaLinkingState State => _state;

    /// <summary>
    /// Gets the current metrics (for REST API queries).
    /// </summary>
    public SchemaLinkingMetrics Metrics => _metrics;

    /// <summary>
    /// Forces an immediate sync cycle outside the regular interval.
    /// </summary>
    public async Task ForceSyncAsync(CancellationToken ct = default)
    {
        _logger.LogInformation("Forced schema linking sync triggered");
        await RunSyncCycleAsync(ct);
    }

    /// <summary>
    /// Resolves a conflict by choosing a side (local or remote).
    /// </summary>
    public void ResolveConflict(string clusterId, string subject, bool useLocal)
    {
        var link = _state.GetLink(clusterId, subject);
        if (link is null || link.Status != SchemaSyncStatus.Conflict)
        {
            return;
        }

        link.Status = SchemaSyncStatus.Synced;
        link.LastSyncedAt = DateTimeOffset.UtcNow;
        link.ErrorMessage = null;
        _metrics.RecordConflictResolved();

        _logger.LogInformation(
            "Resolved conflict for {Subject} with cluster {Cluster}: chose {Side}",
            subject, clusterId, useLocal ? "local" : "remote");

        PersistState();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            _logger.LogInformation("Schema linking is disabled");
            return;
        }

        if (_config.RemoteRegistries.Count == 0)
        {
            _logger.LogWarning("Schema linking enabled but no remote registries configured");
            return;
        }

        _logger.LogInformation(
            "Schema linking started (mode={Mode}, interval={Interval}s, remotes={Count}, patterns={Patterns})",
            _config.SyncMode, _config.SyncIntervalSeconds, _config.RemoteRegistries.Count,
            string.Join(", ", _config.SubjectPatterns));

        // Initial delay to let the broker start up
        await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSyncCycleAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Schema linking sync cycle failed");
                _metrics.RecordError();
            }

            await Task.Delay(TimeSpan.FromSeconds(_config.SyncIntervalSeconds), stoppingToken);
        }

        _logger.LogInformation("Schema linking stopped");
    }

    private async Task RunSyncCycleAsync(CancellationToken ct)
    {
        foreach (var remote in _config.RemoteRegistries)
        {
            if (ct.IsCancellationRequested) break;

            try
            {
                using var client = new RemoteSchemaRegistryClient(remote.SchemaRegistryUrl);
                await SyncWithRemoteAsync(remote, client, ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to sync with remote registry {ClusterId} at {Url}",
                    remote.ClusterId, remote.SchemaRegistryUrl);
                _metrics.RecordError();
            }
        }

        _metrics.RecordSyncCycleComplete();
        PersistState();
    }

    private async Task SyncWithRemoteAsync(
        LinkedSchemaRegistry remote,
        RemoteSchemaRegistryClient client,
        CancellationToken ct)
    {
        // Import: pull remote schemas to local
        if (_config.SyncMode is SchemaSyncMode.Import or SchemaSyncMode.Bidirectional)
        {
            await ImportFromRemoteAsync(remote, client, ct);
        }

        // Export: push local schemas to remote
        if (_config.SyncMode is SchemaSyncMode.Export or SchemaSyncMode.Bidirectional)
        {
            await ExportToRemoteAsync(remote, client, ct);
        }
    }

    private async Task ImportFromRemoteAsync(
        LinkedSchemaRegistry remote,
        RemoteSchemaRegistryClient client,
        CancellationToken ct)
    {
        var remoteSubjects = await client.GetSubjectsAsync(ct);

        foreach (var subject in remoteSubjects)
        {
            if (ct.IsCancellationRequested) break;

            // Check if subject matches our patterns
            if (!SubjectPatternMatcher.MatchesAny(subject, _config.SubjectPatterns))
            {
                continue;
            }

            try
            {
                var remoteVersions = await client.GetVersionsAsync(subject, ct);
                if (remoteVersions.Count == 0) continue;

                var remoteLatestVersion = remoteVersions[^1];
                var localVersions = _store.GetVersions(subject);
                var localLatestVersion = localVersions.Count > 0 ? localVersions[^1] : 0;

                if (remoteLatestVersion <= localLatestVersion)
                {
                    // Local is up to date or ahead; check for conflict in bidirectional mode
                    continue;
                }

                // Remote has newer versions — import them
                var startVersion = localLatestVersion + 1;
                for (var v = startVersion; v <= remoteLatestVersion; v++)
                {
                    if (!remoteVersions.Contains(v)) continue;

                    var remoteSchema = await client.GetSchemaAsync(subject, v, ct);
                    var schemaType = ParseSchemaType(remoteSchema.SchemaType);
                    _store.RegisterSchema(subject, remoteSchema.Schema, schemaType);

                    _logger.LogInformation(
                        "Imported schema {Subject} v{Version} from cluster {Cluster}",
                        subject, v, remote.ClusterId);

                    _metrics.RecordSync(remote.ClusterId);
                }

                // Sync compatibility config if enabled
                if (_config.SyncCompatibilityConfig)
                {
                    try
                    {
                        var remoteCompat = await client.GetCompatibilityAsync(subject, ct);
                        if (Enum.TryParse<CompatibilityMode>(remoteCompat, ignoreCase: true, out var mode))
                        {
                            _store.SetCompatibility(subject, mode);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Failed to sync compatibility for {Subject} from {Cluster}",
                            subject, remote.ClusterId);
                    }
                }

                // Update link state
                var updatedLocalVersions = _store.GetVersions(subject);
                _state.SetLink(remote.ClusterId, subject, new SchemaLink
                {
                    Subject = subject,
                    SourceCluster = remote.ClusterId,
                    TargetCluster = _localClusterId,
                    SourceVersion = remoteLatestVersion,
                    TargetVersion = updatedLocalVersions.Count > 0 ? updatedLocalVersions[^1] : 0,
                    Status = SchemaSyncStatus.Synced,
                    LastSyncedAt = DateTimeOffset.UtcNow
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to import subject {Subject} from cluster {Cluster}",
                    subject, remote.ClusterId);

                _state.SetLink(remote.ClusterId, subject, new SchemaLink
                {
                    Subject = subject,
                    SourceCluster = remote.ClusterId,
                    TargetCluster = _localClusterId,
                    SourceVersion = 0,
                    TargetVersion = 0,
                    Status = SchemaSyncStatus.Failed,
                    LastSyncedAt = DateTimeOffset.UtcNow,
                    ErrorMessage = ex.Message
                });

                _metrics.RecordError();
            }
        }
    }

    private async Task ExportToRemoteAsync(
        LinkedSchemaRegistry remote,
        RemoteSchemaRegistryClient client,
        CancellationToken ct)
    {
        var localSubjects = _store.GetSubjects();

        foreach (var subject in localSubjects)
        {
            if (ct.IsCancellationRequested) break;

            if (!SubjectPatternMatcher.MatchesAny(subject, _config.SubjectPatterns))
            {
                continue;
            }

            try
            {
                var localVersions = _store.GetVersions(subject);
                if (localVersions.Count == 0) continue;

                var localLatestVersion = localVersions[^1];

                // Check what the remote already has
                IReadOnlyList<int> remoteVersions;
                try
                {
                    remoteVersions = await client.GetVersionsAsync(subject, ct);
                }
                catch (HttpRequestException)
                {
                    // Subject doesn't exist on remote — that's fine, we'll create it
                    remoteVersions = [];
                }

                var remoteLatestVersion = remoteVersions.Count > 0 ? remoteVersions[^1] : 0;

                if (localLatestVersion <= remoteLatestVersion)
                {
                    // Remote is up to date or ahead
                    continue;
                }

                // Check for conflict in bidirectional mode
                if (_config.SyncMode == SchemaSyncMode.Bidirectional && remoteLatestVersion > 0)
                {
                    var existingLink = _state.GetLink(remote.ClusterId, subject);
                    if (existingLink?.SourceVersion != remoteLatestVersion)
                    {
                        // Remote may have changed independently — potential conflict
                        var resolved = ResolveVersionConflict(
                            subject, remote.ClusterId,
                            localLatestVersion, remoteLatestVersion);

                        if (!resolved)
                        {
                            continue; // Conflict marked; skip export
                        }
                    }
                }

                // Export newer local versions
                for (var v = remoteLatestVersion + 1; v <= localLatestVersion; v++)
                {
                    if (!localVersions.Contains(v)) continue;

                    var localSchema = _store.GetSchema(subject, v);
                    if (localSchema is null) continue;

                    await client.RegisterSchemaAsync(
                        subject,
                        localSchema.SchemaString,
                        localSchema.SchemaType.ToString().ToUpperInvariant(),
                        ct);

                    _logger.LogInformation(
                        "Exported schema {Subject} v{Version} to cluster {Cluster}",
                        subject, v, remote.ClusterId);

                    _metrics.RecordSync(remote.ClusterId);
                }

                // Sync compatibility config if enabled
                if (_config.SyncCompatibilityConfig)
                {
                    try
                    {
                        var localCompat = _store.GetCompatibility(subject);
                        await client.SetCompatibilityAsync(
                            subject,
                            localCompat.ToString().ToUpperInvariant(),
                            ct);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Failed to sync compatibility for {Subject} to {Cluster}",
                            subject, remote.ClusterId);
                    }
                }

                // Update link state
                _state.SetLink(remote.ClusterId, subject, new SchemaLink
                {
                    Subject = subject,
                    SourceCluster = _localClusterId,
                    TargetCluster = remote.ClusterId,
                    SourceVersion = localLatestVersion,
                    TargetVersion = localLatestVersion,
                    Status = SchemaSyncStatus.Synced,
                    LastSyncedAt = DateTimeOffset.UtcNow
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to export subject {Subject} to cluster {Cluster}",
                    subject, remote.ClusterId);

                _state.SetLink(remote.ClusterId, subject, new SchemaLink
                {
                    Subject = subject,
                    SourceCluster = _localClusterId,
                    TargetCluster = remote.ClusterId,
                    SourceVersion = 0,
                    TargetVersion = 0,
                    Status = SchemaSyncStatus.Failed,
                    LastSyncedAt = DateTimeOffset.UtcNow,
                    ErrorMessage = ex.Message
                });

                _metrics.RecordError();
            }
        }
    }

    /// <summary>
    /// Resolves a version conflict using the configured strategy.
    /// Returns true if the conflict was resolved (proceed with export), false if it was marked as conflict.
    /// </summary>
    private bool ResolveVersionConflict(string subject, string clusterId, int localVersion, int remoteVersion)
    {
        switch (_config.ConflictResolution)
        {
            case ConflictResolution.HighestVersion:
                if (localVersion >= remoteVersion) return true; // Local wins
                return false; // Remote wins, skip export

            case ConflictResolution.LocalWins:
                return true; // Always proceed with export

            case ConflictResolution.RemoteWins:
                return false; // Never export when remote has versions

            default:
                // Mark as conflict for manual resolution
                _state.SetLink(clusterId, subject, new SchemaLink
                {
                    Subject = subject,
                    SourceCluster = _localClusterId,
                    TargetCluster = clusterId,
                    SourceVersion = localVersion,
                    TargetVersion = remoteVersion,
                    Status = SchemaSyncStatus.Conflict,
                    LastSyncedAt = DateTimeOffset.UtcNow,
                    ErrorMessage = $"Version conflict: local v{localVersion} vs remote v{remoteVersion}"
                });

                _metrics.RecordConflict();
                _logger.LogWarning(
                    "Version conflict for {Subject} with cluster {Cluster}: local v{Local} vs remote v{Remote}",
                    subject, clusterId, localVersion, remoteVersion);

                return false;
        }
    }

    private void PersistState()
    {
        if (!string.IsNullOrEmpty(_statePath))
        {
            try
            {
                _state.SaveToFile(_statePath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to persist schema linking state");
            }
        }
    }

    private static SchemaType ParseSchemaType(string? schemaType)
    {
        return schemaType?.ToUpperInvariant() switch
        {
            "JSON" => SchemaType.Json,
            "PROTOBUF" => SchemaType.Protobuf,
            "FLATBUFFERS" => SchemaType.FlatBuffers,
            _ => SchemaType.Avro
        };
    }
}
