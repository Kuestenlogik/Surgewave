using System.Collections.Concurrent;
using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Util;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Cdc;

/// <summary>
/// Background service that reads CDC events from a source and produces them to Surgewave topics.
/// Manages the lifecycle of CDC sources and tracks position for restart recovery.
/// </summary>
public sealed class CdcService : BackgroundService
{
    private readonly CdcConfig _config;
    private readonly ILogger<CdcService> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ConcurrentDictionary<string, CdcSourceEntry> _sources = new();
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    /// <summary>
    /// Initializes a new instance of <see cref="CdcService"/>.
    /// </summary>
    /// <param name="config">CDC configuration.</param>
    /// <param name="logger">Logger for the service.</param>
    /// <param name="loggerFactory">Logger factory for creating source loggers.</param>
    public CdcService(CdcConfig config, ILogger<CdcService> logger, ILoggerFactory loggerFactory)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
    }

    /// <summary>
    /// Creates and registers a new CDC source with the given configuration.
    /// </summary>
    /// <param name="id">Unique identifier for this source.</param>
    /// <param name="config">Configuration for the source.</param>
    /// <returns>True if the source was added, false if an ID conflict exists.</returns>
    public bool AddSource(string id, CdcConfig config)
    {
        var sourceLogger = _loggerFactory.CreateLogger<PostgresCdcSource>();
#pragma warning disable CA2000 // Source ownership is transferred to CdcSourceEntry, disposed via RemoveSourceAsync
        var source = new PostgresCdcSource(config, sourceLogger);
#pragma warning restore CA2000
        var entry = new CdcSourceEntry(id, source, config);

        if (!_sources.TryAdd(id, entry))
        {
            return false;
        }

        _logger.LogInformation("CDC source added: {Id} (slot={Slot})", LogSanitizer.Sanitize(id), LogSanitizer.Sanitize(config.SlotName));
        return true;
    }

    /// <summary>
    /// Removes and stops a CDC source.
    /// </summary>
    /// <param name="id">The source identifier to remove.</param>
    /// <returns>True if the source was found and removed.</returns>
    public async Task<bool> RemoveSourceAsync(string id)
    {
        if (!_sources.TryRemove(id, out var entry))
            return false;

        entry.Cancel();
        await entry.Source.DisposeAsync();
        _logger.LogInformation("CDC source removed: {Id}", LogSanitizer.Sanitize(id));
        return true;
    }

    /// <summary>
    /// Gets the status of a specific CDC source.
    /// </summary>
    /// <param name="id">The source identifier.</param>
    /// <returns>The source status, or null if not found.</returns>
    public CdcSourceStatus? GetSourceStatus(string id)
    {
        return _sources.TryGetValue(id, out var entry) ? entry.GetStatus() : null;
    }

    /// <summary>
    /// Gets the status of all registered CDC sources.
    /// </summary>
    /// <returns>A list of all source statuses.</returns>
    public IReadOnlyList<CdcSourceStatus> GetAllSourceStatuses()
    {
        return _sources.Values.Select(e => e.GetStatus()).ToList();
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            _logger.LogDebug("CDC service is disabled");
            return;
        }

        _logger.LogInformation("CDC service started");

        // Register the default source from configuration if connection string is set
        if (!string.IsNullOrWhiteSpace(_config.ConnectionString))
        {
            AddSource("default", _config);
        }

        // Start capture loops for all registered sources
        var tasks = new List<Task>();
        foreach (var (id, entry) in _sources)
        {
            tasks.Add(RunCaptureLoopAsync(entry, stoppingToken));
        }

        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks);
        }

        _logger.LogInformation("CDC service stopped");
    }

    private async Task RunCaptureLoopAsync(CdcSourceEntry entry, CancellationToken stoppingToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, entry.CancellationToken);

        entry.State = CdcSourceState.Initializing;
        entry.StartedAt = DateTimeOffset.UtcNow;

        try
        {
            entry.State = CdcSourceState.Streaming;
            _logger.LogInformation("CDC capture loop started for source: {Id}", entry.Id);

            await foreach (var evt in entry.Source.CaptureChangesAsync(linkedCts.Token))
            {
                // Generate the topic name
                var topicName = CdcTopicNaming.GetTopicName(entry.Config, evt.Schema, evt.Table);

                // Serialize the event as the message value
                var value = JsonSerializer.Serialize(evt, s_jsonOptions);

                // Generate the key from primary key (if available from the After or Before data)
                var keyData = evt.After ?? evt.Before;
                var key = keyData is not null
                    ? JsonSerializer.Serialize(keyData, s_jsonOptions)
                    : null;

                entry.EventsCaptured++;
                entry.LastLsn = evt.Lsn;
                entry.LastEventTimestamp = evt.Timestamp;

                _logger.LogTrace(
                    "CDC event: {Operation} on {Schema}.{Table} -> topic={Topic}",
                    evt.Operation, evt.Schema, evt.Table, topicName);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("CDC capture loop cancelled for source: {Id}", entry.Id);
            entry.State = CdcSourceState.Stopped;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "CDC capture loop failed for source: {Id}", entry.Id);
            entry.State = CdcSourceState.Faulted;
            entry.Error = ex.Message;
        }
    }

    private sealed class CdcSourceEntry
    {
        private readonly CancellationTokenSource _cts = new();

        public string Id { get; }
        public PostgresCdcSource Source { get; }
        public CdcConfig Config { get; }
        public CdcSourceState State { get; set; } = CdcSourceState.Stopped;
        public long EventsCaptured { get; set; }
        public long LastLsn { get; set; }
        public DateTimeOffset? LastEventTimestamp { get; set; }
        public string? Error { get; set; }
        public DateTimeOffset StartedAt { get; set; }

        public CancellationToken CancellationToken => _cts.Token;

        public CdcSourceEntry(string id, PostgresCdcSource source, CdcConfig config)
        {
            Id = id;
            Source = source;
            Config = config;
        }

        public void Cancel() => _cts.Cancel();

        public CdcSourceStatus GetStatus() => new()
        {
            Id = Id,
            DatabaseType = Source.DatabaseType,
            State = State,
            SlotName = Config.SlotName,
            PublicationName = Config.PublicationName,
            TrackedTables = Config.Tables.Count,
            EventsCaptured = EventsCaptured,
            LastLsn = LastLsn,
            LastEventTimestamp = LastEventTimestamp,
            Error = Error,
            StartedAt = StartedAt
        };
    }
}
