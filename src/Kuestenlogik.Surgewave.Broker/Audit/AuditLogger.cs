using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Audit;

/// <summary>
/// Audit logger that writes audit events to a log file for compliance and monitoring.
/// Events are also retained in-memory for quick querying.
/// </summary>
public sealed class AuditLogger : IAsyncDisposable
{
    /// <summary>
    /// Default audit log file name.
    /// </summary>
    public const string AuditLogFileName = "audit.log";

    private readonly BrokerConfig _config;
    private readonly ILogger<AuditLogger> _logger;
    private readonly Channel<AuditEvent> _eventChannel;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _writerTask;
    private readonly ConcurrentQueue<AuditEvent> _recentEvents = new();
    private readonly string _auditLogPath;
    private readonly AuditTopicSink? _topicSink;
    private StreamWriter? _logWriter;
    private bool _disposed;
    private int _eventCount;

    /// <summary>
    /// Whether audit logging is enabled.
    /// </summary>
    public bool IsEnabled => _config.Audit.Enabled;

    /// <summary>
    /// Broker ID for this logger.
    /// </summary>
    public int BrokerId => _config.BrokerId;

    /// <summary>
    /// Number of events in the recent events buffer.
    /// </summary>
    public int RecentEventCount => _recentEvents.Count;

    public AuditLogger(BrokerConfig config, ILogger<AuditLogger> logger, AuditTopicSink? topicSink = null)
    {
        _config = config;
        _logger = logger;
        _topicSink = topicSink;
        _auditLogPath = Path.Combine(config.LogDirectory, AuditLogFileName);

        _eventChannel = Channel.CreateBounded<AuditEvent>(new BoundedChannelOptions(10000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });

        _writerTask = Task.Run(WriteEventsAsync);
    }

    /// <summary>
    /// Initialize the audit logger.
    /// </summary>
    public Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (!IsEnabled)
        {
            _logger.LogInformation("Audit logging is disabled");
            return Task.CompletedTask;
        }

        // Ensure log directory exists
        var logDir = Path.GetDirectoryName(_auditLogPath);
        if (!string.IsNullOrEmpty(logDir) && !Directory.Exists(logDir))
        {
            Directory.CreateDirectory(logDir);
        }

        // Open log file for appending
        _logWriter = new StreamWriter(
            new FileStream(_auditLogPath, FileMode.Append, FileAccess.Write, FileShare.Read),
            Encoding.UTF8)
        {
            AutoFlush = false
        };

        _logger.LogInformation("Audit logger initialized, writing to {Path}", _auditLogPath);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Log an audit event asynchronously.
    /// </summary>
    public void Log(AuditEvent auditEvent)
    {
        if (!IsEnabled) return;

        // Filter events based on configuration
        if (!ShouldLog(auditEvent)) return;

        if (!_eventChannel.Writer.TryWrite(auditEvent))
        {
            _logger.LogWarning("Audit event channel full, event dropped: {EventType}", auditEvent.EventType);
        }
    }

    /// <summary>
    /// Log a topic event.
    /// </summary>
    public void LogTopicEvent(
        AuditEventType eventType,
        string topicName,
        string? principal,
        string? clientAddress,
        string? clientId,
        bool success = true,
        string? errorMessage = null,
        Dictionary<string, string>? details = null)
    {
        Log(new AuditEvent
        {
            EventId = Guid.NewGuid().ToString("N"),
            EventType = eventType,
            Principal = principal,
            ClientAddress = clientAddress,
            ClientId = clientId,
            BrokerId = BrokerId,
            ResourceType = "topic",
            ResourceName = topicName,
            Success = success,
            ErrorMessage = errorMessage,
            Details = details
        });
    }

    /// <summary>
    /// Log an ACL event.
    /// </summary>
    public void LogAclEvent(
        AuditEventType eventType,
        string resourceType,
        string resourceName,
        string? principal,
        string? clientAddress,
        bool success = true,
        string? errorMessage = null,
        Dictionary<string, string>? details = null)
    {
        Log(new AuditEvent
        {
            EventId = Guid.NewGuid().ToString("N"),
            EventType = eventType,
            Principal = principal,
            ClientAddress = clientAddress,
            BrokerId = BrokerId,
            ResourceType = resourceType,
            ResourceName = resourceName,
            Success = success,
            ErrorMessage = errorMessage,
            Details = details
        });
    }

    /// <summary>
    /// Log an authentication event.
    /// </summary>
    public void LogAuthenticationEvent(
        AuditEventType eventType,
        string? principal,
        string? clientAddress,
        string? mechanism,
        bool success = true,
        string? errorMessage = null)
    {
        Log(new AuditEvent
        {
            EventId = Guid.NewGuid().ToString("N"),
            EventType = eventType,
            Principal = principal,
            ClientAddress = clientAddress,
            BrokerId = BrokerId,
            ResourceType = "authentication",
            Success = success,
            ErrorMessage = errorMessage,
            Details = mechanism != null ? new Dictionary<string, string> { ["mechanism"] = mechanism } : null
        });
    }

    /// <summary>
    /// Log a connector event.
    /// </summary>
    public void LogConnectorEvent(
        AuditEventType eventType,
        string connectorName,
        string? principal,
        string? clientAddress,
        bool success = true,
        string? errorMessage = null,
        Dictionary<string, string>? details = null)
    {
        Log(new AuditEvent
        {
            EventId = Guid.NewGuid().ToString("N"),
            EventType = eventType,
            Principal = principal,
            ClientAddress = clientAddress,
            BrokerId = BrokerId,
            ResourceType = "connector",
            ResourceName = connectorName,
            Success = success,
            ErrorMessage = errorMessage,
            Details = details
        });
    }

    /// <summary>
    /// Log a configuration change event.
    /// </summary>
    public void LogConfigEvent(
        string resourceType,
        string resourceName,
        string? principal,
        string? clientAddress,
        Dictionary<string, string> changes,
        bool success = true,
        string? errorMessage = null)
    {
        Log(new AuditEvent
        {
            EventId = Guid.NewGuid().ToString("N"),
            EventType = AuditEventType.ConfigChanged,
            Principal = principal,
            ClientAddress = clientAddress,
            BrokerId = BrokerId,
            ResourceType = resourceType,
            ResourceName = resourceName,
            Success = success,
            ErrorMessage = errorMessage,
            Details = changes
        });
    }

    /// <summary>
    /// Query recent audit events from memory.
    /// </summary>
    public AuditQueryResult QueryRecent(AuditEventQuery query)
    {
        var events = _recentEvents
            .Where(e => MatchesQuery(e, query))
            .OrderByDescending(e => e.Timestamp)
            .Skip(query.Offset)
            .Take(query.Limit)
            .ToList();

        var totalCount = _recentEvents.Count(e => MatchesQuery(e, query));

        return new AuditQueryResult
        {
            Events = events,
            TotalCount = totalCount,
            HasMore = totalCount > query.Offset + events.Count
        };
    }

    /// <summary>
    /// Query audit events from the log file.
    /// </summary>
    public async Task<AuditQueryResult> QueryFromFileAsync(AuditEventQuery query, CancellationToken cancellationToken = default)
    {
        if (!File.Exists(_auditLogPath))
        {
            return new AuditQueryResult { Events = [], TotalCount = 0, HasMore = false };
        }

        var events = new List<AuditEvent>();
        var totalCount = 0;
        var skipCount = 0;

        using var reader = new StreamReader(
            new FileStream(_auditLogPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite),
            Encoding.UTF8);

        while (!reader.EndOfStream && !cancellationToken.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync(cancellationToken);
            if (string.IsNullOrWhiteSpace(line)) continue;

            try
            {
                var auditEvent = JsonSerializer.Deserialize<AuditEvent>(line);
                if (auditEvent == null) continue;

                // Apply time filters
                if (query.StartTime.HasValue && auditEvent.Timestamp < query.StartTime.Value)
                    continue;
                if (query.EndTime.HasValue && auditEvent.Timestamp >= query.EndTime.Value)
                    continue;

                if (!MatchesQuery(auditEvent, query))
                    continue;

                totalCount++;

                if (skipCount < query.Offset)
                {
                    skipCount++;
                    continue;
                }

                if (events.Count < query.Limit)
                {
                    events.Add(auditEvent);
                }
            }
            catch (JsonException)
            {
                // Skip malformed lines
            }
        }

        return new AuditQueryResult
        {
            Events = events,
            TotalCount = totalCount,
            HasMore = totalCount > query.Offset + events.Count
        };
    }

    private bool ShouldLog(AuditEvent auditEvent)
    {
        // Filter by event types if configured
        if (_config.Audit.IncludeEventTypes.Count > 0 &&
            !_config.Audit.IncludeEventTypes.Contains(auditEvent.EventType))
        {
            return false;
        }

        if (_config.Audit.ExcludeEventTypes.Contains(auditEvent.EventType))
        {
            return false;
        }

        // Filter internal topics if configured
        if (_config.Audit.ExcludeInternalTopics &&
            auditEvent.ResourceType == "topic" &&
            auditEvent.ResourceName?.StartsWith("__") == true)
        {
            return false;
        }

        // Filter authentication events based on config
        if (auditEvent.EventType == AuditEventType.AuthenticationSuccess &&
            !_config.Audit.LogSuccessfulAuthentication)
        {
            return false;
        }

        if (auditEvent.EventType == AuditEventType.AuthorizationCheck &&
            !_config.Audit.LogAuthorizationChecks)
        {
            return false;
        }

        return true;
    }

    private static bool MatchesQuery(AuditEvent auditEvent, AuditEventQuery query)
    {
        if (query.EventType.HasValue && auditEvent.EventType != query.EventType.Value)
            return false;

        if (query.Principal != null && !string.Equals(auditEvent.Principal, query.Principal, StringComparison.OrdinalIgnoreCase))
            return false;

        if (query.ResourceType != null && !string.Equals(auditEvent.ResourceType, query.ResourceType, StringComparison.OrdinalIgnoreCase))
            return false;

        if (query.ResourceName != null && !string.Equals(auditEvent.ResourceName, query.ResourceName, StringComparison.OrdinalIgnoreCase))
            return false;

        if (query.Success.HasValue && auditEvent.Success != query.Success.Value)
            return false;

        return true;
    }

    private async Task WriteEventsAsync()
    {
        var batch = new List<AuditEvent>();
        var flushInterval = TimeSpan.FromSeconds(1);
        var lastFlush = DateTime.UtcNow;

        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                // Wait for events with timeout for periodic flush
                using var timeoutCts = new CancellationTokenSource(flushInterval);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, timeoutCts.Token);

                try
                {
                    while (batch.Count < 100 && await _eventChannel.Reader.WaitToReadAsync(linkedCts.Token))
                    {
                        while (batch.Count < 100 && _eventChannel.Reader.TryRead(out var auditEvent))
                        {
                            batch.Add(auditEvent);
                        }
                    }
                }
                catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
                {
                    // Timeout - flush what we have
                }

                if (batch.Count > 0)
                {
                    await WriteBatchAsync(batch);
                    batch.Clear();
                    lastFlush = DateTime.UtcNow;
                }
                else if ((DateTime.UtcNow - lastFlush) > flushInterval && _logWriter != null)
                {
                    await _logWriter.FlushAsync();
                    lastFlush = DateTime.UtcNow;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error writing audit events");
                await Task.Delay(1000, _cts.Token);
            }
        }

        // Flush remaining events on shutdown
        while (_eventChannel.Reader.TryRead(out var auditEvent))
        {
            batch.Add(auditEvent);
        }

        if (batch.Count > 0)
        {
            try
            {
                await WriteBatchAsync(batch);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error flushing audit events on shutdown");
            }
        }
    }

    private async Task WriteBatchAsync(List<AuditEvent> events)
    {
        if (_logWriter == null) return;

        foreach (var auditEvent in events)
        {
            var json = JsonSerializer.Serialize(auditEvent);
            await _logWriter.WriteLineAsync(json);

            // Keep in recent events for quick access
            _recentEvents.Enqueue(auditEvent);
            Interlocked.Increment(ref _eventCount);
        }

        await _logWriter.FlushAsync();

        // Mirror to the audit topic if a sink was wired up. Errors are logged
        // inside the sink and never propagate — the file write above is the
        // authoritative compliance record.
        if (_topicSink is not null)
        {
            await _topicSink.WriteAsync(events, _cts.Token).ConfigureAwait(false);
        }

        _logger.LogDebug("Wrote {Count} audit events", events.Count);

        // Trim old events from memory (keep last 10000)
        while (_recentEvents.Count > 10000)
        {
            _recentEvents.TryDequeue(out _);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        _cts.Cancel();

        try
        {
            await _writerTask;
        }
        catch
        {
            // Ignore exceptions during shutdown
        }

        if (_logWriter != null)
        {
            await _logWriter.DisposeAsync();
        }

        _cts.Dispose();
        _disposed = true;
    }
}
