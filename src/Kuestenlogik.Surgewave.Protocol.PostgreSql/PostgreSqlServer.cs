using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Protocol.PostgreSql;

/// <summary>
/// Background service that listens for PostgreSQL wire protocol connections
/// and dispatches each to a <see cref="PostgreSqlConnectionHandler"/>.
/// Disabled by default — enable via Surgewave:PostgreSql:Enabled=true.
/// </summary>
public sealed class PostgreSqlServer : BackgroundService
{
    private readonly PostgreSqlConfig _config;
    private readonly LogManager _logManager;
    private readonly ILogger<PostgreSqlServer> _logger;
    private readonly MaterializedViewRegistry? _viewRegistry;
    private TcpListener? _listener;
    private int _activeConnections;

    public PostgreSqlServer(
        IOptions<PostgreSqlConfig> config,
        LogManager logManager,
        ILogger<PostgreSqlServer> logger,
        MaterializedViewRegistry? viewRegistry = null)
    {
        _config = config.Value;
        _logManager = logManager;
        _logger = logger;
        _viewRegistry = viewRegistry;
    }

    /// <summary>
    /// Gets the number of currently active PostgreSQL connections.
    /// </summary>
    public int ActiveConnections => _activeConnections;

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            _logger.LogDebug("PostgreSQL protocol adapter is disabled");
            return;
        }

        _listener = new TcpListener(IPAddress.Any, _config.Port);
        _listener.Start();

        _logger.LogInformation(
            "PostgreSQL wire protocol adapter listening on port {Port} (max connections: {MaxConnections})",
            _config.Port, _config.MaxConnections);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (_activeConnections >= _config.MaxConnections)
                {
                    _logger.LogWarning(
                        "PostgreSQL max connections reached ({Max}), dropping incoming connection",
                        _config.MaxConnections);
                    client.Dispose();
                    continue;
                }

                _ = HandleClientAsync(client, stoppingToken);
            }
        }
        finally
        {
            _listener.Stop();
            _logger.LogInformation("PostgreSQL wire protocol adapter stopped");
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        Interlocked.Increment(ref _activeConnections);
        var remoteEndPoint = client.Client.RemoteEndPoint?.ToString() ?? "unknown";

        _logger.LogInformation("PostgreSQL client connected: {Endpoint} (active: {Active})",
            remoteEndPoint, _activeConnections);

        try
        {
            using (client)
            {
                // NoDelay needs the write buffering below: PgWriter emits one stream write per
                // protocol message (one per result row), which Nagle used to coalesce. The
                // BufferedStream restores coalescing up to the handler's existing per-message
                // FlushAsync discipline — same model as real PostgreSQL (8 KB output buffer +
                // TCP_NODELAY). The handler is strictly sequential, so buffered read/write
                // mode switching is safe.
                client.NoDelay = true;
                var stream = new BufferedStream(client.GetStream(), 8192);
                await using (stream.ConfigureAwait(false))
                {
                    var queryExecutor = new QueryExecutor(_logManager, _config.ServerVersion, _logger, _viewRegistry);
                    var handler = new PostgreSqlConnectionHandler(stream, queryExecutor, _config, _logger);
                    await handler.HandleAsync(ct).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { /* shutdown */ }
        catch (IOException ex)
        {
            _logger.LogDebug("PostgreSQL client {Endpoint} disconnected: {Message}",
                remoteEndPoint, ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PostgreSQL client {Endpoint} error", remoteEndPoint);
        }
        finally
        {
            Interlocked.Decrement(ref _activeConnections);
            _logger.LogInformation("PostgreSQL client disconnected: {Endpoint} (active: {Active})",
                remoteEndPoint, _activeConnections);
        }
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        _listener?.Stop();
        _listener?.Dispose();
        base.Dispose();
    }
}
