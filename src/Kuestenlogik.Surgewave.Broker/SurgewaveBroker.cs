using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Broker.KeyValue;
using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Broker.ShareGroups;
using Kuestenlogik.Surgewave.Broker.StreamsGroups;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Surgewave broker server — a protocol-neutral TCP listener. On each accepted connection it
/// peeks the first magic bytes and hands the socket to the first registered
/// <see cref="IConnectionHandler"/> that claims it (native first, plugin-provided protocols like
/// Kafka as fallbacks). The broker itself carries no wire-protocol code: the Kafka wire loop
/// moved into the Kafka plugin as a registered <see cref="IConnectionHandler"/> (#59 b5).
/// </summary>
public sealed class SurgewaveBroker : IAsyncDisposable, ISurgewaveStreamHandler
{
    private readonly BrokerConfig _config;
    private readonly LogManager _logManager;
    private readonly ILogger<SurgewaveBroker> _logger;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly List<Task> _clientTasks = [];
    private bool _disposed;

    // Frozen at construction: the ordered per-connection protocol handlers, walked once per
    // CONNECTION in HandleAsync (native first, then plugin-provided fallbacks like Kafka) — never
    // per request. Replaces the hardwired native-vs-Kafka branch so Kafka lives in a plugin (#59).
    private readonly IConnectionHandler[] _connectionHandlers;

    // Metrics
    private readonly BrokerMetrics _metrics;

    // TLS handler
    private readonly TlsHandler? _tlsHandler;

    // Surgewave native protocol handler
    private readonly SurgewaveNativeHandler _nativeHandler;
    private readonly ILoggerFactory _nativeLoggerFactory;
    private readonly ConnectorRepositoryManager _connectorRepositoryManager;

    /// <summary>
    /// Connector-repository manager backing the Native <c>SearchPlugins</c>
    /// operation. Exposed so Program.cs can call
    /// <see cref="ConnectorRepositoryManager.SyncFromStore"/> with the
    /// broker's <c>RepositoryStore</c> singleton at startup — without that
    /// hop the manager would keep its hard-coded NuGet.org default and
    /// silently ignore everything the operator edits in
    /// <c>/plugins/sources</c>.
    /// </summary>
    public ConnectorRepositoryManager ConnectorRepositoryManager => _connectorRepositoryManager;

    // Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory
    // private readonly SharedMemoryNativeHandler? _sharedMemoryHandler;

    public SurgewaveBroker(
        BrokerConfig config,
        LogManager logManager,
        RecordBatchSerializer serializer,
        NativeGroupCoordinator nativeGroupCoordinator,
        TransactionCoordinator transactionCoordinator,
        QuotaManager quotaManager,
        BrokerMetrics metrics,
        ILogger<SurgewaveBroker> logger,
        IEnumerable<IConnectionHandler>? connectionHandlers = null,
        SchemaStore? schemaStore = null,
        CompatibilityChecker? compatibilityChecker = null,
        ConnectWorker? connectWorker = null,
        PluginDiscovery? pluginDiscovery = null,
        DlqManager? dlqManager = null,
        Transactions.CrossTopicTransactionManager? crossTopicTxnManager = null,
        KvBucketManager? kvBucketManager = null,
        Kuestenlogik.Surgewave.Core.Monitoring.ILagCalculator? lagCalculator = null)
    {
        _config = config;
        _logManager = logManager;
        _metrics = metrics;
        _logger = logger;

        // Register state accessors for observable gauges
        _metrics.RegisterStateAccessors(
            () => _logManager.ListTopics().Count(),
            () => _logManager.ListTopics().Sum(t =>
            {
                long totalSize = 0;
                for (int i = 0; i < t.PartitionCount; i++)
                {
                    var log = _logManager.GetLog(new Core.Models.TopicPartition { Topic = t.Name, Partition = i });
                    if (log != null) totalSize += log.TotalSize;
                }
                return totalSize;
            }));

        // Create listener - use dual-stack for localhost if enabled
        if (_config.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase))
        {
            if (_config.EnableDualMode)
            {
                // Dual-stack: accept both IPv4 and IPv6 connections
                _listener = new TcpListener(IPAddress.IPv6Any, _config.Port);
                _listener.Server.DualMode = true;
            }
            else
            {
                // IPv4 only: bind to 127.0.0.1 and advertise the resolved address
                // so that clients (librdkafka) don't resolve "localhost" to ::1 (IPv6)
                _listener = new TcpListener(IPAddress.Loopback, _config.Port);
                _config.Host = "127.0.0.1";
            }
        }
        else
        {
            var endpoint = new IPEndPoint(IPAddress.Parse(_config.Host), _config.Port);
            _listener = new TcpListener(endpoint);
        }

        // Initialize TLS handler if enabled
        if (_config.Security.TlsEnabled)
        {
            _tlsHandler = new TlsHandler(_config.Security);
        }

        // Initialize Surgewave native protocol handler
        _nativeLoggerFactory = LoggerFactory.Create(b => b.AddConsole());
        // Connector-Repository-Manager fuer SearchPlugins/InstallPlugin Native-Ops.
        // Verwendet den konfigurierten PluginsDirectory (gleich der vom Connect-Plugin
        // gescannt wird), so dass Control's Plugin-Marketplace die installierten
        // Connectoren als "installed" listet.
        var pluginsDirAbsolute = Path.GetFullPath(_config.Connect.PluginsDirectory ?? "plugins");
        _connectorRepositoryManager = new ConnectorRepositoryManager(pluginsDirAbsolute);
        _nativeHandler = new SurgewaveNativeHandler(
            _logManager,
            serializer,
            nativeGroupCoordinator,
            _config,
            _nativeLoggerFactory.CreateLogger<SurgewaveNativeHandler>(),
            transactionCoordinator: transactionCoordinator,
            quotaManager: quotaManager,
            schemaStore: schemaStore,
            compatibilityChecker: compatibilityChecker,
            connectWorker: connectWorker,
            pluginDiscovery: pluginDiscovery,
            dlqManager: dlqManager,
            crossTopicTxnManager: crossTopicTxnManager,
            kvBucketManager: kvBucketManager,
            repositoryManager: _connectorRepositoryManager,
            lagCalculator: lagCalculator);

        // The broker owns only the NATIVE protocol handler; every other protocol (Kafka today)
        // plugs in as a registered IConnectionHandler contributed via DI (#59 b5). The set is
        // frozen + order-sorted at ctor and walked once per CONNECTION — never per request. When
        // no Kafka handler is contributed (Surgewave:Kafka:Enabled=false, #58) the broker serves
        // native-only and closes any non-native peer.
        var handlers = new List<IConnectionHandler> { new NativeConnectionHandler(_nativeHandler) };
        if (connectionHandlers is not null)
            handlers.AddRange(connectionHandlers);
        _connectionHandlers = [.. handlers.OrderBy(h => h.Order)];

        // Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory
        // Shared memory handler requires the Kuestenlogik.Surgewave.Transport.SharedMemory package.
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        Log.BrokerStarting(_logger, _config.Host, _config.Port);
        Log.DataDirectory(_logger, _config.DataDirectory);

        // Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory

        _listener.Start();
        Log.BrokerStarted(_logger);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);

        while (!linkedCts.Token.IsCancellationRequested)
        {
            try
            {
                var client = await _listener.AcceptTcpClientAsync(linkedCts.Token);
                var clientTask = HandleClientAsync(client, _shutdownCts.Token);
                _clientTasks.Add(clientTask);
                _clientTasks.RemoveAll(t => t.IsCompleted);
            }
            catch (OperationCanceledException) { break; }
            catch (SocketException) { break; } // Listener was stopped
            catch (ObjectDisposedException) { break; } // Listener was disposed
            catch (Exception ex) { Log.ErrorAcceptingClient(_logger, ex); }
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        using var _ = client;
        var endpoint = client.Client.RemoteEndPoint;
        Log.ClientConnected(_logger, endpoint);

        _metrics.RecordConnectionOpened();
        _metrics.IncrementActiveConnections();

        var clientHost = endpoint is IPEndPoint ipEndpoint ? ipEndpoint.Address.ToString() : "*";

        try
        {
#pragma warning disable CA2000
            Stream stream;
            if (_tlsHandler != null)
            {
                try
                {
                    Log.TlsHandshakeStarting(_logger, endpoint);
                    stream = await _tlsHandler.AuthenticateAsServerAsync(client.GetStream(), cancellationToken);
#pragma warning restore CA2000
                    Log.TlsHandshakeCompleted(_logger, endpoint);
                }
                catch (Exception ex)
                {
                    Log.TlsHandshakeFailed(_logger, ex, endpoint);
                    _metrics.RecordError("tls_handshake_failed");
                    return;
                }
            }
            else
            {
                stream = client.GetStream();
            }

            await using var _stream = stream;
            await HandleAsync(stream, clientHost, endpoint, cancellationToken);
        }
        catch (Exception ex)
        {
            Log.ClientError(_logger, ex, endpoint);
            _metrics.RecordError("client_error");
        }
        finally
        {
            _metrics.DecrementActiveConnections();
            Log.ClientDisconnected(_logger, endpoint);
        }
    }

    /// <summary>
    /// Transport-neutral entry point: peeks the first four bytes on <paramref name="stream"/> and
    /// hands the connection to the first registered <see cref="IConnectionHandler"/> that claims
    /// the peeked magic (native first, then plugin-provided fallbacks like Kafka). Used both by the
    /// broker's own TCP accept loop and by alternative transports (QUIC, shared memory) that plug
    /// in via <see cref="SurgewaveStreamHandlerHolder"/>.
    /// </summary>
    public async Task HandleAsync(
        Stream stream,
        string clientHost,
        EndPoint? endpoint,
        CancellationToken cancellationToken)
    {
        // Read exactly 4 bytes for the magic-byte probe. Short reads happen on slow
        // links (and especially on QUIC where each datagram may deliver 1–2 bytes
        // depending on pacing), so loop until we've got all four.
        var magicBuffer = new byte[4];
        var totalRead = 0;
        while (totalRead < 4)
        {
            var n = await stream.ReadAsync(magicBuffer.AsMemory(totalRead, 4 - totalRead), cancellationToken);
            if (n == 0)
            {
                Log.EndOfStream(_logger, endpoint);
                return;
            }
            totalRead += n;
        }

        // Hand the connection to the first registered protocol handler that claims the peeked
        // magic bytes. The registry is frozen at construction and walked once per CONNECTION —
        // never per request (#59). Native registers at Order 0; Kafka (when enabled) is the
        // catch-all fallback. In native-only mode no fallback is registered, so a non-native
        // peer is closed.
        foreach (var handler in _connectionHandlers)
        {
            if (handler.CanHandle(magicBuffer))
            {
                await handler.HandleConnectionAsync(
                    stream, magicBuffer, new ConnectionContext(clientHost, endpoint), cancellationToken);
                return;
            }
        }

        Log.KafkaDisabled(_logger, endpoint);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        Log.ShutdownStarting(_logger);
        _listener.Stop();
        Log.ShutdownStoppedListener(_logger);

        if (_clientTasks.Count > 0)
        {
            Log.ShutdownWaitingForClients(_logger, _clientTasks.Count);
            var shutdownTimeout = TimeSpan.FromSeconds(_config.ShutdownTimeoutSeconds);

            try { await Task.WhenAll(_clientTasks).WaitAsync(shutdownTimeout); }
            catch (TimeoutException) { Log.ShutdownClientTimeout(_logger, _clientTasks.Count(t => !t.IsCompleted)); }
            catch (Exception ex) { Log.ShutdownClientError(_logger, ex); }
        }

        _shutdownCts.Cancel();

        Log.ShutdownDisposingResources(_logger);
        _logManager.Dispose();
        _tlsHandler?.Dispose();
        // Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory
        _nativeLoggerFactory.Dispose();
        _connectorRepositoryManager.Dispose();
        _shutdownCts.Dispose();
        _listener.Dispose();

        _disposed = true;
        Log.ShutdownComplete(_logger);
    }
}
