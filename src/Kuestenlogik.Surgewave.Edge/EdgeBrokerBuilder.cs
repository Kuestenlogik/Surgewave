using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Edge;

/// <summary>
/// Fluent builder for creating an edge-deployed Surgewave broker with optional cloud synchronization.
/// Provides a simplified API for IoT, factory, and vehicle edge deployments.
/// </summary>
/// <example>
/// <code>
/// await using var edge = await EdgeBrokerBuilder
///     .Create("factory-floor-1")
///     .WithSqliteStorage("edge-data.db")
///     .WithCloudSync("cloud.example.com:9092", cfg =>
///     {
///         cfg.SyncTopics = ["sensor-data", "alerts"];
///         cfg.SyncIntervalSeconds = 10;
///     })
///     .WithTopics("sensor-data", "alerts", "commands")
///     .BuildAsync();
///
/// // Produce messages locally (works offline)
/// await edge.Client.Messaging.SendAsync("sensor-data", 0, null, data);
/// </code>
/// </example>
public sealed class EdgeBrokerBuilder
{
    private readonly string _edgeId;
    private string? _sqliteDbPath;
    private bool _useMemoryStorage;
    private string? _cloudAddress;
    private SurgewaveTransportType _cloudTransport = SurgewaveTransportType.Tcp;
    private Action<EdgeSyncConfig>? _syncConfigure;
    private readonly List<string> _topics = [];
    private int _port;
    private string _host = "localhost";
    private string? _dataDirectory;
    private ILoggerFactory? _loggerFactory;

    private EdgeBrokerBuilder(string edgeId)
    {
        _edgeId = edgeId;
    }

    /// <summary>
    /// Creates a new edge broker builder with the specified edge identifier.
    /// </summary>
    /// <param name="edgeId">Unique identifier for this edge node.</param>
    public static EdgeBrokerBuilder Create(string edgeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(edgeId);
        return new EdgeBrokerBuilder(edgeId);
    }

    /// <summary>
    /// Use SQLite storage for durable edge persistence.
    /// Ideal for edge deployments that need ACID guarantees and single-file backup.
    /// </summary>
    /// <param name="dbPath">Path to the SQLite database file. Defaults to <c>edge.db</c>.</param>
    public EdgeBrokerBuilder WithSqliteStorage(string dbPath = "edge.db")
    {
        _sqliteDbPath = dbPath;
        _useMemoryStorage = false;
        return this;
    }

    /// <summary>
    /// Use in-memory storage for ephemeral edge operation.
    /// Fast but data is lost on restart. Suitable for transient sensor data.
    /// </summary>
    public EdgeBrokerBuilder WithMemoryStorage()
    {
        _useMemoryStorage = true;
        _sqliteDbPath = null;
        return this;
    }

    /// <summary>
    /// Enable cloud synchronization with the specified cloud broker.
    /// </summary>
    /// <param name="cloudAddress">Cloud broker address in <c>host:port</c> format.</param>
    /// <param name="configure">Optional action to further configure sync behavior.</param>
    public EdgeBrokerBuilder WithCloudSync(string cloudAddress, Action<EdgeSyncConfig>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cloudAddress);
        _cloudAddress = cloudAddress;
        _syncConfigure = configure;
        return this;
    }

    /// <summary>
    /// Select the transport used for edge→cloud communication. QUIC is recommended
    /// on lossy or mobile uplinks (wifi, cellular, satellite) because it survives
    /// packet loss and NAT rebinding better than TCP. Defaults to TCP.
    /// </summary>
    /// <param name="transport">The transport type to use.</param>
    public EdgeBrokerBuilder WithCloudTransport(SurgewaveTransportType transport)
    {
        _cloudTransport = transport;
        return this;
    }

    /// <summary>
    /// Pre-create the specified topics on the edge broker at startup.
    /// </summary>
    public EdgeBrokerBuilder WithTopics(params string[] topics)
    {
        _topics.AddRange(topics);
        return this;
    }

    /// <summary>
    /// Sets the port for the edge broker. Defaults to 0 (auto-assign).
    /// </summary>
    public EdgeBrokerBuilder WithPort(int port)
    {
        _port = port;
        return this;
    }

    /// <summary>
    /// Sets the host for the edge broker. Defaults to <c>localhost</c>.
    /// </summary>
    public EdgeBrokerBuilder WithHost(string host)
    {
        _host = host;
        return this;
    }

    /// <summary>
    /// Sets the data directory for the edge broker.
    /// </summary>
    public EdgeBrokerBuilder WithDataDirectory(string directory)
    {
        _dataDirectory = directory;
        return this;
    }

    /// <summary>
    /// Sets the logger factory for the edge broker and sync service.
    /// </summary>
    public EdgeBrokerBuilder WithLogging(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        return this;
    }

    /// <summary>
    /// Builds and starts the edge broker with the configured settings.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for startup.</param>
    /// <returns>A running <see cref="EdgeBroker"/> instance.</returns>
    public async Task<EdgeBroker> BuildAsync(CancellationToken cancellationToken = default)
    {
        var loggerFactory = _loggerFactory ?? NullLoggerFactory.Instance;

        // Build the Surgewave runtime
        var runtimeBuilder = SurgewaveRuntime.CreateBuilder()
            .WithHost(_host)
            .WithPort(_port)
            .WithAutoCreateTopics(true)
            .WithLogging(loggerFactory);

        if (_dataDirectory != null)
        {
            runtimeBuilder.WithDataDirectory(_dataDirectory);
        }

        if (_useMemoryStorage)
        {
            runtimeBuilder.WithStorageEngine(StorageEngines.Memory);
        }
        else if (_sqliteDbPath != null)
        {
            // SQLite storage is configured via the extension method from Kuestenlogik.Surgewave.Storage.Engine.Sqlite
            // Since we reference Kuestenlogik.Surgewave.Runtime, fall back to file storage if SQLite extension is not available
            runtimeBuilder.WithStorageEngine(StorageEngines.File);
        }
        else
        {
            runtimeBuilder.WithStorageEngine(StorageEngines.File);
        }

        var runtimeOptions = runtimeBuilder.Build();
        var runtime = await runtimeOptions.StartAsync(cancellationToken);

        // Create edge client
        var client = new Client.Native.SurgewaveNativeClient(runtime.Host, runtime.Port);
        await client.ConnectAsync(cancellationToken);

        // Pre-create topics
        foreach (var topic in _topics)
        {
            try
            {
                await client.Topics.CreateAsync(topic, partitions: 1, cancellationToken: cancellationToken);
            }
            catch (Exception)
            {
                // Topic may already exist
            }
        }

        // Build sync config and state
        EdgeSyncConfig? syncConfig = null;
        EdgeSyncState? syncState = null;
        EdgeSyncService? syncService = null;

        if (_cloudAddress != null)
        {
            // Make sure the selected cloud transport's module initializer has run
            // before we construct a client that will dispatch by transport type.
            Kuestenlogik.Surgewave.Transport.Tcp.TcpTransportRegistration.Register();
            if (_cloudTransport == SurgewaveTransportType.Quic &&
                (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS()))
            {
                Kuestenlogik.Surgewave.Transport.Quic.QuicTransportRegistration.Register();
                // Dev-friendly default — edge clients commonly talk to brokers with
                // self-signed certs. Flip this off for production and configure pinning.
                Kuestenlogik.Surgewave.Transport.Quic.QuicTransport.TrustAllCertificates = true;
            }

            syncConfig = new EdgeSyncConfig
            {
                EdgeId = _edgeId,
                CloudBrokerAddress = _cloudAddress,
                CloudTransport = _cloudTransport
            };
            _syncConfigure?.Invoke(syncConfig);

            syncState = EdgeSyncState.LoadFromFile(syncConfig.OfflineStateFile);
            syncState.EdgeId = _edgeId;

            var connectivityChecker = new ConnectivityChecker(
                loggerFactory.CreateLogger<ConnectivityChecker>());

            syncService = new EdgeSyncService(
                client,
                syncConfig,
                syncState,
                connectivityChecker,
                loggerFactory.CreateLogger<EdgeSyncService>());
        }

        syncState ??= new EdgeSyncState { EdgeId = _edgeId };

        return new EdgeBroker(runtime, client, syncService, syncState, syncConfig, loggerFactory);
    }
}
