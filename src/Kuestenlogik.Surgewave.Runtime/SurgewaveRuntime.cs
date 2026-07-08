using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Runtime;

/// <summary>
/// A Surgewave broker runtime that can run embedded or standalone.
/// Use <see cref="CreateBuilder"/> for fluent configuration.
/// </summary>
/// <example>
/// <code>
/// // Testing with in-memory storage and auto-port
/// await using var surgewave = await SurgewaveRuntime.CreateBuilder()
///     .WithPort(0)
///     .WithStorageEngine(StorageEngines.Memory)
///     .Build()
///     .StartAsync();
///
/// // Production with persistent storage
/// await using var surgewave = await SurgewaveRuntime.CreateBuilder()
///     .WithPort(9092)
///     .WithDataDirectory("./data")
///     .Build()
///     .StartAsync();
/// </code>
/// </example>
#pragma warning disable CA2000 // Objects passed to SurgewaveBroker are disposed by the broker
public sealed class SurgewaveRuntime : IAsyncDisposable
{
    private readonly SurgewaveRuntimeOptions _options;
    private readonly string _dataDirectory;
    private readonly bool _ownsDataDirectory;
    private readonly ILoggerFactory _loggerFactory;

    private SurgewaveBroker? _broker;
    private LogManager? _logManager;
    private OffsetStore? _offsetStore;
    private TransactionStateStore? _transactionStateStore;
    private BrokerMetrics? _metrics;
    private QuotaManager? _quotaManager;
    private CancellationTokenSource? _cts;
    private Task? _brokerTask;
    private int _actualPort;
    private int _actualReplicationPort;
    private bool _disposed;

    // Handler references for cluster wiring
    private TopicAdminHandler? _topicAdminHandler;
    private MetadataApiHandler? _metadataApiHandler;
    // Kept so the inter-broker handler (registered after the broker is up)
    // can share the same coordinator instance for WriteTxnMarkers (#69).
    // Owned and disposed by SurgewaveBroker (see the CA2000 note above), so
    // the runtime only holds a reference for wiring — not for disposal.
#pragma warning disable CA2213
    private TransactionCoordinator? _transactionCoordinator;
#pragma warning restore CA2213

    // Cluster components (only initialized when EnableCluster = true)
    private ClusterState? _clusterState;
    private ClusterController? _clusterController;
    private HeartbeatManager? _heartbeatManager;
    private ReplicaManager? _replicaManager;
    private ReplicationServer? _replicationServer;
    private ConnectionPool? _connectionPool;
    private ControllerClient? _controllerClient;
    private Task? _clusterTask;

    // Raft components (only initialized when UseRaftConsensus = true)
    private RaftNode? _raftNode;
    private RaftPersistence? _raftPersistence;
    private RaftTransport? _raftTransport;
    private MetadataStateMachine? _metadataStateMachine;

    /// <summary>
    /// The actual port the broker is listening on.
    /// Useful when Port was set to 0 for automatic assignment.
    /// </summary>
    public int Port => _actualPort;

    /// <summary>
    /// The host address the broker is bound to.
    /// </summary>
    public string Host => _options.Host;

    /// <summary>
    /// Bootstrap servers string for Kafka clients (host:port).
    /// </summary>
    public string BootstrapServers => $"{Host}:{Port}";

    /// <summary>
    /// The data directory where logs are stored.
    /// </summary>
    public string DataDirectory => _dataDirectory;

    /// <summary>
    /// The broker ID.
    /// </summary>
    public int BrokerId => _options.BrokerId;

    /// <summary>
    /// The underlying SurgewaveBroker instance.
    /// </summary>
    public SurgewaveBroker Broker => _broker ?? throw new InvalidOperationException("Broker not started");

    /// <summary>
    /// The LogManager for direct log access.
    /// </summary>
    public LogManager LogManager => _logManager ?? throw new InvalidOperationException("Broker not started");

    /// <summary>
    /// Whether cluster mode is enabled.
    /// </summary>
    public bool IsClusterEnabled => _options.EnableCluster;

    /// <summary>
    /// The actual replication port (only valid when EnableCluster = true).
    /// </summary>
    public int ReplicationPort => _actualReplicationPort;

    /// <summary>
    /// The ClusterState (only valid when EnableCluster = true).
    /// </summary>
    public ClusterState? ClusterState => _clusterState;

    /// <summary>
    /// Whether this broker is currently the controller (only valid when EnableCluster = true).
    /// </summary>
    public bool IsController => _clusterController?.IsController ?? false;

    /// <summary>
    /// Whether this broker is the Raft leader (only valid when UseRaftConsensus = true).
    /// </summary>
    public bool IsRaftLeader => _raftNode?.IsLeader ?? false;

    /// <summary>
    /// The RaftNode instance (only valid when UseRaftConsensus = true).
    /// </summary>
    public RaftNode? RaftNode => _raftNode;

    private SurgewaveRuntime(SurgewaveRuntimeOptions options)
    {
        _options = options;
        _loggerFactory = options.LoggerFactory ?? NullLoggerFactory.Instance;

        if (options.DataDirectory != null)
        {
            _dataDirectory = options.DataDirectory;
            _ownsDataDirectory = false;
        }
        else
        {
            _dataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-runtime", Guid.NewGuid().ToString("N"));
            _ownsDataDirectory = true;
        }
    }

    /// <summary>
    /// Create a builder for fluent configuration.
    /// </summary>
    public static SurgewaveRuntimeBuilder CreateBuilder() => new();

    /// <summary>
    /// Start a Surgewave broker with the specified options.
    /// </summary>
    public static async Task<SurgewaveRuntime> StartAsync(
        SurgewaveRuntimeOptions options,
        CancellationToken cancellationToken = default)
    {
        var instance = new SurgewaveRuntime(options);
        await instance.StartInternalAsync(cancellationToken);
        return instance;
    }

    private async Task StartInternalAsync(CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(_dataDirectory);

        // Find available ports if using automatic assignment
        _actualPort = _options.Port == 0 ? FindAvailablePort(_options.EnableDualMode) : _options.Port;
        _actualReplicationPort = _options.ReplicationPort == 0 ? FindAvailablePort(_options.EnableDualMode) : _options.ReplicationPort;

        // Build cluster nodes string including this broker
        var clusterNodesStr = BuildClusterNodesString();

        var config = new BrokerConfig
        {
            Host = _options.Host,
            Port = _actualPort,
            EnableDualMode = _options.EnableDualMode,
            BrokerId = _options.BrokerId,
            DataDirectory = _dataDirectory,
            AutoCreateTopics = _options.AutoCreateTopics,
            DefaultNumPartitions = _options.DefaultNumPartitions,
            DefaultReplicationFactor = _options.DefaultReplicationFactor,
            ShutdownTimeoutSeconds = _options.ShutdownTimeoutSeconds,
            // Cluster settings
            ReplicationPort = _actualReplicationPort,
            ClusterNodes = clusterNodesStr,
            UseRaftConsensus = _options.UseRaftConsensus,
            HeartbeatIntervalMs = _options.HeartbeatIntervalMs,
            HeartbeatTimeoutMs = _options.HeartbeatTimeoutMs,
            // Raft settings
            RaftDataDirectory = Path.Combine(_dataDirectory, "raft"),
            RaftElectionTimeoutMinMs = _options.RaftElectionTimeoutMinMs,
            RaftElectionTimeoutMaxMs = _options.RaftElectionTimeoutMaxMs,
            RaftHeartbeatIntervalMs = _options.RaftHeartbeatIntervalMs,
            RaftPeerDiscoveryTimeoutSeconds = _options.RaftPeerDiscoveryTimeoutSeconds
        };

        // Set security settings on nested Security config
        config.Security.SaslEnabled = _options.EnableSasl;
        config.Security.TlsEnabled = _options.EnableTls;
        config.Security.AclEnabled = _options.EnableAcl;

        var retentionPolicy = new RetentionPolicy
        {
            RetentionHours = _options.RetentionHours,
            RetentionBytes = _options.RetentionBytes
        };

        // When Raft is enabled, don't persist topics to file
        var persistTopicsToFile = !_options.UseRaftConsensus;

        // Create segment factory: custom factory takes precedence over storage engine
        ILogSegmentFactory segmentFactory;
        if (_options.CustomLogSegmentFactory != null)
        {
            segmentFactory = _options.CustomLogSegmentFactory();
        }
        else
        {
            segmentFactory = string.Equals(_options.StorageEngine, StorageEngines.Memory, StringComparison.OrdinalIgnoreCase)
                ? new MemoryLogSegmentFactory()
                : FileLogSegmentFactory.Create(useMmap: true);
        }

        _logManager = new LogManager(_dataDirectory, segmentFactory, retentionPolicy: retentionPolicy, persistTopicsToFile: persistTopicsToFile);

        var brokerLogger = _loggerFactory.CreateLogger<SurgewaveBroker>();
        var serializerLogger = _loggerFactory.CreateLogger<RecordBatchSerializer>();
        var coordinatorLogger = _loggerFactory.CreateLogger<ConsumerGroupCoordinator>();
        var txnCoordinatorLogger = _loggerFactory.CreateLogger<TransactionCoordinator>();
        var txnStateStoreLogger = _loggerFactory.CreateLogger<TransactionStateStore>();
        var offsetStoreLogger = _loggerFactory.CreateLogger<OffsetStore>();
        var nativeGroupCoordinatorLogger = _loggerFactory.CreateLogger<NativeGroupCoordinator>();
        var quotaManagerLogger = _loggerFactory.CreateLogger<QuotaManager>();

        var recordBatchSerializer = new RecordBatchSerializer(serializerLogger);
        _offsetStore = new OffsetStore(_dataDirectory, offsetStoreLogger);
        var nativeGroupCoordinator = new NativeGroupCoordinator(nativeGroupCoordinatorLogger, _offsetStore);
        var queueViewConfig = new Kuestenlogik.Surgewave.Broker.Queue.QueueViewConfig();
        var queueViewManager = new Kuestenlogik.Surgewave.Broker.Queue.QueueViewManager(queueViewConfig, _loggerFactory, _logManager);
        var shareGroupCoordinatorLogger = _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupCoordinator>();
        var shareGroupCoordinator = new Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupCoordinator(shareGroupCoordinatorLogger, _logManager, queueViewManager);
        var consumerGroupV2Coordinator = new Kuestenlogik.Surgewave.Broker.ConsumerGroupV2.ConsumerGroupV2Coordinator(
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.ConsumerGroupV2.ConsumerGroupV2Coordinator>(), _logManager);
        // Classic coordinator gets the v2 reference so OffsetCommit/Fetch from a
        // KIP-848 consumer routes through v2 epoch validation when the group is not
        // in the classic state map.
        var consumerGroupCoordinator = new ConsumerGroupCoordinator(
            coordinatorLogger, _offsetStore,
            v2Coordinator: consumerGroupV2Coordinator);
        var streamsGroupCoordinator = new Kuestenlogik.Surgewave.Broker.StreamsGroups.StreamsGroupCoordinator(
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.StreamsGroups.StreamsGroupCoordinator>(), _logManager);
        var producerStateManager = new ProducerStateManager();
        var transactionIndex = new TransactionIndex();
        _transactionStateStore = new TransactionStateStore(_dataDirectory, txnStateStoreLogger);
        var transactionCoordinator = new TransactionCoordinator(
            producerStateManager, _logManager, transactionIndex, _offsetStore, _transactionStateStore, txnCoordinatorLogger);
        _transactionCoordinator = transactionCoordinator;
        _quotaManager = new QuotaManager(config.Quotas, quotaManagerLogger);
        // Kafka wire protocol handler — built only when EnableKafka (#58);
        // null means the embedded broker runs native-only.
        IProtocolHandler? protocolHandler = _options.EnableKafka ? new KafkaProtocolHandler() : null;

        _metrics = new BrokerMetrics();

        // Create dynamic broker config for runtime config modifications
        var dynamicBrokerConfigLogger = _loggerFactory.CreateLogger<DynamicBrokerConfig>();
        var dynamicBrokerConfig = new DynamicBrokerConfig(config, dynamicBrokerConfigLogger);

        // Kafka handler array + dispatcher — built only when EnableKafka (#58).
        // When disabled the embedded broker is native-only: no handlers, no
        // dispatcher; SurgewaveBroker then rejects Kafka connections.
        RequestDispatcher? dispatcher = null;
        if (_options.EnableKafka)
        {
            // Create request handlers
            var dataApiLogger = _loggerFactory.CreateLogger<DataApiHandler>();
            var metadataApiLogger = _loggerFactory.CreateLogger<MetadataApiHandler>();
            var topicAdminLogger = _loggerFactory.CreateLogger<TopicAdminHandler>();
            _topicAdminHandler = new TopicAdminHandler(config, _logManager, _quotaManager, auditLogger: null, topicAdminLogger);
            _metadataApiHandler = new MetadataApiHandler(config, _logManager, metadataApiLogger);
            IKafkaRequestHandler[] handlers =
            [
                new DataApiHandler(config, _logManager, transactionCoordinator, _quotaManager, recordBatchSerializer, aclAuthorizer: null, deduplicationManager: null, delayIndex: null, ttlIndex: null, _metrics, dataApiLogger, partitionAppender: _options.PartitionAppender, disaggregatedReader: _options.DisaggregatedReader),
                _metadataApiHandler,
                _topicAdminHandler,
                new ConfigApiHandler(config, dynamicBrokerConfig, _logManager),
                new SecurityApiHandler(config, saslAuthenticator: null, aclAuthorizer: null, auditLogger: null, _loggerFactory.CreateLogger<SecurityApiHandler>()),
                // Streams-group now routes through the dispatcher (the broker fast-path was removed
                // when the coordinator went protocol-neutral, #59), so the adapter must be registered here too.
                new StreamsGroupApiHandler(streamsGroupCoordinator, _loggerFactory.CreateLogger<StreamsGroupApiHandler>()),
                new TelemetryApiHandler(
                    _loggerFactory.CreateLogger<TelemetryApiHandler>(),
                    config.Telemetry,
                    new Kuestenlogik.Surgewave.Broker.Telemetry.LoggingTelemetryIngestor(
                        _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.Telemetry.LoggingTelemetryIngestor>()))
            ];
            dispatcher = new RequestDispatcher(handlers);
        }

        _broker = new SurgewaveBroker(
            config, _logManager, recordBatchSerializer, consumerGroupCoordinator, shareGroupCoordinator, nativeGroupCoordinator,
            transactionCoordinator, _quotaManager, protocolHandler, _metrics, dispatcher, brokerLogger,
            consumerGroupV2Coordinator: consumerGroupV2Coordinator);

        _cts = new CancellationTokenSource();
        _brokerTask = Task.Run(() => _broker.StartAsync(_cts.Token), cancellationToken);

        // Wait for broker to be ready
        await WaitForBrokerReadyAsync(cancellationToken);

        // Initialize cluster components if enabled
        if (_options.EnableCluster)
        {
            await InitializeClusterAsync(config, cancellationToken);
        }
    }

    private async Task InitializeClusterAsync(BrokerConfig config, CancellationToken cancellationToken)
    {
        var clusterControllerLogger = _loggerFactory.CreateLogger<ClusterController>();
        var heartbeatLogger = _loggerFactory.CreateLogger<HeartbeatManager>();
        var replicaManagerLogger = _loggerFactory.CreateLogger<ReplicaManager>();
        var replicationServerLogger = _loggerFactory.CreateLogger<ReplicationServer>();

        // Create clustering config from broker config
        var clusteringConfig = ClusteringConfig.Create(
            brokerId: config.BrokerId,
            host: config.Host,
            port: config.Port,
            rack: config.Rack,
            clusterId: config.ClusterId,
            dataDirectory: config.DataDirectory,
            clusterNodes: config.ClusterNodes,
            replicationPort: config.ReplicationPort,
            minInSyncReplicas: config.MinInSyncReplicas,
            allowAutoLeaderRebalance: config.AllowAutoLeaderRebalance,
            leaderImbalanceCheckIntervalSeconds: config.LeaderImbalanceCheckIntervalSeconds,
            controlledShutdownMaxRetries: config.ControlledShutdownMaxRetries,
            heartbeatIntervalMs: config.HeartbeatIntervalMs,
            heartbeatTimeoutMs: config.HeartbeatTimeoutMs,
            maxHeartbeatFailures: config.MaxHeartbeatFailures,
            useRaftConsensus: config.UseRaftConsensus,
            raftDataDirectory: config.RaftDataDirectory,
            raftElectionTimeoutMinMs: config.RaftElectionTimeoutMinMs,
            raftElectionTimeoutMaxMs: config.RaftElectionTimeoutMaxMs,
            raftHeartbeatIntervalMs: config.RaftHeartbeatIntervalMs,
            raftPeerDiscoveryTimeoutSeconds: config.RaftPeerDiscoveryTimeoutSeconds,
            autoRebalanceEnabled: config.AutoRebalanceEnabled,
            rebalanceCheckIntervalSeconds: config.RebalanceCheckIntervalSeconds,
            rebalanceImbalanceThreshold: config.RebalanceImbalanceThreshold,
            reassignmentThrottleBytesPerSec: config.ReassignmentThrottleBytesPerSec,
            reassignmentMaxConcurrent: config.ReassignmentMaxConcurrent
        );

        // Create cluster state
        _clusterState = new ClusterState();

        // Register this broker with actual replication port
        _clusterState.AddBroker(new BrokerNode
        {
            BrokerId = _options.BrokerId,
            Host = _options.Host,
            Port = _actualPort,
            ReplicationPort = _actualReplicationPort
        });
        _clusterState.LocalBrokerId = _options.BrokerId;

        // Inter-broker peer transport (TCP for embedded runtime; QUIC support
        // would require msquic and mutual-auth setup — opted out here).
        //
        // Explicitly invoke the TCP registration so the module initializer
        // runs even when no call site in this assembly references a type
        // from Kuestenlogik.Surgewave.Transport.Tcp. Without this, test hosts that pull
        // SurgewaveRuntime via its assembly-level reference still see the
        // factory empty because the JIT hasn't loaded the TCP assembly yet.
        Kuestenlogik.Surgewave.Transport.Tcp.TcpTransportRegistration.Register();
        var peerTransport = Kuestenlogik.Surgewave.Transport.PeerTransportFactory.Create(clusteringConfig.InterBrokerTransport);

        // Create heartbeat manager
        _heartbeatManager = new HeartbeatManager(
            heartbeatLogger,
            _clusterState,
            clusteringConfig);

        // Create the controller client FIRST — it doubles as the leader-side
        // IIsrChangeNotifier that ReplicaManager fires on ISR change (reverse
        // ISR propagation, #69). It depends only on peerTransport/state/config,
        // so there is no cycle with ReplicaManager/ClusterController.
        var connectionPoolLogger = _loggerFactory.CreateLogger<ConnectionPool>();
        var controllerClientLogger = _loggerFactory.CreateLogger<ControllerClient>();
        _connectionPool = new ConnectionPool(connectionPoolLogger, peerTransport);
        _controllerClient = new ControllerClient(_connectionPool, _clusterState, clusteringConfig, controllerClientLogger);

        // Create replica manager, wiring the notifier so a leader reports ISR
        // growth back to the controller.
        _replicaManager = new ReplicaManager(
            replicaManagerLogger,
            _clusterState,
            _logManager!,
            clusteringConfig,
            peerTransport,
            isrChangeNotifier: _controllerClient);

        // Create replication server
        _replicationServer = new ReplicationServer(
            replicationServerLogger,
            _clusterState,
            _logManager!,
            _replicaManager,
            clusteringConfig,
            peerTransport);

        // Create cluster controller — it is the controller-side IIsrUpdateApplier.
        _clusterController = new ClusterController(
            clusterControllerLogger,
            _clusterState,
            _replicaManager,
            clusteringConfig);

        // Wire up heartbeat manager
        _clusterController.SetHeartbeatManager(_heartbeatManager);
        _replicationServer.SetHeartbeatManager(_heartbeatManager);

        // Wire up the controller client so the controller can push LeaderAndIsr
        // to remote brokers when topology changes (topic create, reelection).
        // Without this the controller updates only its own local state and
        // followers never learn to fetch, so the ISR never grows past {leader}.
        _clusterController.SetControllerClient(_controllerClient);

        // Register the inter-broker API handler now that the cluster components
        // exist. It handles LeaderAndIsr / StopReplica / UpdateMetadata pushed
        // by the controller over the client port (turning them into
        // BecomeLeader/BecomeFollower calls), plus AlterPartition reported by
        // leaders — applied via the controller (IIsrUpdateApplier). The broker
        // is already serving, so we hot-add it (#69).
        // The inter-broker control plane rides the Kafka wire today, so it only
        // exists when Kafka is enabled; a native-only broker is single-broker
        // until the control path is native (#60).
        if (_options.EnableKafka)
        {
            var interBrokerApiLogger = _loggerFactory.CreateLogger<InterBrokerApiHandler>();
            _broker!.AddHandler(new InterBrokerApiHandler(
                config, _clusterState, _replicaManager, _logManager!, interBrokerApiLogger,
                _transactionCoordinator, isrUpdateApplier: _clusterController));
        }

        // Wire up topic admin handler to use cluster controller for topic creation
        _topicAdminHandler?.SetClusterTopicCreator(_clusterController);

        // Wire up metadata handler to use cluster state for accurate replica info
        _metadataApiHandler?.SetClusterState(_clusterState);
        _metadataApiHandler?.SetClusterTopicCreator(_clusterController);

        // Initialize Raft components if enabled
        if (_options.UseRaftConsensus)
        {
            var raftNodeLogger = _loggerFactory.CreateLogger<RaftNode>();
            var raftPersistenceLogger = _loggerFactory.CreateLogger<RaftPersistence>();
            var raftTransportLogger = _loggerFactory.CreateLogger<RaftTransport>();
            var metadataStateMachineLogger = _loggerFactory.CreateLogger<MetadataStateMachine>();

            _raftPersistence = new RaftPersistence(raftPersistenceLogger, clusteringConfig);
            _raftTransport = new RaftTransport(raftTransportLogger, _clusterState, clusteringConfig, peerTransport);
            _metadataStateMachine = new MetadataStateMachine(metadataStateMachineLogger, _clusterState);

            _raftNode = new RaftNode(
                raftNodeLogger,
                clusteringConfig,
                _raftPersistence,
                _raftTransport,
                _metadataStateMachine);

            // Wire up Raft node with controller and replication server
            _clusterController.SetRaftNode(_raftNode);
            _replicationServer.SetRaftNode(_raftNode);
        }

        // Start cluster components
        _clusterTask = Task.Run(async () =>
        {
            var replicationTask = _replicationServer.StartAsync(_cts!.Token);
            var heartbeatTask = _heartbeatManager.StartAsync(_cts.Token);
            var controllerTask = _clusterController.StartAsync(_cts.Token);
            var replicaManagerTask = _replicaManager.StartAsync(_cts.Token);

            await Task.WhenAll(replicationTask, heartbeatTask, controllerTask, replicaManagerTask);
        }, cancellationToken);

        // Wait for replication server to be ready
        await WaitForReplicationServerReadyAsync(cancellationToken);
    }

    private async Task WaitForReplicationServerReadyAsync(CancellationToken cancellationToken, int maxRetries = 50)
    {
        for (int i = 0; i < maxRetries; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                using var client = new TcpClient();
                await client.ConnectAsync(_options.Host, _actualReplicationPort, cancellationToken);
                return;
            }
            catch (SocketException)
            {
                await Task.Delay(50, cancellationToken);
            }
        }

        throw new InvalidOperationException($"Replication server did not start within {maxRetries * 50}ms");
    }

    private string BuildClusterNodesString()
    {
        if (!_options.EnableCluster || _options.ClusterNodes.Count == 0)
        {
            return string.Empty;
        }

        // Include this broker and all configured nodes
        var thisNode = $"{_options.BrokerId}:{_options.Host}:{_actualPort}:{_actualReplicationPort}";
        var allNodes = new List<string> { thisNode };
        allNodes.AddRange(_options.ClusterNodes);

        return string.Join(",", allNodes.Distinct());
    }

    private async Task WaitForBrokerReadyAsync(CancellationToken cancellationToken, int maxRetries = 50)
    {
        for (int i = 0; i < maxRetries; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                using var client = new TcpClient();
                await client.ConnectAsync(_options.Host, _actualPort, cancellationToken);
                return;
            }
            catch (SocketException)
            {
                await Task.Delay(50, cancellationToken);
            }
        }

        throw new InvalidOperationException($"Surgewave broker did not start within {maxRetries * 50}ms");
    }

    private static int FindAvailablePort(bool enableDualMode)
    {
        IPAddress bindAddress;
        if (enableDualMode)
        {
            bindAddress = IPAddress.IPv6Any;
        }
        else
        {
            bindAddress = IPAddress.Loopback;
        }

        using var listener = new TcpListener(bindAddress, 0);
        if (enableDualMode)
        {
            listener.Server.DualMode = true;
        }
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>
    /// Initiates a graceful shutdown, transferring leadership and notifying the cluster.
    /// </summary>
    /// <param name="timeout">Maximum time to wait for graceful shutdown</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>True if shutdown was graceful</returns>
    public async Task<bool> GracefulShutdownAsync(TimeSpan timeout, CancellationToken ct = default)
    {
        if (_clusterController != null)
        {
            return await _clusterController.GracefulShutdownAsync(timeout, ct);
        }

        // Not in cluster mode - graceful shutdown is trivial
        return true;
    }

    /// <summary>
    /// Stop the broker and clean up resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Phase 1: Graceful shutdown (if in cluster mode)
        if (_clusterController != null)
        {
            try
            {
                var gracefulTimeout = TimeSpan.FromSeconds(_options.ShutdownTimeoutSeconds / 2);
                await GracefulShutdownAsync(gracefulTimeout);
            }
            catch (Exception)
            {
                // Graceful shutdown failed, proceed with hard shutdown
            }
        }

        // Phase 2: Signal cancellation for remaining tasks
        if (_cts != null)
        {
            await _cts.CancelAsync();
            _cts.Dispose();
        }

        // Wait for cluster task
        if (_clusterTask != null)
        {
            try
            {
                await _clusterTask;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        // Wait for broker task
        if (_brokerTask != null)
        {
            try
            {
                await _brokerTask;
            }
            catch (OperationCanceledException)
            {
                // Expected during normal shutdown
            }
            catch (Exception)
            {
                // Startup may have failed. Swallow to avoid confusing cleanup failures.
            }
        }

        // Dispose Raft components first
        if (_raftNode != null)
            await _raftNode.DisposeAsync();
        if (_raftTransport != null)
            await _raftTransport.DisposeAsync();

        // Dispose cluster components
        if (_clusterController != null)
            await _clusterController.DisposeAsync();
        if (_heartbeatManager != null)
            await _heartbeatManager.DisposeAsync();
        if (_replicaManager != null)
            await _replicaManager.DisposeAsync();
        if (_replicationServer != null)
            await _replicationServer.DisposeAsync();

        // Dispose the controller client and its connection pool (runtime-owned).
        _controllerClient?.Dispose();
        _connectionPool?.Dispose();

        // Dispose broker
        if (_broker != null)
        {
            await _broker.DisposeAsync();
        }

        // Dispose other resources
        _metrics?.Dispose();
        _quotaManager?.Dispose();
        _transactionStateStore?.Dispose();
        _offsetStore?.Dispose();
        _logManager?.Dispose();

        // Clean up data directory if we created it and cleanup is enabled
        if (_ownsDataDirectory && _options.CleanupOnDispose)
        {
            try
            {
                if (Directory.Exists(_dataDirectory))
                {
                    Directory.Delete(_dataDirectory, recursive: true);
                }
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}
