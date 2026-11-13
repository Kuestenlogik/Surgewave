using Kuestenlogik.Surgewave.Core.Configuration;
using System.Collections.ObjectModel;
using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
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
public sealed class SurgewaveRuntime : IAsyncDisposable
{
    private readonly SurgewaveRuntimeOptions _options;
    private readonly string _dataDirectory;
    private readonly bool _ownsDataDirectory;
    private readonly ILoggerFactory _loggerFactory;

    private SurgewaveBroker? _broker;
    private LogManager? _logManager;
    private OffsetStore? _offsetStore;
    private Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupCoordinator? _shareGroupCoordinator;
    private Kuestenlogik.Surgewave.Broker.Transactions.TransactionStateLog? _transactionStateLog;
    private Kuestenlogik.Surgewave.Broker.ShareGroups.ShareStateStore? _shareStateStore;
    private Kuestenlogik.Surgewave.Broker.ShareGroups.RoutingShareStatePersister? _shareStatePersister;
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
    private ConsumerGroupApiHandler? _consumerGroupApiHandler;
    private NativeGroupCoordinator? _nativeGroupCoordinator;
    private ConsumerGroupV2ApiHandler? _consumerGroupV2ApiHandler;
    private DataApiHandler? _dataApiHandler;
    // Kept so the inter-broker handler (registered after the broker is up) can share the same
    // coordinator instance for WriteTxnMarkers (#69). This runtime creates it, so this runtime
    // disposes it — the note that used to stand here said SurgewaveBroker owned it, which was
    // never true: the broker's DisposeAsync has never touched it, so every embedded runtime left
    // the coordinator's transaction-timeout timer running after shutdown. Broker.App is unaffected
    // — there it is a DI singleton and the container disposes it.
    private TransactionCoordinator? _transactionCoordinator;

    // Created here and therefore ended here. Both used to be locals that nobody disposed — the
    // queue-view manager holds per-view state, the telemetry ingestor its own Meter — and the
    // file-wide CA2000 suppression that hid them claimed SurgewaveBroker disposed what it was
    // passed, which it never did.
    private Kuestenlogik.Surgewave.Broker.Queue.QueueViewManager? _queueViewManager;
    private Kuestenlogik.Surgewave.Broker.Telemetry.LoggingTelemetryIngestor? _telemetryIngestor;

    // This broker's cluster id (#203): its cluster's, adopted from the metadata log, or its own without a cluster.
    private ClusterIdManager? _clusterIdentity;

    // Cluster components (only initialized when EnableCluster = true)
    private ClusterState? _clusterState;
    private ClusterController? _clusterController;
    private HeartbeatManager? _heartbeatManager;
    private ReplicaManager? _replicaManager;
    private ReplicationServer? _replicationServer;
    private ConnectionPool? _connectionPool;
    private ControllerClient? _controllerClient;
    private KafkaConnectionHandler? _kafkaConnectionHandler;
    private ConfigApiHandler? _configApiHandler;
    private Kuestenlogik.Surgewave.Clustering.Cluster.BrokerLifecycleLoop? _lifecycleLoop;
    private Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignmentManager? _reassignmentManager;
    private Task? _clusterTask;
    private BrokerSessionSweeper? _sessionSweeper;
    private ReplicationTransitionQueue? _replicationTransitions;

    // Raft components — every broker has a metadata log (#163 step 3).
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
    /// Whether this broker answers the Kafka API: a request for an API without a handler gets a frame without a body,
    /// which a Kafka client cannot read (#240).
    /// </summary>
    public bool HandlesKafkaApi(ApiKey apiKey) => _kafkaConnectionHandler?.HasHandler(apiKey) ?? false;

    /// <summary>
    /// This broker's cluster id (#203): configured, persisted, or adopted from its cluster's metadata log.
    /// </summary>
    public ClusterIdManager ClusterIdentity => _clusterIdentity ?? throw new InvalidOperationException("Broker not started");

    /// <summary>
    /// Whether this broker is currently the controller (only valid when EnableCluster = true).
    /// </summary>
    public bool IsController => _clusterController?.IsController ?? false;

    /// <summary>
    /// The startup handshake of a broker in a cluster (#250): completes when it has caught up with the metadata log —
    /// applied what the leader of the current term had committed, and taken up the partitions that gives it — at once
    /// if it already has, and at once without a cluster, where there is no such log. Cancelled with
    /// <paramref name="cancellationToken"/>, and when the broker stops first.
    /// </summary>
    /// <remarks>
    /// <see cref="StartAsync"/> waits for it itself where the broker is its whole quorum. A broker of a larger quorum
    /// gets there only once a majority of the voters runs, which is after the host has started them; until then it
    /// answers from metadata it has not applied, and a host that lets clients in waits for this first.
    /// </remarks>
    public Task WaitForMetadataCaughtUpAsync(CancellationToken cancellationToken = default) =>
        _clusterController?.WaitForMetadataCaughtUpAsync(cancellationToken) ?? Task.CompletedTask;

    /// <summary>
    /// Health of the peer brokers as seen by this broker's failure detector — the input to
    /// controller re-election and ISR shrinking. Empty until the first heartbeat is sent to a peer; never
    /// contains this broker itself, which is why it is not the place to ask whether the local
    /// broker is alive (<c>HeartbeatManager.IsBrokerAlive</c> answers that one directly).
    /// </summary>
    public IReadOnlyDictionary<int, BrokerHealthState> PeerHealth =>
        _heartbeatManager?.AllBrokerHealth ?? ReadOnlyDictionary<int, BrokerHealthState>.Empty;

    /// <summary>
    /// Completes at the next change of <see cref="PeerHealth"/>: a peer watched for the first time, reported failed,
    /// or recovered. Take it before looking, then await it. Never completes on a broker without a cluster.
    /// </summary>
    public Task PeerHealthChanged => _heartbeatManager?.HealthChanged ?? Task.Delay(Timeout.Infinite);

    /// <summary>
    /// Whether this broker is the Raft leader — that is, the active controller.
    /// </summary>
    public bool IsRaftLeader => _raftNode?.IsLeader ?? false;

    /// <summary>
    /// The RaftNode instance.
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
        try
        {
            await instance.StartInternalAsync(cancellationToken);
        }
        catch
        {
            // A start that failed leaves nothing behind: the caller gets no instance it could dispose.
            await instance.DisposeAsync();
            throw;
        }
        return instance;
    }

    private async Task StartInternalAsync(CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(_dataDirectory);

        // Find available ports if using automatic assignment
        // Both dynamic ports in one reservation: picked one after the other, the OS could hand the same
        // ephemeral port out twice and the broker failed validation with Port == ReplicationPort.
        var dynamicPorts = FindAvailablePorts(
            (_options.Port == 0 ? 1 : 0) + (_options.ReplicationPort == 0 ? 1 : 0), _options.EnableDualMode);
        var nextDynamic = 0;
        _actualPort = _options.Port == 0 ? dynamicPorts[nextDynamic++] : _options.Port;
        _actualReplicationPort = _options.ReplicationPort == 0 ? dynamicPorts[nextDynamic++] : _options.ReplicationPort;

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
            ReplicationBindAddress = LoopbackReplicationBindAddress(_options),
            ClusterNodes = clusterNodesStr,
            ProcessRoles = _options.ProcessRoles,
            ControllerQuorumVoters = _options.ControllerQuorumVoters,
            ClusterId = _options.ClusterId,
            OffsetsTopicNumPartitions = _options.OffsetsTopicNumPartitions ?? new BrokerConfig().OffsetsTopicNumPartitions,
            TransactionStateLogNumPartitions = _options.TransactionStateLogNumPartitions ?? new BrokerConfig().TransactionStateLogNumPartitions,
            ShareStateTopicNumPartitions = _options.ShareStateTopicNumPartitions ?? new BrokerConfig().ShareStateTopicNumPartitions,
            HeartbeatIntervalMs = _options.HeartbeatIntervalMs,
            HeartbeatTimeoutMs = _options.HeartbeatTimeoutMs,
            // Raft settings
            RaftDataDirectory = Path.Combine(_dataDirectory, "raft"),
            RaftElectionTimeoutMinMs = _options.RaftElectionTimeoutMinMs,
            RaftElectionTimeoutMaxMs = _options.RaftElectionTimeoutMaxMs,
            RaftHeartbeatIntervalMs = _options.RaftHeartbeatIntervalMs,
            RaftPeerDiscoveryTimeoutSeconds = _options.RaftPeerDiscoveryTimeoutSeconds
        };

        // The cluster id every client is told (#203). Checked before anything starts: a configured id that is not the
        // one this broker persisted stops it. A broker without a cluster is its own and has its id at once; in a
        // cluster the first controller writes one to the metadata log and every broker adopts it.
        _clusterIdentity = new ClusterIdManager(
            _options.ClusterId, _dataDirectory, _loggerFactory.CreateLogger<ClusterIdManager>());
        if (!_options.EnableCluster)
            _clusterIdentity.EnsureClusterId();
        var clusterIdentity = _clusterIdentity;
        config.UseClusterIdentity(() => clusterIdentity.ClusterId);

        // Set security settings on nested Security config
        config.Security.SaslEnabled = _options.EnableSasl;
        config.Security.TlsEnabled = _options.EnableTls;
        config.Security.AclEnabled = _options.EnableAcl;

        var retentionPolicy = new RetentionPolicy
        {
            RetentionHours = _options.RetentionHours,
            RetentionBytes = _options.RetentionBytes
        };

        // The metadata log is the source of truth for topics (#163 step 3).
        const bool persistTopicsToFile = false;

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
        var offsetStoreLogger = _loggerFactory.CreateLogger<OffsetStore>();
        var nativeGroupCoordinatorLogger = _loggerFactory.CreateLogger<NativeGroupCoordinator>();
        var quotaManagerLogger = _loggerFactory.CreateLogger<QuotaManager>();

        var recordBatchSerializer = new RecordBatchSerializer(serializerLogger);
        // Group offsets live in __consumer_offsets (#205): led locally by a broker on its own, followed through the
        // replica manager in a cluster.
        _offsetStore = new OffsetStore(_logManager, recordBatchSerializer, config.OffsetsTopicNumPartitions, offsetStoreLogger);
        var nativeGroupCoordinator = new NativeGroupCoordinator(nativeGroupCoordinatorLogger, _offsetStore);
        _nativeGroupCoordinator = nativeGroupCoordinator;
        // On its own the broker coordinates every group, at the address it actually bound; a cluster replaces this
        // with the __consumer_offsets partition leaders once it forms (#204).
        nativeGroupCoordinator.SetCoordinatorLocator(
            new Kuestenlogik.Surgewave.Coordination.Coordinators.LocalCoordinatorLocator(
                _options.BrokerId, () => config.Host, () => _actualPort));
        var queueViewConfig = new Kuestenlogik.Surgewave.Broker.Queue.QueueViewConfig();
        var queueViewManager = new Kuestenlogik.Surgewave.Broker.Queue.QueueViewManager(queueViewConfig, _loggerFactory, _logManager);
        _queueViewManager = queueViewManager;
        var shareGroupCoordinatorLogger = _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupCoordinator>();
        // Share partitions' state in __share_group_state (#228): this broker's share coordinators, and the others'
        // once a cluster forms.
        _shareStateStore = new Kuestenlogik.Surgewave.Broker.ShareGroups.ShareStateStore(
            _logManager, recordBatchSerializer, config.ShareStateTopicNumPartitions,
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.ShareGroups.ShareStateStore>());
        _shareStatePersister = new Kuestenlogik.Surgewave.Broker.ShareGroups.RoutingShareStatePersister(_shareStateStore);
        var shareGroupCoordinator = new Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupCoordinator(
            shareGroupCoordinatorLogger, _logManager, _offsetStore, Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupDefaults.From(config),
            shareState: _shareStatePersister);
        _shareGroupCoordinator = shareGroupCoordinator;
        var consumerGroupV2Coordinator = new Kuestenlogik.Surgewave.Broker.ConsumerGroupV2.ConsumerGroupV2Coordinator(
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.ConsumerGroupV2.ConsumerGroupV2Coordinator>(), _logManager, _offsetStore);
        // Classic coordinator gets the v2 reference so OffsetCommit/Fetch from a
        // KIP-848 consumer routes through v2 epoch validation when the group is not
        // in the classic state map.
        var consumerGroupCoordinator = new ConsumerGroupCoordinator(
            coordinatorLogger, _offsetStore,
            v2Coordinator: consumerGroupV2Coordinator);
        var streamsGroupCoordinator = new Kuestenlogik.Surgewave.Broker.StreamsGroups.StreamsGroupCoordinator(
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.StreamsGroups.StreamsGroupCoordinator>(), _logManager);
        var producerStateManager = new ProducerStateManager(new ProducerIdBlocks(_options.BrokerId, _dataDirectory));
        var transactionIndex = new TransactionIndex();
        // Transaction state lives in __transaction_state (#197), as the group offsets do.
        _transactionStateLog = new Kuestenlogik.Surgewave.Broker.Transactions.TransactionStateLog(
            _logManager, recordBatchSerializer, config.TransactionStateLogNumPartitions,
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.Transactions.TransactionStateLog>());
        var transactionCoordinator = new TransactionCoordinator(
            producerStateManager, _logManager, transactionIndex, _offsetStore, _transactionStateLog, txnCoordinatorLogger);
        _transactionCoordinator = transactionCoordinator;
        _quotaManager = new QuotaManager(config.Quotas, quotaManagerLogger);

        _metrics = new BrokerMetrics();

        // Create dynamic broker config for runtime config modifications
        var dynamicBrokerConfigLogger = _loggerFactory.CreateLogger<DynamicBrokerConfig>();
        var dynamicBrokerConfig = new DynamicBrokerConfig(config, dynamicBrokerConfigLogger);

        // Kafka handler array + connection handler — built only when EnableKafka (#58).
        // When disabled the embedded broker is native-only: no handlers, no Kafka connection
        // handler; SurgewaveBroker then rejects Kafka connections (#59 b5).
        IConnectionHandler[]? connectionHandlers = null;
        if (_options.EnableKafka)
        {
            // Create request handlers
            var dataApiLogger = _loggerFactory.CreateLogger<DataApiHandler>();
            var metadataApiLogger = _loggerFactory.CreateLogger<MetadataApiHandler>();
            var topicAdminLogger = _loggerFactory.CreateLogger<TopicAdminHandler>();
            _topicAdminHandler = new TopicAdminHandler(config, _logManager, _quotaManager, auditLogger: null, topicAdminLogger);
            _metadataApiHandler = new MetadataApiHandler(config, _logManager, metadataApiLogger);
            // Held as a field: the durability gate it needs is built later, once clustering exists.
            _dataApiHandler = new DataApiHandler(config, _logManager, transactionCoordinator, _quotaManager, recordBatchSerializer, aclAuthorizer: null, deduplicationManager: null, delayIndex: null, ttlIndex: null, _metrics, dataApiLogger, partitionAppender: _options.PartitionAppender, disaggregatedReader: _options.DisaggregatedReader);
            IKafkaRequestHandler[] handlers =
            [
                _dataApiHandler,
                _metadataApiHandler,
                _topicAdminHandler,
                _configApiHandler = new ConfigApiHandler(config, dynamicBrokerConfig, _logManager),
                new SecurityApiHandler(config, saslAuthenticator: null, aclAuthorizer: null, auditLogger: null, _loggerFactory.CreateLogger<SecurityApiHandler>()),
                // The classic consumer-group, consumer-group v2 (KIP-848) and streams-group
                // (KIP-1071) APIs now route through the dispatcher (their broker fast-paths
                // were removed when the coordinators went protocol-neutral, #59), so their
                // adapters must be registered here too.
                _consumerGroupApiHandler = new ConsumerGroupApiHandler(consumerGroupCoordinator, consumerGroupV2Coordinator, _loggerFactory.CreateLogger<ConsumerGroupApiHandler>()),
                _consumerGroupV2ApiHandler = new ConsumerGroupV2ApiHandler(consumerGroupV2Coordinator, _logManager.ResolveTopicId, _loggerFactory.CreateLogger<ConsumerGroupV2ApiHandler>()),
                new StreamsGroupApiHandler(streamsGroupCoordinator, _loggerFactory.CreateLogger<StreamsGroupApiHandler>()),
                // Transaction + share-group (KIP-932) APIs also route through the dispatcher now
                // (broker fast-paths removed when the coordinators went protocol-neutral, #59).
                new TransactionApiHandler(transactionCoordinator, _loggerFactory.CreateLogger<TransactionApiHandler>()),
                new ShareGroupApiHandler(shareGroupCoordinator, _loggerFactory.CreateLogger<ShareGroupApiHandler>()),
                // Advertised by ApiVersions like the rest, so they are answered here as in the broker host (#240).
                new QuotaApiHandler(_quotaManager, _loggerFactory.CreateLogger<QuotaApiHandler>()),
                new DelegationTokenApiHandler(
                    new DelegationTokenManager(config.DelegationTokens, config.DataDirectory, _loggerFactory.CreateLogger<DelegationTokenManager>()),
                    _loggerFactory.CreateLogger<DelegationTokenApiHandler>()),
                new TelemetryApiHandler(
                    _loggerFactory.CreateLogger<TelemetryApiHandler>(),
                    config.Telemetry,
                    _telemetryIngestor = new Kuestenlogik.Surgewave.Broker.Telemetry.LoggingTelemetryIngestor(
                        _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.Telemetry.LoggingTelemetryIngestor>()))
            ];
            _kafkaConnectionHandler = new KafkaConnectionHandler(
                handlers,
                config.KafkaPipelineDepth,
                _metrics,
                _loggerFactory.CreateLogger<KafkaConnectionHandler>(),
                _loggerFactory.CreateLogger<RequestDispatcher>());
            connectionHandlers = [_kafkaConnectionHandler];
        }

        _broker = new SurgewaveBroker(
            config, _logManager, recordBatchSerializer, nativeGroupCoordinator,
            transactionCoordinator, _quotaManager, _metrics, brokerLogger,
            connectionHandlers: connectionHandlers, loggerFactory: _loggerFactory);

        _cts = new CancellationTokenSource();
        _brokerTask = Task.Run(() => _broker.StartAsync(_cts.Token), cancellationToken);

        // Wait until the listener is bound and accepts; a bind that failed is reported here.
        await _broker.WaitUntilAcceptingAsync(cancellationToken);

        // Initialize cluster components if enabled
        if (_options.EnableCluster)
        {
            await InitializeClusterAsync(config, cancellationToken);
        }
        else
        {
            // A broker without a cluster advertises the cluster admin APIs too, so it answers them: nothing is being
            // reassigned, and there is no controller to elect a leader or move a replica (#240).
            _kafkaConnectionHandler?.AddHandler(new ClusterAdminHandler(
                controller: null, reassignmentManager: null, new ClusterState(),
                _loggerFactory.CreateLogger<ClusterAdminHandler>()));
            await _offsetStore!.StartLocalAsync(cancellationToken);
            await _transactionStateLog!.StartLocalAsync(cancellationToken);
            await _shareStateStore!.StartLocalAsync(cancellationToken);
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
            raftDataDirectory: config.RaftDataDirectory,
            raftElectionTimeoutMinMs: config.RaftElectionTimeoutMinMs,
            raftElectionTimeoutMaxMs: config.RaftElectionTimeoutMaxMs,
            raftHeartbeatIntervalMs: config.RaftHeartbeatIntervalMs,
            raftPeerDiscoveryTimeoutSeconds: config.RaftPeerDiscoveryTimeoutSeconds,
            autoRebalanceEnabled: config.AutoRebalanceEnabled,
            rebalanceCheckIntervalSeconds: config.RebalanceCheckIntervalSeconds,
            rebalanceImbalanceThreshold: config.RebalanceImbalanceThreshold,
            reassignmentThrottleBytesPerSec: config.ReassignmentThrottleBytesPerSec,
            reassignmentMaxConcurrent: config.ReassignmentMaxConcurrent,
            processRoles: config.ProcessRoles,
            controllerQuorumVoters: config.ControllerQuorumVoters,
            offsetsTopicNumPartitions: config.OffsetsTopicNumPartitions,
            offsetsTopicReplicationFactor: config.OffsetsTopicReplicationFactor,
            transactionStateLogNumPartitions: config.TransactionStateLogNumPartitions,
            transactionStateLogReplicationFactor: config.TransactionStateLogReplicationFactor,
            shareStateTopicNumPartitions: config.ShareStateTopicNumPartitions,
            shareStateTopicReplicationFactor: config.ShareStateTopicReplicationFactor,
            replicationBindAddress: config.ReplicationBindAddress
        );

        // #170 — an invalid configuration stops the runtime here rather than surfacing later as a
        // cluster that will not form. Both configs together, so a caller fixing one sees all of it.
        ConfigValidator.ThrowIfAnyInvalid(config, clusteringConfig);

        // Create cluster state
        _clusterState = new ClusterState();

        // Register this broker with actual replication port. Self-advertise the highest
        // inter-broker protocol level this build speaks (#60 Inc5) — the finalized level is the MIN
        // over all nodes including the local one.
        _clusterState.AddBroker(new BrokerNode
        {
            BrokerId = _options.BrokerId,
            Host = _options.Host,
            Port = _actualPort,
            ReplicationPort = _actualReplicationPort,
            InterBrokerProtocol = InterBrokerProtocolFeature.LocalMaxSupported
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

        // #60 Inc5 — native controller client + the finalized-level gate. Control-plane pushes go
        // native (SRWV frames to the ReplicationPort) once EVERY registered broker advertises the
        // native inter-broker protocol; until then they stay on the Kafka-wire ControllerClient.
        var nativeControllerClient = new NativeControllerClient(
            _connectionPool, _clusterState, clusteringConfig,
            _loggerFactory.CreateLogger<NativeControllerClient>());
        var controllerRpc = new GatedControllerReplicaRpc(
            _clusterState, nativeControllerClient, kafkaWireFallback: _controllerClient,
            _loggerFactory.CreateLogger<GatedControllerReplicaRpc>());

        // Create replica manager, wiring the notifier so a leader reports ISR
        // growth back to the controller.
        _replicaManager = new ReplicaManager(
            replicaManagerLogger,
            _clusterState,
            _logManager!,
            clusteringConfig,
            peerTransport,
            // Without this every clustering metric the runtime could emit was dropped on the
            // floor — ISR joins and departures, replicated bytes, replication lag, fetch spans.
            // Broker.App has always passed its BrokerMetrics here; the in-process runtime simply
            // never did, so an embedded host was blind to its own replication.
            _metrics,
            isrChangeNotifier: controllerRpc);

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
        // Every topic and every topic config change goes through the controller into the metadata log, whatever the
        // entry point (#214, #119).
        var topics = new MetadataLogTopics(_clusterController, _clusterController, _clusterState, _logManager!, clusteringConfig.BrokerId);
        _logManager!.RouteTopicCreation(topics);
        _logManager.RouteTopicConfigChanges(topics);

        // #72 Inc4 — controller-epoch high-water: prime from the node-local persisted floor and
        // persist every strict advance, so a restarted broker elects strictly above every reign it
        // already observed. Broker epochs are committed metadata-log indices now (#171) and do not
        // depend on this.
        var controllerEpochStore = new ControllerEpochStore(
            clusteringConfig.DataDirectory, _loggerFactory.CreateLogger<ControllerEpochStore>());
        _clusterState.PrimeControllerEpochFloor(controllerEpochStore.Load());
        _clusterState.OnControllerEpochAdvanced = controllerEpochStore.Save;

        // #122 — acks=all is refused rather than silently downgraded once a partition is short of
        // in-sync replicas. Wired here because the gate needs the cluster state, which is built
        // after the request handlers.
        _dataApiHandler?.SetCommitGate(new ReplicaCommitGate(_clusterState, _replicaManager));
        _offsetStore?.FollowLeadershipOf(_replicaManager, new ReplicaCommitGate(_clusterState, _replicaManager));
        _transactionStateLog?.FollowLeadershipOf(_replicaManager, new ReplicaCommitGate(_clusterState, _replicaManager));
        _shareStateStore?.FollowLeadershipOf(_replicaManager, new ReplicaCommitGate(_clusterState, _replicaManager));

        // #164 — the produce path refuses a partition another broker leads. Supplied here
        // rather than in the constructor because clustering does not exist yet up there;
        // a runtime without clustering never calls this, and its produce path is unchanged.
        var partitionLeadership = new ClusterStatePartitionLeadership(_clusterState, clusteringConfig.BrokerId);
        _dataApiHandler?.SetPartitionLeadership(partitionLeadership);

        // The native path is this product's default and gets the same refusal — checking
        // only the Kafka compatibility surface would protect the wrong one. It lives on
        // SurgewaveBroker, which owns the native handler.
        _broker?.SetPartitionLeadership(partitionLeadership);

        // #228 — a share partition is served by its partition's leader.
        _shareGroupCoordinator?.SetPartitionLeadership(partitionLeadership);

        // Wire up heartbeat manager
        _clusterController.SetHeartbeatManager(_heartbeatManager);
        _replicationServer.SetHeartbeatManager(_heartbeatManager);

        // #60 Inc6b — the shared, protocol-neutral cluster-membership authority (broker registration
        // epochs + fencing). Wired unconditionally so a plugin-free broker owns it too.
        var clusterIdManager = _clusterIdentity!;
        _clusterController.SetClusterIdentity(clusterIdManager);
        // epochsAreMetadataIndices stays false: a JOINING broker registers over the native
        // lifecycle RPC, which calls ClusterMembershipService.Register directly and mints a
        // composed epoch — the metadata log carries only the controller's own registration.
        // Until registration goes through the log, an epoch is not an index and the strict
        // caught-up comparison would fence every joiner forever. Tracked separately; it is
        // what lets the composed mint go and Kafka's rule apply (#160, #163 step 3).
        var membershipService = new ClusterMembershipService(
            clusterIdManager, _clusterState, _loggerFactory.CreateLogger<ClusterMembershipService>(),
            minFencedDwell: TimeSpan.FromMilliseconds(clusteringConfig.NativeMinFencedDwellMs),
            timeProvider: TimeProvider.System);

        // #123 — the native path only ever learned that a broker exists, never that one
        // stopped: a silent follower stayed registered, unfenced and holding a valid
        // epoch, and went into every metadata push as live. The sweeper is what asks.
        _sessionSweeper = new BrokerSessionSweeper(
            membershipService,
            TimeSpan.FromMilliseconds(clusteringConfig.NativeSessionTimeoutMs),
            _loggerFactory.CreateLogger<BrokerSessionSweeper>());
        _sessionSweeper.Start();

        // #162 — a broker's heartbeat reports a failed log directory; the controller moves the partitions off it.
        _clusterController.SetMembership(membershipService);

        // And what makes the fencing observable: a fenced broker stops being pushed as
        // live. Wired unconditionally — the filter falling back to "no filter" without a
        // membership authority is for a host that has none, not an opt-in.
        // When this broker is both controller and partition leader, its own ISR report has to go
        // into the metadata log rather than onto the wire (#176).
        nativeControllerClient.SetIsrUpdateApplier(_clusterController);
        // #180 — how this broker asks the controller to move its partitions when it leaves.
        _clusterController.SetControlledShutdownRequester(nativeControllerClient);
        _clusterController.SetTopicCreationRequester(nativeControllerClient);
        // #228 — a group's configs are in the metadata log, changed through the controller and read by every broker.
        _clusterController.SetGroupConfigRequester(nativeControllerClient);
        _clusterController.SetTopicConfigRequester(nativeControllerClient);
        _shareGroupCoordinator?.SetGroupConfigStore(new ClusterGroupConfigStore(_clusterState, _clusterController));

        // #60 Inc4/Inc5/Inc6b — wire the native SRWV inter-broker receive server onto the
        // ReplicationServer's shared port. It applies decoded native control-plane frames
        // (LeaderAndIsr / UpdateMetadata / StopReplica / AlterPartition) and handles native
        // registration/heartbeat via the membership authority — with no Kafka-wire dependency. Wired
        // unconditionally (native clustering must not depend on the Kafka plugin); legacy fetch/Raft
        // traffic is unaffected by the multiplex.
        var nativeInterBrokerLogger = _loggerFactory.CreateLogger<NativeInterBrokerServer>();

        // #171 — registration goes through the metadata log, so a broker's epoch is the committed
        // index of its own registration entry rather than a locally minted number.
        var registrationCoordinator = new BrokerRegistrationCoordinator(
            membershipService, _clusterController, _clusterState, clusteringConfig.BrokerId,
            _loggerFactory.CreateLogger<BrokerRegistrationCoordinator>());

        _replicationServer.SetNativeInterBrokerServer(new NativeInterBrokerServer(
            nativeInterBrokerLogger,
            new ClusterStateInterBrokerService(
                _loggerFactory.CreateLogger<ClusterStateInterBrokerService>(),
                _clusterState, _replicaManager, _logManager!, clusteringConfig.BrokerId,
                isrUpdateApplier: _clusterController, membership: membershipService,
                markerSink: _transactionCoordinator, // #60 Inc7: apply native WriteTxnMarkers to the txn index
                registrationCoordinator: registrationCoordinator,
                shutdownCoordinator: _clusterController,
                topicCreator: _clusterController,
                groupConfigs: _clusterController,
                topicConfigs: _clusterController,
                transactionVerifier: _transactionCoordinator,
                commitGate: new ReplicaCommitGate(_clusterState, _replicaManager),
                shareState: _shareStateStore)));
        _shareStatePersister?.SetRemote(new NativeShareStatePersister(
            _connectionPool, _clusterState, clusteringConfig.BrokerId,
            _loggerFactory.CreateLogger<NativeShareStatePersister>()));

        // The coordinator joins the cluster over the same native transport (#197): markers go to the partitions'
        // leaders (gated by the finalized inter-broker level; no Kafka-wire fallback in the embedded runtime), and a
        // partition this broker leads is verified with the transactional id's coordinator.
        var nativeMarkerReplicator = new NativeTransactionMarkerReplicator(
            _connectionPool, _clusterState, clusteringConfig.BrokerId,
            _loggerFactory.CreateLogger<NativeTransactionMarkerReplicator>());
        _transactionCoordinator!.JoinCluster(
            new GatedTransactionMarkerReplicator(
                _clusterState, nativeMarkerReplicator, kafkaWireFallback: null,
                _loggerFactory.CreateLogger<GatedTransactionMarkerReplicator>()),
            new ReplicaCommitGate(_clusterState, _replicaManager),
            new NativeTransactionVerifier(
                _connectionPool, _clusterState, clusteringConfig.BrokerId,
                _loggerFactory.CreateLogger<NativeTransactionVerifier>()));

        // #60 Inc6b — the native broker-lifecycle loop: registers this broker with the controller over
        // the ReplicationPort and heartbeats, so a plugin-free broker JOINS. It no-ops on the
        // controller/seed itself (the client refuses to dial self) and when standalone.
        var lifecycleClient = new NativeBrokerLifecycleClient(
            _connectionPool, _clusterState, clusteringConfig,
            _loggerFactory.CreateLogger<NativeBrokerLifecycleClient>());
        _lifecycleLoop = new Kuestenlogik.Surgewave.Clustering.Cluster.BrokerLifecycleLoop(
            lifecycleClient, clusteringConfig, _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Clustering.Cluster.BrokerLifecycleLoop>(),
            metadataPosition: () => _raftNode?.CommitIndex ?? 0,
            offlineLogDirs: () => _logManager!.OfflineLogDirectoryIds,
            clusterIdentity: clusterIdManager);
        // The controller hears of a failed log directory with the next heartbeat, sent at once rather than after the
        // interval (#162).
        var lifecycleLoop = _lifecycleLoop;
        _logManager!.LogDirectory.WentOffline += _ => lifecycleLoop.RequestHeartbeat();

        // Wire up the controller client (gated native/Kafka-wire, Inc5) so the controller can push
        // LeaderAndIsr to remote brokers when topology changes (topic create, reelection).
        // Without this the controller updates only its own local state and
        // followers never learn to fetch, so the ISR never grows past {leader}.

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
            _kafkaConnectionHandler?.AddHandler(new InterBrokerApiHandler(
                config, _clusterState, _replicaManager, _logManager!, interBrokerApiLogger,
                _transactionCoordinator, isrUpdateApplier: _clusterController));

            // The cluster admin APIs (ElectLeaders, AlterPartitionReassignments, ListPartitionReassignments) need the
            // controller too. ApiVersions advertises them, so they must be answered: kafka-topics --describe lists
            // the partition reassignments before it describes a topic, and without this handler it got a frame with
            // no body and failed (#240).
            _reassignmentManager = new Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignmentManager(
                _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignmentManager>(),
                _clusterState, _clusterController, _replicaManager, _logManager!, clusteringConfig);
            _kafkaConnectionHandler?.AddHandler(new ClusterAdminHandler(
                _clusterController, _reassignmentManager, _clusterState,
                _loggerFactory.CreateLogger<ClusterAdminHandler>()));
        }

        // Wire up topic admin handler to use cluster controller for topic creation

        // Wire up metadata handler to use cluster state for accurate replica info
        _metadataApiHandler?.SetClusterState(_clusterState);
        _configApiHandler?.SetClusterState(_clusterState);

        // In a cluster, a group's coordinator is the leader of its __consumer_offsets partition (#204); without a
        // cluster the handlers keep coordinating every group themselves.
        var coordinators = new ClusterCoordinatorLocator(_clusterState);
        _metadataApiHandler?.SetCoordinatorLocator(coordinators);
        _consumerGroupApiHandler?.SetCoordinatorLocator(coordinators);
        _consumerGroupV2ApiHandler?.SetCoordinatorLocator(coordinators);
        _nativeGroupCoordinator?.SetCoordinatorLocator(coordinators);
        _metadataApiHandler?.SetBrokerFencing(membershipService.IsKnownFenced);

        // Initialize Raft components — every broker has a metadata log (#163 step 3).
        {
            var raftNodeLogger = _loggerFactory.CreateLogger<RaftNode>();
            var raftPersistenceLogger = _loggerFactory.CreateLogger<RaftPersistence>();
            var raftTransportLogger = _loggerFactory.CreateLogger<RaftTransport>();
            var metadataStateMachineLogger = _loggerFactory.CreateLogger<MetadataStateMachine>();

            _raftPersistence = new RaftPersistence(raftPersistenceLogger, clusteringConfig);
            _raftTransport = new RaftTransport(raftTransportLogger, _clusterState, clusteringConfig, peerTransport);
            // #165 — the log apply has to act, not only record: a committed leadership change
            // must reach BecomeLeader/BecomeFollower on this broker, exactly as a controller
            // push does. Queued rather than awaited, because Apply runs in the Raft apply
            // loop and a follower's transition must not block consensus.
            _replicationTransitions = new ReplicationTransitionQueue(
                _replicaManager, clusteringConfig.BrokerId,
                _loggerFactory.CreateLogger<ReplicationTransitionQueue>());

            _metadataStateMachine = new MetadataStateMachine(
                metadataStateMachineLogger, _clusterState, membershipService, _replicationTransitions);

            _raftNode = new RaftNode(
                raftNodeLogger,
                clusteringConfig,
                _raftPersistence,
                _raftTransport,
                _metadataStateMachine);

            // Wire up Raft node with controller and replication server
            _clusterController.SetRaftNode(_raftNode);
            _clusterController.SetReplicationTransitions(_replicationTransitions);
            _replicationServer.SetRaftNode(_raftNode);
        }

        // Start cluster components, in the order a Raft node needs (#275): the node reads the term, the vote and the
        // log it persisted, then the replication port opens, then the node looks for its peers and starts its
        // elections. The port and the controller used to be started side by side, so a peer's request could reach a
        // node that had not read its state yet and was answered from term 0 and no vote. Both steps are awaited
        // here: a log that cannot be read and a port that cannot be bound stop the start, and the port is listening
        // when its start returns — it was waited for by connecting to it every 50 ms.
        await _clusterController.LoadAsync(_cts!.Token);
        await _replicationServer.StartAsync(_cts.Token);

        _clusterTask = Task.Run(async () =>
        {
            var heartbeatTask = _heartbeatManager.StartAsync(_cts.Token);
            var controllerTask = _clusterController.StartAsync(_cts.Token);
            var replicaManagerTask = _replicaManager.StartAsync(_cts.Token);

            await Task.WhenAll(heartbeatTask, controllerTask, replicaManagerTask);
        }, cancellationToken);

        // #60 Inc6b — start the native lifecycle loop once our replication server is up. It idles on
        // the controller/seed and when standalone; on a joining broker it registers with the
        // controller (retrying until reachable) and heartbeats.
        if (_lifecycleLoop is not null)
            await _lifecycleLoop.StartAsync(_cts!.Token);

        // The startup handshake (#250), where this broker can complete it on its own: a broker that is its whole
        // quorum elects itself and applies its own log, so it is returned to the caller caught up — a topic created
        // before a restart is there for the first request. A broker of a larger quorum, and an observer, catch up
        // only once a majority runs and knows them, and an embedding host starts its brokers one after the other; it
        // waits on WaitForMetadataCaughtUpAsync once it has started them.
        if (_raftNode.CatchesUpAlone)
        {
            // A cluster component that failed to start is reported, not waited out: the node behind it never leads.
            var caughtUp = _clusterController.WaitForMetadataCaughtUpAsync(cancellationToken);
            if (await Task.WhenAny(caughtUp, _clusterTask) == _clusterTask)
                await _clusterTask;
            await caughtUp;
        }
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

    /// <summary>
    /// The address the replication listener binds where the client listener binds loopback: the same loopback
    /// address, so an embedded broker that only its own host reaches listens on nothing else. Null, and with it
    /// every interface, where the client listener is reachable from outside the host — a host that is not a
    /// loopback address, or dual mode — since the other brokers of such a cluster may be on other hosts.
    /// </summary>
    private static string? LoopbackReplicationBindAddress(SurgewaveRuntimeOptions options)
    {
        if (options.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase))
            return options.EnableDualMode ? null : IPAddress.Loopback.ToString();
        return IPAddress.TryParse(options.Host, out var address) && IPAddress.IsLoopback(address)
            ? address.ToString()
            : null;
    }

    /// <summary>
    /// Free ports, distinct from each other: every listener stays bound until all are picked, so the OS cannot
    /// return one port twice.
    /// </summary>
    private static int[] FindAvailablePorts(int count, bool enableDualMode)
    {
        var bindAddress = enableDualMode ? IPAddress.IPv6Any : IPAddress.Loopback;
        var listeners = new List<TcpListener>(count);
        try
        {
            for (var i = 0; i < count; i++)
            {
                var listener = new TcpListener(bindAddress, 0);
                if (enableDualMode)
                {
                    listener.Server.DualMode = true;
                }
                listener.Start();
                listeners.Add(listener);
            }

            return [.. listeners.Select(l => ((IPEndPoint)l.LocalEndpoint).Port)];
        }
        finally
        {
            foreach (var listener in listeners)
            {
                listener.Stop();
            }
        }
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
    /// <summary>
    /// Awaits a background task during shutdown, reporting a fault instead of
    /// propagating it. Cancellation is the normal path and says nothing.
    /// </summary>
    private async Task ObserveBackgroundTaskAsync(Task? task, string name)
    {
        if (task is null)
            return;

        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
            // Expected: shutdown cancelled it.
        }
        catch (Exception ex)
        {
            _loggerFactory.CreateLogger<SurgewaveRuntime>().LogWarning(
                ex, "The {Task} task had faulted before shutdown observed it", name);
        }
    }

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

        // Wait for the background tasks. A fault in either is observed and logged
        // here, never rethrown: Dispose reporting a failure that happened minutes
        // earlier, somewhere else, is how a startup problem turns into a confusing
        // teardown error.
        //
        // #151 was exactly that. The replication listener failed to bind with
        // "Address already in use" — on a wildcard port, so ephemeral-port pressure
        // rather than a conflict — the cluster task faulted, nobody observed it, and
        // the exception surfaced from `await controller.DisposeAsync()` in a failover
        // test whose own stack said nothing about binding. The broker task was
        // already swallowing faults for this reason; the cluster task was not, which
        // is the whole of the inconsistency.
        //
        // Logged rather than swallowed silently: the previous code left no trace at
        // all, so a startup failure could be invisible in both directions.
        await ObserveBackgroundTaskAsync(_clusterTask, "cluster");
        await ObserveBackgroundTaskAsync(_brokerTask, "broker");

        if (_sessionSweeper is not null)
            await _sessionSweeper.DisposeAsync();

        if (_replicationTransitions is not null)
            await _replicationTransitions.DisposeAsync();

        if (_reassignmentManager is not null)
            await _reassignmentManager.DisposeAsync();

        // #60 Inc6b — stop the lifecycle loop first so it no longer heartbeats over the pool.
        if (_lifecycleLoop != null)
            await _lifecycleLoop.DisposeAsync();

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

        // Before the broker, not after: the coordinator's timeout timer works against state the
        // shutdown is dismantling, and stopping it while that state is still whole keeps the
        // teardown ordered. This runtime creates the coordinator, so this runtime ends it (the
        // note on the field used to claim SurgewaveBroker did, which was never true).
        if (_transactionCoordinator is not null)
            await _transactionCoordinator.DisposeAsync();

        // Dispose broker
        if (_broker != null)
        {
            await _broker.DisposeAsync();
        }

        // Dispose other resources
        if (_queueViewManager is not null)
            await _queueViewManager.DisposeAsync();
        _telemetryIngestor?.Dispose();
        _metrics?.Dispose();
        _quotaManager?.Dispose();
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
