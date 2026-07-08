using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests.Fixtures;

/// <summary>
/// Shared test fixture that starts a SASL-enabled Surgewave broker.
/// Uses a dynamic port to avoid conflicts with the regular BrokerFixture.
/// </summary>
#pragma warning disable CA2213 // Disposable fields should be disposed - disposed via Interlocked.Exchange in DisposeAsync
public sealed class SaslBrokerFixture : IAsyncLifetime, IDisposable
{
    private SurgewaveBroker? _broker;
    private LogManager? _logManager;
    private OffsetStore? _offsetStore;
    private TransactionStateStore? _transactionStateStore;
    private BrokerMetrics? _metrics;
    private QuotaManager? _quotaManager;
    private CancellationTokenSource? _cts;
    private Task? _brokerTask;
    private readonly string _dataDirectory;
    private readonly ILoggerFactory _loggerFactory;
    private int _actualPort;

    // Static instance for test compatibility (collection fixture is singleton per collection)
    private static SaslBrokerFixture? _instance;

    /// <summary>
    /// The actual bootstrap servers string (dynamically assigned port).
    /// </summary>
    public static string BootstrapServers => _instance != null
        ? $"localhost:{_instance._actualPort}"
        : throw new InvalidOperationException("SASL Broker not started");

    public int Port => _actualPort;
    public const string TestUsername = "testuser";
    public const string TestPassword = "testpassword";

    public SaslBrokerFixture()
    {
        // Create unique data directory for each test run
        _dataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-sasl-tests", Guid.NewGuid().ToString("N"));

        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Warning);
            builder.AddConsole();
        });
    }

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_dataDirectory);

        // Find an available port dynamically
        _actualPort = FindAvailablePort();

        var config = new BrokerConfig
        {
            Host = "localhost",
            Port = _actualPort,
            DataDirectory = _dataDirectory,
            AutoCreateTopics = true,
            DefaultNumPartitions = 3,
            ShutdownTimeoutSeconds = 5,
            Security = new SecurityConfig
            {
                SaslEnabled = true,
                SaslMechanisms = ["PLAIN"],
                Users = [$"{TestUsername}:{TestPassword}"]
            }
        };

        var retentionPolicy = new RetentionPolicy
        {
            RetentionHours = 1,
            RetentionBytes = -1
        };

        _logManager = new LogManager(_dataDirectory, FileLogSegmentFactory.Create(), retentionPolicy: retentionPolicy);

        var brokerLogger = _loggerFactory.CreateLogger<SurgewaveBroker>();
        var serializerLogger = _loggerFactory.CreateLogger<RecordBatchSerializer>();
        var coordinatorLogger = _loggerFactory.CreateLogger<ConsumerGroupCoordinator>();
        var txnCoordinatorLogger = _loggerFactory.CreateLogger<TransactionCoordinator>();
        var txnStateStoreLogger = _loggerFactory.CreateLogger<TransactionStateStore>();
        var offsetStoreLogger = _loggerFactory.CreateLogger<OffsetStore>();

        var recordBatchSerializer = new RecordBatchSerializer(serializerLogger);
        _offsetStore = new OffsetStore(_dataDirectory, offsetStoreLogger);
        var consumerGroupCoordinator = new ConsumerGroupCoordinator(coordinatorLogger, _offsetStore);
        var queueViewConfig = new Kuestenlogik.Surgewave.Broker.Queue.QueueViewConfig();
        var queueViewManager = new Kuestenlogik.Surgewave.Broker.Queue.QueueViewManager(queueViewConfig, _loggerFactory, _logManager);
        var shareGroupCoordinator = new Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupCoordinator(
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupCoordinator>(), _logManager, queueViewManager);
        var consumerGroupV2Coordinator = new Kuestenlogik.Surgewave.Broker.ConsumerGroupV2.ConsumerGroupV2Coordinator(
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.ConsumerGroupV2.ConsumerGroupV2Coordinator>(), _logManager);
        var streamsGroupCoordinator = new Kuestenlogik.Surgewave.Broker.StreamsGroups.StreamsGroupCoordinator(
            _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.StreamsGroups.StreamsGroupCoordinator>(), _logManager);
        var nativeGroupCoordinatorLogger = _loggerFactory.CreateLogger<Kuestenlogik.Surgewave.Broker.Native.NativeGroupCoordinator>();
        var nativeGroupCoordinator = new Kuestenlogik.Surgewave.Broker.Native.NativeGroupCoordinator(nativeGroupCoordinatorLogger, _offsetStore);
        var producerStateManager = new ProducerStateManager();
        var transactionIndex = new TransactionIndex();
        _transactionStateStore = new TransactionStateStore(_dataDirectory, txnStateStoreLogger);
        var transactionCoordinator = new TransactionCoordinator(
            producerStateManager, _logManager, transactionIndex, _offsetStore, _transactionStateStore, txnCoordinatorLogger);
        var quotaManagerLogger = _loggerFactory.CreateLogger<QuotaManager>();
        _quotaManager = new QuotaManager(config.Quotas, quotaManagerLogger);
        IProtocolHandler protocolHandler = new KafkaProtocolHandler();

        // Create SASL authenticator with configured users
        var credentialStore = new CredentialStore();
        credentialStore.AddUser(TestUsername, TestPassword);
        var saslAuthenticator = new SaslAuthenticator(credentialStore, config.Security.SaslMechanisms);

        // Create handlers and dispatcher
        var dataApiHandler = new DataApiHandler(
            config, _logManager, transactionCoordinator, _quotaManager, recordBatchSerializer, null, null, null, null, null,
            _loggerFactory.CreateLogger<DataApiHandler>());
        var metadataApiHandler = new MetadataApiHandler(
            config, _logManager, _loggerFactory.CreateLogger<MetadataApiHandler>());
        var topicAdminHandler = new TopicAdminHandler(
            config, _logManager, _quotaManager, auditLogger: null, _loggerFactory.CreateLogger<TopicAdminHandler>());
        var dynamicBrokerConfig = new DynamicBrokerConfig(config, _loggerFactory.CreateLogger<DynamicBrokerConfig>());
        var configApiHandler = new ConfigApiHandler(config, dynamicBrokerConfig, _logManager);
        var securityApiHandler = new SecurityApiHandler(
            config, saslAuthenticator, aclAuthorizer: null, auditLogger: null, _loggerFactory.CreateLogger<SecurityApiHandler>());

        var handlers = new IKafkaRequestHandler[]
        {
            dataApiHandler, metadataApiHandler, topicAdminHandler, configApiHandler, securityApiHandler
        };
        var dispatcher = new RequestDispatcher(handlers);

        _metrics = new BrokerMetrics();
        _broker = new SurgewaveBroker(
            config, _logManager, recordBatchSerializer, consumerGroupCoordinator, shareGroupCoordinator, nativeGroupCoordinator,
            transactionCoordinator, _quotaManager, protocolHandler, _metrics, dispatcher, brokerLogger);

        _cts = new CancellationTokenSource();
        _brokerTask = Task.Run(() => _broker.StartAsync(_cts.Token));

        // Wait for broker to be ready
        await WaitForBrokerReady();

        // Set static instance for backward compatibility
        _instance = this;
    }

    private static int FindAvailablePort()
    {
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.IPv6Any, 0);
        listener.Server.DualMode = true;
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private async Task WaitForBrokerReady(int maxRetries = 30)
    {
        for (int i = 0; i < maxRetries; i++)
        {
            try
            {
                using var client = new System.Net.Sockets.TcpClient();
                await client.ConnectAsync("localhost", _actualPort);
                return; // Connected successfully
            }
            catch
            {
                await Task.Delay(100);
            }
        }

        throw new InvalidOperationException($"SASL Broker did not start within {maxRetries * 100}ms");
    }

    public async ValueTask DisposeAsync()
    {
        // Clear static instance
        if (_instance == this)
            _instance = null;

        var cts = Interlocked.Exchange(ref _cts, null);
        if (cts != null)
        {
            await cts.CancelAsync();
            cts.Dispose();
        }

        var broker = Interlocked.Exchange(ref _broker, null);
        if (broker != null)
        {
            await broker.DisposeAsync();
        }

        _metrics?.Dispose();
        _quotaManager?.Dispose();
        _transactionStateStore?.Dispose();
        _offsetStore?.Dispose();
        _logManager?.Dispose();
        _loggerFactory.Dispose();

        // Clean up test data directory
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

    public void Dispose()
    {
        DisposeAsync().GetAwaiter().GetResult();
    }
}

/// <summary>
/// Collection definition for sharing the SASL broker fixture across multiple test classes.
/// </summary>
[CollectionDefinition("SaslBroker")]
public class SaslBrokerCollection : ICollectionFixture<SaslBrokerFixture>
{
}
