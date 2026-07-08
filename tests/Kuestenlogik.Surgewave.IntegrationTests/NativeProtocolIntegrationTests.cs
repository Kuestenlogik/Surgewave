using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Integration tests for Surgewave native protocol with both Kafka and native clients.
/// Tests protocol auto-detection and interoperability between clients.
/// </summary>
[Collection(nameof(BrokerSpawningCollection))]
public sealed class NativeProtocolIntegrationTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly int _port;
    private readonly string _dataDir;
    private SurgewaveBroker? _broker;
    private LogManager? _logManager;
    private OffsetStore? _offsetStore;
    private TransactionStateStore? _transactionStateStore;
    private BrokerMetrics? _metrics;
    private QuotaManager? _quotaManager;
    private CancellationTokenSource? _brokerCts;
    private Task? _brokerTask;
    private ILoggerFactory? _loggerFactory;

    public NativeProtocolIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
        _port = 19092 + Random.Shared.Next(1000); // Random port to avoid conflicts
        _dataDir = Path.Combine(Path.GetTempPath(), $"surgewave-native-test-{Guid.NewGuid():N}");
    }

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_dataDir);

        var config = new BrokerConfig
        {
            Host = "localhost",
            Port = _port,
            DataDirectory = _dataDir,
            BrokerId = 0,
            AutoCreateTopics = true,
            DefaultNumPartitions = 1
        };

        _loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

        _logManager = new LogManager(_dataDir, FileLogSegmentFactory.Create());

        var brokerLogger = _loggerFactory.CreateLogger<SurgewaveBroker>();
        var serializerLogger = _loggerFactory.CreateLogger<RecordBatchSerializer>();
        var coordinatorLogger = _loggerFactory.CreateLogger<ConsumerGroupCoordinator>();
        var txnCoordinatorLogger = _loggerFactory.CreateLogger<TransactionCoordinator>();
        var txnStateStoreLogger = _loggerFactory.CreateLogger<TransactionStateStore>();
        var offsetStoreLogger = _loggerFactory.CreateLogger<OffsetStore>();

        var recordBatchSerializer = new RecordBatchSerializer(serializerLogger);
        _offsetStore = new OffsetStore(_dataDir, offsetStoreLogger);
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
        _transactionStateStore = new TransactionStateStore(_dataDir, txnStateStoreLogger);
        var transactionCoordinator = new TransactionCoordinator(
            producerStateManager, _logManager, transactionIndex, _offsetStore, _transactionStateStore, txnCoordinatorLogger);
        var quotaManagerLogger = _loggerFactory.CreateLogger<QuotaManager>();
        _quotaManager = new QuotaManager(config.Quotas, quotaManagerLogger);
        IProtocolHandler protocolHandler = new KafkaProtocolHandler();

        _metrics = new BrokerMetrics();

        // Create dynamic broker config for runtime config modifications
        var dynamicBrokerConfig = new DynamicBrokerConfig(config, _loggerFactory.CreateLogger<DynamicBrokerConfig>());

        // Create request handlers
        IKafkaRequestHandler[] handlers =
        [
            new DataApiHandler(config, _logManager, transactionCoordinator, _quotaManager, recordBatchSerializer, aclAuthorizer: null, deduplicationManager: null, delayIndex: null, ttlIndex: null, _metrics, _loggerFactory.CreateLogger<DataApiHandler>()),
            new MetadataApiHandler(config, _logManager, _loggerFactory.CreateLogger<MetadataApiHandler>()),
            new TopicAdminHandler(config, _logManager, _quotaManager, auditLogger: null, _loggerFactory.CreateLogger<TopicAdminHandler>()),
            new ConfigApiHandler(config, dynamicBrokerConfig, _logManager),
            new SecurityApiHandler(config, saslAuthenticator: null, aclAuthorizer: null, auditLogger: null, _loggerFactory.CreateLogger<SecurityApiHandler>())
        ];
        var dispatcher = new RequestDispatcher(handlers);

        _broker = new SurgewaveBroker(
            config, _logManager, recordBatchSerializer, consumerGroupCoordinator, shareGroupCoordinator, nativeGroupCoordinator,
            transactionCoordinator, _quotaManager, protocolHandler, _metrics, dispatcher, brokerLogger);

        _brokerCts = new CancellationTokenSource();
        _brokerTask = Task.Run(() => _broker.StartAsync(_brokerCts.Token));

        // Wait for broker to be ready
        await WaitForBrokerReady();
        _output.WriteLine($"Broker started on port {_port}");
    }

    private async Task WaitForBrokerReady(int maxRetries = 30)
    {
        for (int i = 0; i < maxRetries; i++)
        {
            try
            {
                using var client = new System.Net.Sockets.TcpClient();
                await client.ConnectAsync("localhost", _port);
                return;
            }
            catch
            {
                await Task.Delay(100);
            }
        }
        throw new InvalidOperationException($"Broker did not start within {maxRetries * 100}ms");
    }

    public async ValueTask DisposeAsync()
    {
        if (_brokerCts != null)
        {
            await _brokerCts.CancelAsync();
            _brokerCts.Dispose();
        }

        if (_broker != null)
        {
            await _broker.DisposeAsync();
        }

        _metrics?.Dispose();
        _quotaManager?.Dispose();
        _transactionStateStore?.Dispose();
        _offsetStore?.Dispose();
        _logManager?.Dispose();
        _loggerFactory?.Dispose();

        // Cleanup data directory
        if (Directory.Exists(_dataDir))
        {
            try { Directory.Delete(_dataDir, true); } catch { }
        }
    }

    #region Native Client Tests

    [Fact]
    public async Task NativeClient_CanConnect_And_Ping()
    {
        // Arrange
        await using var client = new SurgewaveNativeClient("localhost", _port);

        // Act
        await client.ConnectAsync();
        var serverTimestamp = await client.Messaging.PingAsync();

        // Assert
        Assert.True(serverTimestamp > 0);
        _output.WriteLine($"Server timestamp: {serverTimestamp}");
    }

    [Fact]
    public async Task NativeClient_CanCreateAndListTopics()
    {
        // Arrange
        await using var client = new SurgewaveNativeClient("localhost", _port);
        await client.ConnectAsync();

        var topicName = $"test-native-topic-{Guid.NewGuid():N}";

        // Act - Create topic
        await client.Topics.CreateAsync(topicName, 3);

        // Act - List topics
        var topics = await client.Topics.ListAsync();

        // Assert
        var topic = topics.FirstOrDefault(t => t.Name == topicName);
        Assert.NotNull(topic);
        Assert.Equal(3, topic.PartitionCount);
        _output.WriteLine($"Created topic: {topicName} with {topic.PartitionCount} partitions");
    }

    [Fact]
    public async Task NativeClient_CanProduceAndFetch()
    {
        // Arrange
        await using var client = new SurgewaveNativeClient("localhost", _port);
        await client.ConnectAsync();

        var topicName = $"test-produce-fetch-{Guid.NewGuid():N}";
        await client.Topics.CreateAsync(topicName, 1);

        // Act - Produce
        var offset1 = await client.Messaging.SendAsync(topicName, 0, "key1", "Hello from native client!");
        var offset2 = await client.Messaging.SendAsync(topicName, 0, "key2", "Second message");
        var offset3 = await client.Messaging.SendAsync(topicName, 0, null, "Message without key");

        _output.WriteLine($"Produced messages at offsets: {offset1}, {offset2}, {offset3}");

        // Act - Fetch
        var result = await client.Messaging.ReceiveAsync(topicName, 0, 0);

        // Assert
        Assert.Equal(3, result.Messages.Count);
        Assert.Equal("Hello from native client!", result.Messages[0].ValueString);
        Assert.Equal("key1", result.Messages[0].KeyString);
        Assert.Equal("Second message", result.Messages[1].ValueString);
        Assert.Equal("Message without key", result.Messages[2].ValueString);
        Assert.Null(result.Messages[2].Key);

        _output.WriteLine($"Fetched {result.Messages.Count} messages, high watermark: {result.HighWatermark}");
    }

    [Fact]
    public async Task NativeClient_GetOffsets()
    {
        // Arrange
        await using var client = new SurgewaveNativeClient("localhost", _port);
        await client.ConnectAsync();

        var topicName = $"test-offsets-{Guid.NewGuid():N}";
        await client.Topics.CreateAsync(topicName, 1);

        // Produce some messages
        for (int i = 0; i < 5; i++)
        {
            await client.Messaging.SendAsync(topicName, 0, $"key{i}", $"Message {i}");
        }

        // Act
        var earliestOffset = await client.Messaging.GetEarliestOffsetAsync(topicName, 0);
        var latestOffset = await client.Messaging.GetLatestOffsetAsync(topicName, 0);

        // Assert
        Assert.Equal(0, earliestOffset);
        Assert.Equal(5, latestOffset);
        _output.WriteLine($"Earliest: {earliestOffset}, Latest: {latestOffset}");
    }

    #endregion

    #region Kafka Client Tests (proving auto-detection works)

    [Fact]
    public async Task KafkaClient_CanProduceAndConsume()
    {
        // Arrange
        var topicName = $"test-kafka-{Guid.NewGuid():N}";

        // First create topic using native client (easier)
        await using var nativeClient = new SurgewaveNativeClient("localhost", _port);
        await nativeClient.ConnectAsync();
        await nativeClient.Topics.CreateAsync(topicName, 1);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = $"localhost:{_port}",
            MessageTimeoutMs = 5000,
            RequestTimeoutMs = 5000
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = $"localhost:{_port}",
            GroupId = "test-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        // Act - Produce using Kafka client
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        await producer.ProduceAsync(topicName, new Message<string, string>
        {
            Key = "kafka-key",
            Value = "Hello from Kafka client!"
        });
        producer.Flush(TimeSpan.FromSeconds(5));
        _output.WriteLine("Produced message using Kafka client");

        // Act - Consume using Kafka client
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(topicName);

        var result = consumer.Consume(TimeSpan.FromSeconds(10));

        // Assert
        Assert.NotNull(result);
        Assert.Equal("kafka-key", result.Message.Key);
        Assert.Equal("Hello from Kafka client!", result.Message.Value);
        _output.WriteLine($"Consumed message: {result.Message.Value}");
    }

    #endregion

    #region Cross-Protocol Interoperability Tests

    [Fact]
    public async Task Interop_NativeProducer_KafkaConsumer()
    {
        // Arrange
        var topicName = $"test-interop-n2k-{Guid.NewGuid():N}";

        // Create topic and produce using native client
        await using var nativeClient = new SurgewaveNativeClient("localhost", _port);
        await nativeClient.ConnectAsync();
        await nativeClient.Topics.CreateAsync(topicName, 1);

        // Act - Produce messages using native protocol
        var offset1 = await nativeClient.Messaging.SendAsync(topicName, 0, "native-key1", "Message from native client 1");
        var offset2 = await nativeClient.Messaging.SendAsync(topicName, 0, "native-key2", "Message from native client 2");
        _output.WriteLine($"Produced 2 messages using native client at offsets {offset1}, {offset2}");

        // Verify native client can read them back first
        var nativeCheck = await nativeClient.Messaging.ReceiveAsync(topicName, 0, 0);
        _output.WriteLine($"Native client verification: {nativeCheck.Messages.Count} messages");
        foreach (var msg in nativeCheck.Messages)
        {
            _output.WriteLine($"  Native read: offset={msg.Offset}, key={msg.KeyString}, value={msg.ValueString}");
        }

        // Query broker for offsets to verify ListOffsets response
        var earliestOffset = await nativeClient.Messaging.GetEarliestOffsetAsync(topicName, 0);
        var latestOffset = await nativeClient.Messaging.GetLatestOffsetAsync(topicName, 0);
        _output.WriteLine($"Broker reports: earliest={earliestOffset}, latest={latestOffset}");

        // Consume using Kafka client - use direct assignment to avoid consumer group overhead
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = $"localhost:{_port}",
            GroupId = $"interop-test-group-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        // Use explicit offset 0 for reliable testing (Offset.Beginning requires ListOffsets request)
        consumer.Assign(new TopicPartitionOffset(topicName, 0, 0));

        var messages = new List<ConsumeResult<string, string>>();
        var attempts = 0;
        while (messages.Count < 2 && attempts < 10)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(5));
            attempts++;
            if (result != null && result.Message != null)
            {
                messages.Add(result);
                _output.WriteLine($"Kafka consumer received (attempt {attempts}): offset={result.Offset}, key={result.Message.Key} -> {result.Message.Value}");
            }
            else if (result != null)
            {
                _output.WriteLine($"Kafka consumer got non-message result (attempt {attempts}): IsPartitionEOF={result.IsPartitionEOF}");
            }
            else
            {
                _output.WriteLine($"Kafka consumer timed out (attempt {attempts})");
            }
        }

        // Assert
        Assert.Equal(2, messages.Count);
        Assert.Contains(messages, m => m.Message.Value == "Message from native client 1");
        Assert.Contains(messages, m => m.Message.Value == "Message from native client 2");
    }

    [Fact]
    public async Task Interop_KafkaProducer_NativeConsumer()
    {
        // Arrange
        var topicName = $"test-interop-k2n-{Guid.NewGuid():N}";

        // Create topic using native client
        await using var nativeClient = new SurgewaveNativeClient("localhost", _port);
        await nativeClient.ConnectAsync();
        await nativeClient.Topics.CreateAsync(topicName, 1);

        // Act - Produce using Kafka client
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = $"localhost:{_port}",
            MessageTimeoutMs = 5000,
            RequestTimeoutMs = 5000
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        await producer.ProduceAsync(topicName, new Message<string, string>
        {
            Key = "kafka-key1",
            Value = "Message from Kafka producer 1"
        });
        await producer.ProduceAsync(topicName, new Message<string, string>
        {
            Key = "kafka-key2",
            Value = "Message from Kafka producer 2"
        });
        producer.Flush(TimeSpan.FromSeconds(5));
        _output.WriteLine("Produced 2 messages using Kafka client");

        // Act - Consume using native client
        var result = await nativeClient.Messaging.ReceiveAsync(topicName, 0, 0);

        // Assert
        Assert.Equal(2, result.Messages.Count);
        Assert.Contains(result.Messages, m => m.ValueString == "Message from Kafka producer 1");
        Assert.Contains(result.Messages, m => m.ValueString == "Message from Kafka producer 2");

        foreach (var msg in result.Messages)
        {
            _output.WriteLine($"Native consumer received: {msg.KeyString} -> {msg.ValueString}");
        }
    }

    [Fact]
    public async Task Interop_MixedProducers_MixedConsumers()
    {
        // This test uses both clients to produce and consume, demonstrating full interoperability
        var topicName = $"test-interop-mixed-{Guid.NewGuid():N}";

        // Setup
        await using var nativeClient = new SurgewaveNativeClient("localhost", _port);
        await nativeClient.ConnectAsync();
        await nativeClient.Topics.CreateAsync(topicName, 1);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = $"localhost:{_port}",
            MessageTimeoutMs = 5000,
            RequestTimeoutMs = 5000
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = $"localhost:{_port}",
            GroupId = $"mixed-test-group-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        // Produce alternating between native and Kafka
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var o1 = await nativeClient.Messaging.SendAsync(topicName, 0, "n1", "Native message 1");
        await producer.ProduceAsync(topicName, new Message<string, string> { Key = "k1", Value = "Kafka message 1" });
        var o3 = await nativeClient.Messaging.SendAsync(topicName, 0, "n2", "Native message 2");
        await producer.ProduceAsync(topicName, new Message<string, string> { Key = "k2", Value = "Kafka message 2" });
        producer.Flush(TimeSpan.FromSeconds(5));

        _output.WriteLine($"Produced 4 messages (2 native at {o1}, {o3}, 2 Kafka)");

        // Consume all using native client
        var nativeResult = await nativeClient.Messaging.ReceiveAsync(topicName, 0, 0);
        _output.WriteLine($"Native client fetched {nativeResult.Messages.Count} messages:");
        foreach (var msg in nativeResult.Messages)
        {
            _output.WriteLine($"  offset={msg.Offset}, key={msg.KeyString}, value={msg.ValueString}");
        }
        Assert.Equal(4, nativeResult.Messages.Count);

        // Query earliest offset via native client for debugging
        var earliestOffset = await nativeClient.Messaging.GetEarliestOffsetAsync(topicName, 0);
        var latestOffset = await nativeClient.Messaging.GetLatestOffsetAsync(topicName, 0);
        _output.WriteLine($"Broker reports: earliest={earliestOffset}, latest={latestOffset}");

        // Consume all using Kafka client - use Assign for direct partition access
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Assign(new TopicPartitionOffset(topicName, 0, 0)); // Use explicit offset 0 instead of Offset.Beginning

        var kafkaMessages = new List<ConsumeResult<string, string>>();
        var attempts = 0;
        while (kafkaMessages.Count < 4 && attempts < 15)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(5));
            attempts++;
            if (result != null && result.Message != null)
            {
                kafkaMessages.Add(result);
                _output.WriteLine($"Kafka received (attempt {attempts}): offset={result.Offset}, key={result.Message.Key}, value={result.Message.Value}");
            }
            else if (result != null)
            {
                _output.WriteLine($"Kafka got non-message (attempt {attempts}): IsPartitionEOF={result.IsPartitionEOF}");
            }
        }
        _output.WriteLine($"Kafka client consumed {kafkaMessages.Count} messages total");
        Assert.Equal(4, kafkaMessages.Count);

        // Verify order is preserved
        Assert.Equal("Native message 1", nativeResult.Messages[0].ValueString);
        Assert.Equal("Kafka message 1", nativeResult.Messages[1].ValueString);
        Assert.Equal("Native message 2", nativeResult.Messages[2].ValueString);
        Assert.Equal("Kafka message 2", nativeResult.Messages[3].ValueString);
    }

    #endregion

    #region Performance Comparison Tests

    [Fact]
    public async Task Performance_NativeVsKafka_ProduceThroughput()
    {
        // Setup
        var topicNative = $"perf-native-{Guid.NewGuid():N}";
        var topicKafka = $"perf-kafka-{Guid.NewGuid():N}";
        const int messageCount = 1000;
        const int messageSize = 100;

        var messageValue = new string('X', messageSize);
        var messageBytes = Encoding.UTF8.GetBytes(messageValue);

        // Create topics
        await using var nativeClient = new SurgewaveNativeClient("localhost", _port);
        await nativeClient.ConnectAsync();
        await nativeClient.Topics.CreateAsync(topicNative, 1);
        await nativeClient.Topics.CreateAsync(topicKafka, 1);

        // Warmup
        for (int i = 0; i < 10; i++)
        {
            await nativeClient.Messaging.SendAsync(topicNative, 0, null, messageBytes);
        }

        // Benchmark Native protocol
        var nativeSw = Stopwatch.StartNew();
        for (int i = 0; i < messageCount; i++)
        {
            await nativeClient.Messaging.SendAsync(topicNative, 0, null, messageBytes);
        }
        nativeSw.Stop();

        var nativeMs = nativeSw.Elapsed.TotalMilliseconds;
        var nativeThroughput = messageCount / (nativeMs / 1000.0);

        // Benchmark Kafka protocol
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = $"localhost:{_port}",
            MessageTimeoutMs = 30000,
            RequestTimeoutMs = 30000,
            LingerMs = 0 // Disable batching for fair comparison
        };

        using var kafkaProducer = new ProducerBuilder<Null, string>(producerConfig).Build();

        // Warmup
        for (int i = 0; i < 10; i++)
        {
            await kafkaProducer.ProduceAsync(topicKafka, new Message<Null, string> { Value = messageValue });
        }

        var kafkaSw = Stopwatch.StartNew();
        for (int i = 0; i < messageCount; i++)
        {
            await kafkaProducer.ProduceAsync(topicKafka, new Message<Null, string> { Value = messageValue });
        }
        kafkaProducer.Flush(TimeSpan.FromSeconds(30));
        kafkaSw.Stop();

        var kafkaMs = kafkaSw.Elapsed.TotalMilliseconds;
        var kafkaThroughput = messageCount / (kafkaMs / 1000.0);

        // Output results
        _output.WriteLine("=== PRODUCE THROUGHPUT COMPARISON ===");
        _output.WriteLine($"Message count: {messageCount}, Message size: {messageSize} bytes");
        _output.WriteLine($"Native protocol: {nativeMs:F2}ms, {nativeThroughput:F0} msg/sec");
        _output.WriteLine($"Kafka protocol:  {kafkaMs:F2}ms, {kafkaThroughput:F0} msg/sec");
        _output.WriteLine($"Native speedup:  {kafkaMs / nativeMs:F2}x faster");
    }

    [Fact]
    public async Task Performance_NativeVsKafka_FetchLatency()
    {
        // Setup
        var topicName = $"perf-fetch-{Guid.NewGuid():N}";
        const int messageCount = 100;
        const int iterations = 10;

        await using var nativeClient = new SurgewaveNativeClient("localhost", _port);
        await nativeClient.ConnectAsync();
        await nativeClient.Topics.CreateAsync(topicName, 1);

        // Produce messages
        for (int i = 0; i < messageCount; i++)
        {
            await nativeClient.Messaging.SendAsync(topicName, 0, $"key{i}", $"Message {i} with some content");
        }

        // Benchmark Native fetch
        var nativeLatencies = new List<double>();
        for (int i = 0; i < iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            var result = await nativeClient.Messaging.ReceiveAsync(topicName, 0, 0);
            sw.Stop();
            nativeLatencies.Add(sw.Elapsed.TotalMilliseconds);
            Assert.Equal(messageCount, result.Messages.Count);
        }

        // Benchmark Kafka fetch (using consumer assign to specific offset)
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = $"localhost:{_port}",
            GroupId = $"perf-group-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            FetchMaxBytes = 10 * 1024 * 1024 // 10MB
        };

        var kafkaLatencies = new List<double>();
        for (int iter = 0; iter < iterations; iter++)
        {
            using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            consumer.Assign(new TopicPartitionOffset(topicName, 0, 0));

            var sw = Stopwatch.StartNew();
            var messages = new List<ConsumeResult<string, string>>();
            while (messages.Count < messageCount)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result != null)
                {
                    messages.Add(result);
                }
            }
            sw.Stop();
            kafkaLatencies.Add(sw.Elapsed.TotalMilliseconds);
        }

        // Output results
        _output.WriteLine("=== FETCH LATENCY COMPARISON ===");
        _output.WriteLine($"Messages to fetch: {messageCount}, Iterations: {iterations}");
        _output.WriteLine($"Native protocol: avg {nativeLatencies.Average():F2}ms, min {nativeLatencies.Min():F2}ms, max {nativeLatencies.Max():F2}ms");
        _output.WriteLine($"Kafka protocol:  avg {kafkaLatencies.Average():F2}ms, min {kafkaLatencies.Min():F2}ms, max {kafkaLatencies.Max():F2}ms");
        _output.WriteLine($"Native speedup:  {kafkaLatencies.Average() / nativeLatencies.Average():F2}x faster");
    }

    [Fact]
    public async Task Performance_NativeVsKafka_RoundTripLatency()
    {
        // Measure end-to-end latency: produce + fetch
        var topicNative = $"perf-rt-native-{Guid.NewGuid():N}";
        var topicKafka = $"perf-rt-kafka-{Guid.NewGuid():N}";
        const int iterations = 100;

        await using var nativeClient = new SurgewaveNativeClient("localhost", _port);
        await nativeClient.ConnectAsync();
        await nativeClient.Topics.CreateAsync(topicNative, 1);
        await nativeClient.Topics.CreateAsync(topicKafka, 1);

        // Warmup
        for (int i = 0; i < 10; i++)
        {
            await nativeClient.Messaging.SendAsync(topicNative, 0, null, "warmup");
            await nativeClient.Messaging.ReceiveAsync(topicNative, 0, i);
        }

        // Native round-trip
        var nativeLatencies = new List<double>();
        for (int i = 0; i < iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            var offset = await nativeClient.Messaging.SendAsync(topicNative, 0, null, $"Message {i}");
            var result = await nativeClient.Messaging.ReceiveAsync(topicNative, 0, offset);
            sw.Stop();
            nativeLatencies.Add(sw.Elapsed.TotalMilliseconds);
        }

        // Kafka round-trip
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = $"localhost:{_port}",
            MessageTimeoutMs = 5000,
            LingerMs = 0,
            Acks = Acks.Leader
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = $"localhost:{_port}",
            GroupId = $"perf-rt-group-{Guid.NewGuid():N}",
            EnableAutoCommit = false
        };

        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
        consumer.Assign(new TopicPartitionOffset(topicKafka, 0, 0));

        // Warmup Kafka
        for (int i = 0; i < 10; i++)
        {
            await producer.ProduceAsync(topicKafka, new Message<Null, string> { Value = "warmup" });
            consumer.Consume(TimeSpan.FromSeconds(1));
        }

        var kafkaLatencies = new List<double>();
        for (int i = 0; i < iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            await producer.ProduceAsync(topicKafka, new Message<Null, string> { Value = $"Message {i}" });
            var result = consumer.Consume(TimeSpan.FromSeconds(5));
            sw.Stop();
            if (result != null)
            {
                kafkaLatencies.Add(sw.Elapsed.TotalMilliseconds);
            }
        }

        // Output results
        _output.WriteLine("=== ROUND-TRIP LATENCY COMPARISON ===");
        _output.WriteLine($"Iterations: {iterations}");
        _output.WriteLine($"Native protocol: avg {nativeLatencies.Average():F2}ms, p50 {Percentile(nativeLatencies, 50):F2}ms, p99 {Percentile(nativeLatencies, 99):F2}ms");
        _output.WriteLine($"Kafka protocol:  avg {kafkaLatencies.Average():F2}ms, p50 {Percentile(kafkaLatencies, 50):F2}ms, p99 {Percentile(kafkaLatencies, 99):F2}ms");
        _output.WriteLine($"Native speedup:  {kafkaLatencies.Average() / nativeLatencies.Average():F2}x faster");
    }

    private static double Percentile(List<double> values, double percentile)
    {
        var sorted = values.OrderBy(v => v).ToList();
        var index = (int)Math.Ceiling((percentile / 100.0) * sorted.Count) - 1;
        return sorted[Math.Max(0, Math.Min(index, sorted.Count - 1))];
    }

    #endregion
}
