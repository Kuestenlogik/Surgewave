using Confluent.Kafka;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Tests for Kafka transaction support in Surgewave broker.
///
/// How Kafka Transactions Work:
///
/// 1. TRANSACTION FLOW:
///    - Producer calls InitProducerId to get a ProducerId and epoch
///    - Producer begins transaction (client-side)
///    - Producer sends AddPartitionsToTxn to register partitions
///    - Producer sends messages with transactional bit set
///    - Producer calls EndTxn to commit or abort
///    - Broker writes control batches (commit/abort markers) to all partitions
///
/// 2. ISOLATION LEVELS:
///    - READ_UNCOMMITTED: Consumer sees ALL messages immediately, including uncommitted
///    - READ_COMMITTED: Consumer only sees committed messages. Uncommitted messages
///      are held back until the transaction completes (commit or abort)
///
/// 3. LAST STABLE OFFSET (LSO):
///    - The broker tracks LSO per partition
///    - LSO = highest offset where all prior transactions are complete
///    - READ_COMMITTED consumers only see messages up to LSO
///
/// 4. INCOMPLETE TRANSACTIONS:
///    - If producer crashes, transaction remains in-flight
///    - New producer with same transactional.id "fences" the old one
///    - Transaction timeout can also abort hanging transactions
/// </summary>
[Collection("Broker")]
[Trait("Category", TestCategories.Integration)]
public class TransactionTests
{
    private readonly ITestOutputHelper _output;

    public TransactionTests(BrokerFixture fixture, ITestOutputHelper output)
    {
        _ = fixture; // Ensure broker is started
        _output = output;
    }

    /// <summary>
    /// Test that a transactional producer can commit messages successfully.
    /// After commit, both READ_UNCOMMITTED and READ_COMMITTED consumers should see the messages.
    /// </summary>
    [Fact]
    public async Task TransactionalProducer_CommittedMessages_AreVisibleToAllConsumers()
    {
        var topic = $"txn-commit-test-{Guid.NewGuid():N}";
        var transactionalId = $"test-txn-producer-{Guid.NewGuid():N}";

        // Arrange - Create transactional producer
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "txn-test-producer",
            TransactionalId = transactionalId,
            EnableIdempotence = true,
            Acks = Acks.All
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        // Act - Produce messages in a transaction
        producer.InitTransactions(TimeSpan.FromSeconds(10));
        producer.BeginTransaction();

        var messageCount = 5;
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"txn-key-{i}",
                Value = $"Transactional message {i}"
            });
            _output.WriteLine($"Produced message {i} in transaction");
        }

        // Commit the transaction
        producer.CommitTransaction();
        _output.WriteLine("Transaction committed");

        // Assert - READ_COMMITTED consumer should see all messages
        var messages = await ConsumeMessages(topic, IsolationLevel.ReadCommitted, messageCount);
        Assert.Equal(messageCount, messages.Count);
        _output.WriteLine($"READ_COMMITTED consumer received {messages.Count} messages");
    }

    /// <summary>
    /// Test that aborted transaction messages are NOT visible to READ_COMMITTED consumers.
    /// </summary>
    [Fact]
    public async Task TransactionalProducer_AbortedMessages_AreNotVisibleToReadCommittedConsumers()
    {
        var topic = $"txn-abort-test-{Guid.NewGuid():N}";
        var transactionalId = $"test-txn-abort-{Guid.NewGuid():N}";

        // Arrange - Create transactional producer
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "txn-abort-producer",
            TransactionalId = transactionalId,
            EnableIdempotence = true,
            Acks = Acks.All
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        // Act - Produce messages then ABORT
        producer.InitTransactions(TimeSpan.FromSeconds(10));
        producer.BeginTransaction();

        var messageCount = 5;
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"abort-key-{i}",
                Value = $"This message should be aborted {i}"
            });
            _output.WriteLine($"Produced message {i} (will be aborted)");
        }

        // ABORT the transaction
        producer.AbortTransaction();
        _output.WriteLine("Transaction ABORTED");

        // Assert - READ_COMMITTED consumer should see NO messages
        var messages = await ConsumeMessages(topic, IsolationLevel.ReadCommitted, 0, timeoutSeconds: 5);
        Assert.Empty(messages);
        _output.WriteLine($"READ_COMMITTED consumer correctly received 0 messages from aborted transaction");
    }

    /// <summary>
    /// Test the key isolation level difference:
    /// - READ_UNCOMMITTED sees messages immediately (even before commit)
    /// - READ_COMMITTED waits until transaction completes
    /// </summary>
    [Fact]
    public async Task IsolationLevel_ReadCommitted_HoldsBackUncommittedMessages()
    {
        var topic = $"txn-isolation-test-{Guid.NewGuid():N}";
        var transactionalId = $"test-txn-isolation-{Guid.NewGuid():N}";

        // First, produce some non-transactional messages
        var nonTxnProducerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "non-txn-producer"
        };

        using (var nonTxnProducer = new ProducerBuilder<string, string>(nonTxnProducerConfig).Build())
        {
            for (int i = 0; i < 3; i++)
            {
                await nonTxnProducer.ProduceAsync(topic, new Message<string, string>
                {
                    Key = $"non-txn-{i}",
                    Value = $"Non-transactional message {i}"
                });
            }
            nonTxnProducer.Flush(TimeSpan.FromSeconds(5));
            _output.WriteLine("Produced 3 non-transactional messages");
        }

        // Now produce transactional messages but DON'T commit yet
        var txnProducerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "txn-producer-isolation",
            TransactionalId = transactionalId,
            EnableIdempotence = true
        };

        using var txnProducer = new ProducerBuilder<string, string>(txnProducerConfig).Build();
        txnProducer.InitTransactions(TimeSpan.FromSeconds(10));
        txnProducer.BeginTransaction();

        for (int i = 0; i < 3; i++)
        {
            await txnProducer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"txn-pending-{i}",
                Value = $"Pending transactional message {i}"
            });
        }
        _output.WriteLine("Produced 3 transactional messages (NOT committed yet)");

        // READ_UNCOMMITTED should see ALL 6 messages (3 non-txn + 3 uncommitted txn)
        var uncommittedMessages = await ConsumeMessages(topic, IsolationLevel.ReadUncommitted, 6, timeoutSeconds: 10);
        _output.WriteLine($"READ_UNCOMMITTED consumer received {uncommittedMessages.Count} messages");

        // READ_COMMITTED should only see 3 non-transactional messages
        var committedMessages = await ConsumeMessages(topic, IsolationLevel.ReadCommitted, 3, timeoutSeconds: 5);
        _output.WriteLine($"READ_COMMITTED consumer received {committedMessages.Count} messages");

        // Now commit and verify READ_COMMITTED sees all messages
        txnProducer.CommitTransaction();
        _output.WriteLine("Transaction committed");

        var afterCommitMessages = await ConsumeMessages(topic, IsolationLevel.ReadCommitted, 6, timeoutSeconds: 10);
        _output.WriteLine($"After commit, READ_COMMITTED consumer received {afterCommitMessages.Count} messages");

        // Assertions
        Assert.Equal(6, uncommittedMessages.Count); // READ_UNCOMMITTED sees all
        Assert.Equal(3, committedMessages.Count);   // READ_COMMITTED only sees non-txn
        Assert.Equal(6, afterCommitMessages.Count); // After commit, sees all
    }

    /// <summary>
    /// Test mixed transaction scenario:
    /// - Some messages committed
    /// - Some messages aborted
    /// READ_COMMITTED should only see committed messages
    /// </summary>
    [Fact]
    public async Task MixedTransactions_ReadCommittedSeesOnlyCommittedMessages()
    {
        var topic = $"txn-mixed-test-{Guid.NewGuid():N}";

        // Transaction 1: Commit 3 messages
        var txnId1 = $"test-txn-commit-{Guid.NewGuid():N}";
        var producerConfig1 = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            TransactionalId = txnId1,
            EnableIdempotence = true
        };

        using (var producer1 = new ProducerBuilder<string, string>(producerConfig1).Build())
        {
            producer1.InitTransactions(TimeSpan.FromSeconds(10));
            producer1.BeginTransaction();

            for (int i = 0; i < 3; i++)
            {
                await producer1.ProduceAsync(topic, new Message<string, string>
                {
                    Key = $"committed-{i}",
                    Value = $"This message WILL be committed {i}"
                });
            }

            producer1.CommitTransaction();
            _output.WriteLine("Transaction 1: COMMITTED 3 messages");
        }

        // Transaction 2: Abort 3 messages
        var txnId2 = $"test-txn-abort-{Guid.NewGuid():N}";
        var producerConfig2 = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            TransactionalId = txnId2,
            EnableIdempotence = true
        };

        using (var producer2 = new ProducerBuilder<string, string>(producerConfig2).Build())
        {
            producer2.InitTransactions(TimeSpan.FromSeconds(10));
            producer2.BeginTransaction();

            for (int i = 0; i < 3; i++)
            {
                await producer2.ProduceAsync(topic, new Message<string, string>
                {
                    Key = $"aborted-{i}",
                    Value = $"This message will be ABORTED {i}"
                });
            }

            producer2.AbortTransaction();
            _output.WriteLine("Transaction 2: ABORTED 3 messages");
        }

        // READ_COMMITTED should only see the 3 committed messages
        var messages = await ConsumeMessages(topic, IsolationLevel.ReadCommitted, 3, timeoutSeconds: 10);

        Assert.Equal(3, messages.Count);
        Assert.All(messages, m => Assert.StartsWith("committed-", m.Message.Key));
        _output.WriteLine($"READ_COMMITTED consumer correctly received only {messages.Count} committed messages");
    }

    /// <summary>
    /// Test producer fencing - when a new producer with the same transactional.id
    /// starts, it should fence (abort) the old producer's transaction.
    /// </summary>
    [Fact]
    public async Task ProducerFencing_NewProducerAbortsOldTransaction()
    {
        var topic = $"txn-fence-test-{Guid.NewGuid():N}";
        var sharedTxnId = $"shared-txn-id-{Guid.NewGuid():N}";

        // Producer 1: Start transaction but don't complete it
        var producerConfig1 = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            TransactionalId = sharedTxnId,
            EnableIdempotence = true,
            TransactionTimeoutMs = 60000 // Long timeout
        };

        var producer1 = new ProducerBuilder<string, string>(producerConfig1).Build();
        producer1.InitTransactions(TimeSpan.FromSeconds(10));
        producer1.BeginTransaction();

        for (int i = 0; i < 3; i++)
        {
            await producer1.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"producer1-{i}",
                Value = $"From producer 1 (will be fenced) {i}"
            });
        }
        _output.WriteLine("Producer 1: Produced 3 messages in uncommitted transaction");

        // Producer 2: Same transactional.id - this should fence Producer 1
        var producerConfig2 = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            TransactionalId = sharedTxnId, // SAME ID - triggers fencing
            EnableIdempotence = true
        };

        using var producer2 = new ProducerBuilder<string, string>(producerConfig2).Build();
        producer2.InitTransactions(TimeSpan.FromSeconds(10)); // This fences producer1
        _output.WriteLine("Producer 2: Initialized with same transactional.id (fenced producer 1)");

        producer2.BeginTransaction();

        for (int i = 0; i < 3; i++)
        {
            await producer2.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"producer2-{i}",
                Value = $"From producer 2 (committed) {i}"
            });
        }

        producer2.CommitTransaction();
        _output.WriteLine("Producer 2: Committed 3 messages");

        // Producer 1's transaction should now be invalid
        var fenced = false;
        try
        {
            producer1.CommitTransaction();
        }
        catch (KafkaException ex) when (ex.Error.Code == ErrorCode.InvalidProducerEpoch ||
                                        ex.Error.Code == ErrorCode.ProducerFenced ||
                                        ex.Error.Code == ErrorCode.Local_Fenced)
        {
            fenced = true;
            _output.WriteLine($"Producer 1 correctly fenced: {ex.Error.Reason}");
        }
        finally
        {
            producer1.Dispose();
        }

        // READ_COMMITTED should only see producer2's messages
        var messages = await ConsumeMessages(topic, IsolationLevel.ReadCommitted, 3, timeoutSeconds: 10);

        Assert.True(fenced || messages.All(m => m.Message.Key.StartsWith("producer2-", StringComparison.Ordinal)),
            "Either producer1 was fenced, or only producer2's messages are visible");
        _output.WriteLine($"Consumer received {messages.Count} messages, all from producer 2");
    }

    /// <summary>
    /// Helper method to consume messages with specified isolation level
    /// </summary>
    private async Task<List<ConsumeResult<string, string>>> ConsumeMessages(
        string topic,
        IsolationLevel isolationLevel,
        int expectedCount,
        int timeoutSeconds = 15)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            IsolationLevel = isolationLevel
        };

        var messages = new List<ConsumeResult<string, string>>();

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));

        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var result = consumer.Consume(cts.Token);
                if (result != null && !result.IsPartitionEOF)
                {
                    messages.Add(result);
                    _output.WriteLine($"  [{isolationLevel}] Received: {result.Message.Key} = {result.Message.Value}");

                    if (messages.Count >= expectedCount)
                        break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout - expected
        }

        consumer.Close();
        return messages;
    }

    /// <summary>
    /// Consume-process-produce EOS pattern: a transactional producer reads from
    /// an input topic, transforms each record, writes to an output topic, and
    /// commits both the produced records AND the input-topic offsets atomically
    /// in a single transaction. This is the only test that explicitly exercises
    /// the AddOffsetsToTxn (25) and TxnOffsetCommit (28) RPCs on the Kafka wire
    /// — the other transaction tests only cover InitProducerId / AddPartitionsToTxn
    /// / EndTxn. Together with the four existing tests this proves that all
    /// five KIP-98 EOS RPCs (22, 24, 25, 26, 28) are wired through Surgewave's
    /// `ProcessRequestAsync` fast-path switch and round-trip green via Confluent.Kafka.
    /// </summary>
    [Fact]
    public async Task ConsumeProcessProduce_AtomicallyCommitsOffsetsAndOutputs()
    {
        var inputTopic = $"txn-cpp-in-{Guid.NewGuid():N}";
        var outputTopic = $"txn-cpp-out-{Guid.NewGuid():N}";
        var transactionalId = $"test-txn-cpp-{Guid.NewGuid():N}";
        var consumerGroup = $"test-cpp-grp-{Guid.NewGuid():N}";
        const int messageCount = 5;

        // Seed the input topic with non-transactional records.
        var seedConfig = new ProducerConfig { BootstrapServers = BrokerFixture.BootstrapServers, ClientId = "seed" };
        using (var seed = new ProducerBuilder<string, string>(seedConfig).Build())
        {
            for (var i = 0; i < messageCount; i++)
            {
                await seed.ProduceAsync(inputTopic, new Message<string, string>
                {
                    Key = $"in-{i}",
                    Value = $"raw-{i}",
                });
            }
            seed.Flush(TimeSpan.FromSeconds(5));
        }

        // Consume + process + produce + commit-offsets, all inside one transaction.
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = consumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            IsolationLevel = IsolationLevel.ReadCommitted,
        };
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "txn-cpp-producer",
            TransactionalId = transactionalId,
            EnableIdempotence = true,
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(inputTopic);

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        producer.InitTransactions(TimeSpan.FromSeconds(10));
        producer.BeginTransaction();

        var consumed = new List<ConsumeResult<string, string>>();
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (consumed.Count < messageCount && DateTime.UtcNow < deadline)
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(2));
            if (cr is null || cr.Message is null) continue;
            consumed.Add(cr);

            await producer.ProduceAsync(outputTopic, new Message<string, string>
            {
                Key = cr.Message.Key,
                Value = $"processed:{cr.Message.Value}",
            });
        }

        Assert.Equal(messageCount, consumed.Count);

        // SendOffsetsToTransaction triggers AddOffsetsToTxn (25) + TxnOffsetCommit (28).
        var offsets = consumed
            .GroupBy(c => c.TopicPartition)
            .Select(g => new TopicPartitionOffset(g.Key, g.Max(c => c.Offset.Value) + 1))
            .ToList();
        producer.SendOffsetsToTransaction(offsets, consumer.ConsumerGroupMetadata, TimeSpan.FromSeconds(10));
        producer.CommitTransaction();

        // After commit a fresh READ_COMMITTED consumer must see the processed records
        // on the output topic — proves AddOffsetsToTxn + TxnOffsetCommit didn't break
        // the transaction-end path.
        var outputs = await ConsumeMessages(outputTopic, IsolationLevel.ReadCommitted, messageCount, timeoutSeconds: 15);
        Assert.Equal(messageCount, outputs.Count);
        Assert.All(outputs, m => Assert.StartsWith("processed:", m.Message.Value));

        consumer.Close();
    }
}
