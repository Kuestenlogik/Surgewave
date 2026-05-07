using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// KIP-848 conformance: drive Surgewave's <c>ConsumerGroupV2Coordinator</c> from the
/// official Confluent.Kafka 2.14 client with <c>group.protocol=consumer</c>. This
/// is the externally-visible side of the next-gen consumer-group protocol — if
/// these pass, the librdkafka next-gen consumer can drive the broker
/// transparently.
/// </summary>
[Collection("Broker")]
[Trait("Category", TestCategories.Integration)]
public sealed class Kip848ConsumerProtocolTests
{
    private readonly BrokerFixture _fixture;
    private readonly string _topic;
    private readonly string _groupId;

    public Kip848ConsumerProtocolTests(BrokerFixture fixture)
    {
        _fixture = fixture;
        var id = Guid.NewGuid().ToString("N")[..8];
        _topic = $"kip848-{id}";
        _groupId = $"kip848-grp-{id}";
    }

    [Fact]
    public async Task NextGenConsumer_CanSubscribeAndConsumeMessages()
    {
        await CreateTopicAsync(_topic, partitions: 3);
        await ProduceAsync(_topic, count: 9);

        var config = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = _groupId,
            ClientId = "kip848-consumer",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        // KIP-848 — opt into the next-gen consumer-group protocol. librdkafka 2.4+
        // routes Subscribe through ConsumerGroupHeartbeat instead of JoinGroup/SyncGroup.
        config.Set("group.protocol", "consumer");

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_topic);

        var consumed = new List<ConsumeResult<string, string>>();
        // KIP-848 reconciliation goes through: heartbeat → assignment → offset-fetch
        // → fetch-start → first records. With cold caches each step is ~5-10ms but
        // the reconcile loop has its own polling cadence. 30 s gives plenty of slack
        // without slowing CI more than the classic-protocol path.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (consumed.Count < 9 && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (result?.Message != null)
            {
                consumed.Add(result);
            }
        }

        consumer.Close();

        Assert.Equal(9, consumed.Count);
        Assert.All(consumed, r => Assert.Equal(_topic, r.Topic));
    }

    [Fact]
    public async Task NextGenConsumer_TwoMembers_PartitionsSplitAcrossGroup()
    {
        var topic = $"kip848-split-{Guid.NewGuid().ToString("N")[..8]}";
        var groupId = $"kip848-split-grp-{Guid.NewGuid().ToString("N")[..8]}";
        await CreateTopicAsync(topic, partitions: 4);

        var baseConfig = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        baseConfig.Set("group.protocol", "consumer");

        var c1Config = new ConsumerConfig(baseConfig.ToDictionary(kv => kv.Key, kv => kv.Value)) { ClientId = "c1" };
        var c2Config = new ConsumerConfig(baseConfig.ToDictionary(kv => kv.Key, kv => kv.Value)) { ClientId = "c2" };
        c1Config.Set("group.protocol", "consumer");
        c2Config.Set("group.protocol", "consumer");

        using var c1 = new ConsumerBuilder<string, string>(c1Config).Build();
        using var c2 = new ConsumerBuilder<string, string>(c2Config).Build();
        c1.Subscribe(topic);
        c2.Subscribe(topic);

        // Drive a few poll cycles so each consumer goes through the heartbeat dance and
        // gets its assignment from the broker.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        while (DateTime.UtcNow < deadline && (c1.Assignment.Count == 0 || c2.Assignment.Count == 0))
        {
            _ = c1.Consume(TimeSpan.FromMilliseconds(200));
            _ = c2.Consume(TimeSpan.FromMilliseconds(200));
        }

        // Snapshot assignments BEFORE Close — Close disposes the underlying librdkafka handle.
        var c1Assignment = c1.Assignment.ToList();
        var c2Assignment = c2.Assignment.ToList();

        c1.Close();
        c2.Close();

        // Both members must have been assigned at least one partition by the
        // server-side range/uniform assignor, with no overlap.
        Assert.NotEmpty(c1Assignment);
        Assert.NotEmpty(c2Assignment);
        var combined = c1Assignment.Concat(c2Assignment).Select(tp => tp.Partition.Value).ToList();
        Assert.Equal(combined.Count, combined.Distinct().Count());
    }

    private static async Task CreateTopicAsync(string topic, int partitions)
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = BrokerFixture.BootstrapServers };
        using var admin = new AdminClientBuilder(adminConfig).Build();
        try
        {
            await admin.CreateTopicsAsync(
            [
                new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }
            ]);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // already there
        }
    }

    private static async Task ProduceAsync(string topic, int count)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
        };
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        for (int i = 0; i < count; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"k{i}",
                Value = $"v{i}",
            });
        }
        producer.Flush(TimeSpan.FromSeconds(5));
    }
}
