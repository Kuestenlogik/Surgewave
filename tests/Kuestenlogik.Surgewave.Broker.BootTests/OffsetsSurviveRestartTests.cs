using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// A group's committed offset is still there after the shipped broker restarts (#205). Offsets now live in
/// <c>__consumer_offsets</c>, which the broker's own controller creates and whose partitions the broker loads when it
/// leads them again after the restart — wiring only the real host has.
/// </summary>
[Trait("Category", "Integration")]
[Collection("BrokerBoot")]
public sealed class OffsetsSurviveRestartTests
{
    [Fact(Timeout = 240_000)]
    public async Task ACommittedOffsetIsServedAfterARestart()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var dataDir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);
        var topic = $"orders-{Guid.NewGuid():N}";
        var group = $"orders-app-{Guid.NewGuid():N}";
        var partition = new TopicPartition(topic, 0);

        try
        {
            await using (var first = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false"))
            {
                using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = first.BootstrapServers }).Build())
                {
                    await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
                }

                using var consumer = Consumer(first, group);
                consumer.Assign(partition);
                // Synchronous: answered once the broker has the commit in __consumer_offsets.
                consumer.Commit([new TopicPartitionOffset(partition, new Offset(17))]);
                consumer.Close();
            }

            await using var second = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false");
            using var again = Consumer(second, group);

            // The coordinator answers COORDINATOR_LOAD_IN_PROGRESS while it reads the partition; the client retries.
            var committed = again.Committed([partition], TimeSpan.FromSeconds(60));
            Assert.True(committed.Single().Offset.Value == 17,
                $"committed offset after the restart: {committed.Single().Offset}\n{second.Dump()}");
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }

    private static IConsumer<Ignore, string> Consumer(BrokerProcess broker, string group) =>
        new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = broker.BootstrapServers,
            GroupId = group,
            EnableAutoCommit = false,
        }).Build();
}
