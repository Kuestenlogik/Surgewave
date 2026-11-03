using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// A topic created over Kafka is still there after the broker restarts (#212). The metadata log is the only store of
/// topics (#163), and the shipped host never gave the Kafka plugin's CreateTopics handler the cluster controller: the
/// topic went into the local log manager only, past the metadata log, and was gone on the next start while its
/// partition directories stayed on disk. The embedded runtime wired it, so only the real host shows it.
/// </summary>
[Trait("Category", "Integration")]
[Collection("BrokerBoot")]
public sealed class TopicSurvivesRestartTests
{
    [Fact(Timeout = 240_000)]
    public async Task ATopicCreatedOverKafkaIsListedAfterARestart()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var dataDir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);
        var topic = $"orders-{Guid.NewGuid():N}";

        try
        {
            await using (var first = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false"))
            {
                using var admin = Admin(first);
                await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 2, ReplicationFactor = 1 }]);
                Assert.Equal(2, PartitionsOf(admin, topic));
            }

            await using var second = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false");
            using var again = Admin(second);
            Assert.True(PartitionsOf(again, topic) == 2, $"topic {topic} did not survive the restart\n{second.Dump()}");
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }

    private static IAdminClient Admin(BrokerProcess broker) =>
        new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker.BootstrapServers }).Build();

    // The partition count the broker reports for the topic, 0 when it does not know it.
    private static int PartitionsOf(IAdminClient admin, string topic)
    {
        var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
        return metadata.Topics.SingleOrDefault(t => t.Topic == topic) is { Error.IsError: false } t ? t.Partitions.Count : 0;
    }
}
